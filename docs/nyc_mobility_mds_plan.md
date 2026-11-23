# NYC Mobility — Modern Data Stack Project Plan

> Working name: **nyc-mobility**. Successor to the `lakehouse` (Spark/Iceberg/Airflow) thesis project.
> Status: plan, pre-scaffold. Written 2026-11-23.

## 1. Goal

Build a public website that tells data stories about 15+ years of NYC for-hire mobility, backed by a
small, production-grade analytics stack (DuckDB + dbt + Dagster) that is visible and explained on the site.

Portfolio positioning — this project should demonstrate what the day job (Spark at scale) does not:

- Analytics engineering: layered dbt models, tests, contracts, documentation.
- Asset-based orchestration with partitions, backfills, and data quality checks (Dagster).
- Cost-aware architecture: single-node compute, static hosting, near-zero running cost.
- Product thinking: a site people actually read, with a point of view, not a generic dashboard.

### Non-goals

- No Spark, no cluster, no Kubernetes. Single machine is a feature.
- No live/streaming data. TLC publishes monthly with a ~2-month lag; batch is correct.
- No backend API for the website in v1. Everything served is precomputed.
- No ML in v1 (forecasting can be a later story).

## 2. Data inventory (verified against local `data/`, ~68 GB, as of 2026-11-23)

> Snapshot taken before a top-up download of the latest TLC months (late 2025 and 2026). Refresh this
> table after the download; the M0 ingest asset should be able to regenerate it.

| Dataset | Coverage held | Notes |
|---|---|---|
| yellow | 2009-01 → 2025-12 (2009 has 11 files) | ~14M rows/month (2009) → ~3.5M (2025) |
| green | 2014-01 → 2025-09 | Street-hail outside Manhattan core |
| fhv | 2015-07 → 2025-08 | Sparse schema; base-level FHV |
| fhvhv | 2019-02 → 2025-09 | Uber/Lyft/Via; ~20M rows/month, ~1.5B rows total — the heavy one |

Source URL pattern (for re-download / new months):
`https://d37ci6vzurychx.cloudfront.net/trip-data/{dataset}_tripdata_{YYYY-MM}.parquet`

### Schema eras (yellow) — must be handled explicitly in staging

| Era | Columns | Location |
|---|---|---|
| 2009 | `vendor_name`, `Trip_Pickup_DateTime`, `Fare_Amt`, `Start_Lon/Lat`, `End_Lon/Lat`, … | raw lat/lon |
| 2010 | `vendor_id`, `pickup_datetime`, `pickup_longitude/latitude`, … | raw lat/lon |
| 2011+ | `VendorID`, `tpep_pickup_datetime`, `PULocationID`, `DOLocationID`, … | taxi zone IDs |
| 2025+ | adds `cbd_congestion_fee`; `airport_fee` becomes `Airport_fee` | taxi zone IDs |

Key wrinkle: **2009–2010 trips have coordinates, everything after has zone IDs only.** Unifying them
requires a spatial join of points to taxi zone polygons (DuckDB `spatial` extension). This is a
deliberate, write-up-worthy engineering problem (v2 scope).

Other known issues to handle: column-name casing drift across years (`Airport_fee` vs `airport_fee`,
`vendorID` vs `VendorID` in green), type drift (int vs double for IDs/passenger counts), out-of-range
timestamps (trips stamped in the wrong month/year), negative fares, zero-duration trips.

Reference data needed (seeds):
- `taxi_zone_lookup.csv` (LocationID → borough, zone, service_zone) from TLC.
- Taxi zone shapefile/GeoJSON from TLC (for maps and the 2009–2010 spatial join).
- `cbd_zones.csv` — taxi zones inside the Congestion Relief Zone (Manhattan south of 60th St). Hand-built
  from the MTA definition; document the method.
- `hvfhs_license_num` → company (HV0003 Uber, HV0005 Lyft, etc.).

## 3. Stories (the website)

Each story is a page. v1 ships the first three.

1. **Congestion pricing, one year on** (flagship). Starting 2025-01-05, Manhattan below 60th St
   charges a toll; `cbd_congestion_fee` appears in 2025 data. Compare 2025 vs 2024 by zone, hour,
   and service (yellow vs Uber/Lyft): trip volumes into/out of the zone, fares, trip times, who absorbed the fee.
2. **The rise of ride-hail.** Yellow decline 2011→2019, FHVHV takeover, market share by borough.
   Where did yellow cabs stop going?
3. **COVID and recovery.** March 2020 collapse, recovery curves by service and borough, what never came back.
4. *(v2)* **2009 vs today on a map.** Uses the coordinate-era data joined to zones.
5. *(v2)* **Tipping and payments.** Cash→card shift, tip ratio by zone/time, effect of in-cab screens.
6. *(v2)* **Airports.** JFK/LGA/EWR flows, airport fees, ride-hail vs taxi share.

Plus a permanent **"How this is built"** page: architecture diagram, lineage, data quality results,
dbt docs link, and costs. For a hiring manager this page matters as much as the stories.

## 4. Architecture

```
TLC CloudFront ──► raw parquet (local disk / R2 bucket, partitioned year/dataset)
                        │
                 Dagster  (monthly-partitioned assets, sensors for new months)
                        │
                 dbt-duckdb
                   staging      one model per dataset × schema era, rename/cast only
                   intermediate unified trips across eras and services, flags not filters
                   marts        small, story-shaped aggregates
                        │
                 export marts → Parquet (+ zone GeoJSON)  ──► R2 / static assets
                        │
                 Website (static) reads Parquet via DuckDB-WASM or build-time loaders
                 hosted on Cloudflare Pages
```

### Technology decisions

| Concern | Choice | Why |
|---|---|---|
| Env / packaging | uv, Python 3.12, ruff | Same conventions as `lakehouse` |
| Compute | DuckDB (local file) | Handles 1.5B-row FHVHV aggregations on one machine; zero cost |
| Transform | dbt-core + dbt-duckdb | Industry-standard modeling; tests and docs for free |
| Orchestration | Dagster + dagster-dbt | dbt models become assets; partitions, backfills, asset checks, lineage UI |
| Ingestion | Dagster asset with plain `httpx` download | Files are already clean parquet; dlt adds little here (revisit if adding API sources) |
| Storage | Raw parquet on disk; DuckDB database for transforms | Simple. **Stretch:** DuckLake as the table format, as a nod to the Iceberg thesis |
| Serving data | Parquet exports of marts | Browser-friendly, cacheable, no backend |
| Hosting | Cloudflare Pages + R2 | Static, no egress fees, free tier likely sufficient |
| Frontend | **Decide at M3** — see §9 | |

### Serving size budget

The website must never touch trip-level data. Rule of thumb: each mart exported for the site
should be < 10 MB compressed, the full site payload < 50 MB.

- zone × month × service: ~265 zones × ~200 months × 4 services ≈ 210k rows — fine.
- hour × day-of-week × month × service: ≈ 135k rows — fine.
- zone × zone × month: ≈ 14M rows per service — **too big**; precompute top-N OD pairs or
  aggregate to borough × borough instead.

## 5. Data model (dbt)

```
models/
  staging/
    tlc/
      stg_yellow__2009.sql          # coordinate era, vendor_name schema
      stg_yellow__2010.sql          # coordinate era, vendor_id schema
      stg_yellow__2011_plus.sql     # zone era
      stg_green.sql
      stg_fhv.sql
      stg_fhvhv.sql
    reference/
      stg_taxi_zones.sql
  intermediate/
    int_trips__unioned.sql          # common grain: one row per trip, all services
    int_trips__enriched.sql         # zone/borough/CBD joins, duration, speed, quality flags
  marts/
    core/
      dim_zone.sql
      dim_date.sql
      dim_service.sql
      fct_trips_daily_zone.sql      # incremental by month; the workhorse aggregate
    stories/
      mart_congestion_pricing__zone_month.sql
      mart_congestion_pricing__hourly.sql
      mart_rideshare_share__borough_month.sql
      mart_covid_recovery__service_week.sql
    ops/
      mart_data_quality__month.sql  # feeds the "How this is built" page
```

Conventions:
- **Flag, don't filter.** Carry `is_valid_trip` and specific reason flags (`is_zero_duration`,
  `is_negative_fare`, `is_out_of_period`, `is_passenger_count_invalid`) through intermediate; marts
  decide. (The thesis Spark job filtered `passenger_count` to 1–6 in staging, which silently dropped
  rows — don't repeat that.)
- Large models are `incremental` with a monthly partition key aligned to Dagster partitions
  (`--vars '{year: 2024, month: 3}'`), using `delete+insert` on the month so reruns are idempotent
  (same retry-safety contract as `overwritePartitions()` in the thesis project).
- `int_trips__unioned` may be too large to materialize in full for FHVHV; start as a view over
  staging, materialize only `fct_trips_daily_zone`. Measure before optimizing.
- Contracts (`contract: enforced`) on all marts consumed by the website.

## 6. Orchestration (Dagster)

- Partition definition: `MonthlyPartitionsDefinition` per dataset (start dates from §2), or a
  multi-partition `dataset × month`. Prefer multi-partition if dagster-dbt handles it cleanly; validate in M1.
- Assets:
  - `raw_tlc_file` (partitioned): download if missing, verify row count > 0 and schema fingerprint;
    record schema-era as metadata.
  - dbt assets via `@dbt_assets`, partition-aware for incremental models.
  - `site_exports`: writes Parquet for each website mart, uploads to R2.
- Asset checks: row counts vs previous month (anomaly band), share of invalid trips per month,
  freshness of latest partition, schema fingerprint unchanged within an era.
- Sensor/schedule: monthly check of TLC for newly published months → materialize new partitions → rebuild exports.
- Run Dagster locally / on the homelab; the site does not depend on Dagster being up.

## 7. Data quality

Carry over from the thesis (`nyc_tlc_silver_quality.py`) and translate to dbt tests / Dagster asset checks:

| Thesis check | New home |
|---|---|
| `partition_row_count` | Dagster asset check on raw + fct partitions |
| `is_valid_trip_derivation` (duration > 0) | dbt flag column + `accepted_values`/expression test |
| Pandera schema validation | dbt model contracts + `not_null`/`accepted_range` (dbt-utils / dbt-expectations) |

New: per-month invalid-trip rate published to `mart_data_quality__month` and rendered on the site.

## 8. Repository layout

```
nyc-mobility/
  pyproject.toml / uv.lock
  README.md
  AGENTS.md                       # agent instructions, like the lakehouse repo
  orchestration/                  # Dagster code location
    nyc_mobility/
      definitions.py
      assets/{ingest,dbt,exports}.py
      checks.py
      partitions.py
      resources.py
  transform/                      # dbt project
    dbt_project.yml
    profiles.yml                  # duckdb path via env var
    models/ seeds/ macros/ tests/
  web/                            # site (framework decided at M3)
  scripts/                        # one-off utilities
  docs/                           # this plan, ADRs, write-ups
  data/                           # gitignored: raw parquet + duckdb file
  .github/workflows/
```

## 9. Frontend decision (M3)

| Option | Pros | Cons |
|---|---|---|
| Evidence.dev | SQL + Markdown pages, DuckDB built in, fastest to ship | Less control over custom maps/interactions |
| Observable Framework | Data loaders, Observable Plot, DuckDB-WASM, flexible | More JS; check project maintenance status before committing |
| Vite + React + DuckDB-WASM + MapLibre/deck.gl | Full control, most impressive maps | Most work; risk of frontend eating the project |

Recommendation: prototype the congestion-pricing page in Evidence first. Switch only if the map
interactions the story needs are not achievable. Check each option's release activity at decision time.

## 10. CI/CD

- On PR: `ruff check`, `ruff format --check`, `dbt build` against a **fixture DuckDB** built from 1
  month per dataset (sampled, committed or generated in CI), `dbt docs generate`.
- On main: build site from committed/exported marts, deploy to Cloudflare Pages; publish dbt docs as a static subpath.
- Full backfills run locally (too large for CI), triggered via Dagster.

## 11. Milestones

| # | Milestone | Exit criteria |
|---|---|---|
| M0 | Scaffold | Repo with uv/ruff, empty Dagster code location loads, dbt project compiles, CI green |
| M1 | Thin slice | yellow 2024–2025 + fhvhv 2024–2025 → staging → `fct_trips_daily_zone` via Dagster partitions; tests pass |
| M2 | Full history | All datasets, all eras from 2011; backfill runtime and DuckDB file size recorded; asset checks live |
| M3 | First story online | Congestion pricing page deployed; frontend choice made and written up as an ADR |
| M4 | v1 launch | Stories 2 and 3 + "How this is built" page + public dbt docs; README with screenshots |
| M5 | v2 | 2009–2010 coordinate era via spatial join; monthly auto-update sensor; one v2 story |

Keep M1 deliberately small: one dataset pair, two years, end to end to a deployed placeholder
page beats complete models with no site.

## 12. Carry-over from `lakehouse`

- Copy raw parquet from `lakehouse/data/` (or symlink) — no need to re-download 68 GB.
- Reuse the column rename maps in `src/etl/jobs/nyc_tlc_stg_trip_data.py` as the starting point for staging SQL.
- Reuse quality check definitions from `src/etl/jobs/nyc_tlc_silver_quality.py` and
  `src/etl/schemas/nyc_tlc_silver_quality_schemas.py`.
- Archive `lakehouse` with a README pointing to the thesis and to this project.

## 13. Open questions

- Single DuckDB file vs DuckLake from day one? (Default: single file; DuckLake as stretch.)
- Include `fhv` (non-HV bases) at all, or only yellow/green/fhvhv? Its schema is thin.
- Machine for full FHVHV backfill: confirm RAM/disk (DuckDB spills to disk, but give it a fast SSD).
- Domain name and project name.
