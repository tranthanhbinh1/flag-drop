# Flag Drop — Project Plan

> **Flag Drop: 15 years of NYC trips, in data.** Successor to the `lakehouse` (Spark/Iceberg/Airflow) thesis project.
> Status: plan, pre-scaffold. Written 2026-11-23; stack revised 2026-11-23 (see §14).

## 1. Goal

Build a public website that tells data stories about 15+ years of NYC for-hire mobility, backed by a
small, production-grade lakehouse stack (Polars + Delta Lake → DuckDB + dbt, orchestrated by Dagster)
that is visible and explained on the site.

Portfolio positioning — this project should demonstrate what the day job (Spark at scale) does not:

- Analytics engineering: layered dbt models, tests, contracts, documentation.
- Lakehouse fundamentals without a cluster: Delta Lake tables (ACID partition overwrites, time travel,
  schema enforcement, OPTIMIZE/VACUUM) written by Polars via delta-rs. Directly transferable to Databricks.
- Choosing the right engine per layer: dataframe code for messy per-file conformance, SQL for modeling.
- Asset-based orchestration with partitions, backfills, and data quality checks (Dagster).
- Cost-aware architecture: single-node compute, static hosting, near-zero running cost.
- Product thinking: a site people actually read, with a point of view, not a generic dashboard.

### Non-goals

- No Spark, no cluster, no Kubernetes. Single machine is a feature (Delta via delta-rs, no JVM).
- No live/streaming data. TLC publishes monthly with a ~2-month lag; batch is correct.
- No backend API for the website in v1. Everything served is precomputed.
- No ML in v1 (forecasting can be a later story).

## 2. Data inventory (MinIO `raw` bucket, verified 2026-11-24 after the top-up)

> Every month TLC has published is materialized by the `raw_<dataset>` assets: 598 files, 81.9 GB.
> The asset metadata (row count, schema fingerprint) is the live inventory; this table is a snapshot.

| Dataset | Coverage | Notes |
|---|---|---|
| yellow | 2009-01 → 2026-09 (213 files) | ~14M rows/month (2009) → ~3.5M (2025) |
| green | 2014-01 → 2026-09 (153) | Street-hail outside Manhattan core |
| fhv | 2015-01 → 2026-08 (140) | Sparse schema; base-level FHV |
| fhvhv | 2019-02 → 2026-09 (92) | Uber/Lyft/Via; ~20M rows/month, ~1.5B rows total — the heavy one |

Found during the top-up: 9 archive files in `lakehouse/data` (and their MinIO copies) were CloudFront 403
error pages saved as `.parquet` (yellow 2015-07…12 and 2025-10…12), and yellow 2009-04 was missing.
All were re-downloaded into `data/landing/` and re-uploaded; `data/landing` holds 67 files (8.4 GB).
The 9 broken copies were deleted from `lakehouse/data` the same day. Downstream steps must still read raw
files through `RawFiles.candidates` (landing first) or from MinIO, since those months exist only there.

Source URL pattern (for re-download / new months):
`https://d37ci6vzurychx.cloudfront.net/trip-data/{dataset}_tripdata_{YYYY-MM}.parquet`

### Done: top-up download to latest published month (2026-11-24)

Gaps to fill: green after 2025-09, fhv after 2025-08, fhvhv after 2025-09, yellow after 2025-12,
through whatever TLC has published in 2026 (~2-month lag).

- [x] Implement as the `raw_<dataset>` Dagster assets (download-if-missing per monthly
      partition), not a one-off script, so the same code handles future months via the sensor.
- [x] Start from `lakehouse/scripts/bootstrap/initial_load.py`, fixing its known issues:
  - its URLs use `{dataset}_trip_data_` but TLC files are `{dataset}_tripdata_` (match the local filenames);
  - the yellow fetch has no `raise_for_status()`, so a 403 error body would be saved as `.parquet`;
  - downloads load the whole file into memory; stream to a temp file and atomically rename instead.
- [x] Treat HTTP 403 as "not published yet" (TLC's response for missing months), not as a failure.
- [x] Validate each new file (readable by Polars, row count > 0, schema fingerprint) before it counts as materialized.
- [x] Write new files into `data/landing/{year}/` (owned by Flag Drop, same naming). `data/raw` (the
      lakehouse archive) stays read-only; `RawFiles.candidates` prefers `landing/` over `raw/` and skips invalid copies. MinIO holds the union.
- [x] **Prerequisite:** run the MinIO defined in `infra-homelab/docker-composes/backend` (holds the data).
      Done 2026-11-24 (orphaned `golden-retriever-minio-1` removed).
- [x] Upload to the existing layout: bucket `raw`, key `data/{year}/{dataset}_tripdata_{YYYY-MM}.parquet`.
      As of 2026-11-23 it already holds all local files from 2011–2025 (517 objects); missing are yellow
      2009 (11) + 2010 (12) and every newly downloaded month. Skip objects that already exist with the same size.
      New months: upload as part of `raw_<dataset>` so local and MinIO stay in sync.
- [x] Use a dedicated MinIO access key for Flag Drop (user `flag-drop`, read/write on `raw` + `flag-drop`), not the root user. The S3 API
      (boto3 or `obstore`) gets endpoint/credentials from env vars, never committed.
- [x] Refresh the coverage table above once done.

### Schema versions (yellow) — handled explicitly in the Silver conform step (Polars)

| Schema version | Columns | Location |
|---|---|---|
| 2009 | `vendor_name`, `Trip_Pickup_DateTime`, `Fare_Amt`, `Start_Lon/Lat`, `End_Lon/Lat`, … | raw lat/lon |
| 2010 | `vendor_id`, `pickup_datetime`, `pickup_longitude/latitude`, … | raw lat/lon |
| 2011+ | `VendorID`, `tpep_pickup_datetime`, `PULocationID`, `DOLocationID`, … | taxi zone IDs |
| 2023-02+ | `airport_fee` becomes `Airport_fee`; IDs Int64 → Int32 | taxi zone IDs |
| 2025+ | adds `cbd_congestion_fee` | taxi zone IDs |
| 2026-06+ | adds `request_source` | taxi zone IDs |

The authoritative schema-version list per dataset (incl. fhvhv: 2019-02, 2025-01) lives in `etl/flag_drop/transformation/standardized/schemas.py`.

Key wrinkle: **2009–2010 trips have coordinates, everything after has zone IDs only.** Silver keeps
both (nullable lat/lon and nullable zone IDs in one yellow table). Unifying them requires a spatial
join of points to taxi zone polygons, done in the Gold layer with the DuckDB `spatial` extension
(Polars' geo support is not mature enough). This is a deliberate, write-up-worthy engineering problem (v2 scope).

Other known issues to handle: column-name casing drift across years (`Airport_fee` vs `airport_fee`,
`vendorID` vs `VendorID` in green), type drift (int vs double for IDs/passenger counts), out-of-range
timestamps (trips stamped in the wrong month/year), negative fares, zero-duration trips.

Reference data needed (dbt seeds):
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

Medallion layout with an engine per layer: **Polars owns Bronze → Silver, DuckDB owns Gold.**

```
TLC CloudFront ──► BRONZE  raw parquet files, as published (data/raw/{year}/…)
                        │      immutable landing zone; never rewritten
                        │
                 Dagster  (dataset × month partitions, sensors for new months)
                        │
                        ▼  Polars: one raw file → one Silver partition
                   SILVER  Delta Lake tables, one per dataset (data/silver/{dataset}/)
                        │      partitioned by source_month; conformed schema across versions,
                        │      typed, row-level quality flags; partition overwrite per run
                        │
                        ▼  dbt-duckdb reads Silver via delta_scan()
                   GOLD    DuckDB file (data/flag_drop.duckdb)
                             staging       thin 1:1 views over Silver sources
                             intermediate  unified trips across services, zone/CBD enrichment
                             marts         small, story-shaped aggregates, contracts enforced
                        │
                 export marts → Parquet (+ zone GeoJSON)  ──► R2 / static assets
                        │
                 Website (static) reads Parquet via DuckDB-WASM or build-time loaders
                 hosted on Cloudflare Pages
```

Why the split: per-file schema conformance across 15 years of drift is easiest as tested Python
(Polars); joins/aggregates/contracts are easiest as SQL (dbt). Delta gives each monthly partition
an atomic, idempotent overwrite, so Dagster backfills can run in parallel — a single DuckDB file
allows only one writer, which would have serialized the FHVHV backfill. DuckDB then only holds Gold,
which stays small.

### Technology decisions

| Concern | Choice | Why |
|---|---|---|
| Env / packaging | uv, Python 3.12, ruff; deps in `pipeline` + `dev` groups, pinned `==` | Same conventions as `lakehouse`; bump versions deliberately |
| Bronze → Silver compute | Polars (lazy/streaming where possible) | Per-file conform logic in plain, unit-testable Python; fast single-node |
| Silver storage | Delta Lake via `deltalake` (delta-rs) | ACID partition overwrite, schema enforcement, time travel; same format as Databricks |
| Silver validation | Native Polars (strict cast to a target `pl.Schema`, expression checks) + Delta schema enforcement + Dagster asset checks | No extra dependency; checks show up in the Dagster UI |
| Gold compute | DuckDB (local file) | Handles 1.5B-row FHVHV aggregations on one machine; zero cost |
| Gold transform | dbt-core + dbt-duckdb | Industry-standard modeling; tests, contracts and docs for free |
| Orchestration | Dagster + dagster-dbt | Polars assets and dbt models in one lineage graph; partitions, backfills, asset checks |
| Ingestion | Dagster asset with plain `httpx` download | Files are already clean parquet; dlt adds little here (revisit if adding API sources) |
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

## 5. Data model

### Silver (Delta, written by Polars)

One Delta table per dataset: `silver/yellow`, `silver/green`, `silver/fhv`, `silver/fhvhv`.

- **Partition column `source_month`** = the month of the raw file, *not* the pickup month. This keeps
  one Dagster partition = one raw file = one Delta partition, so overwrites are exact and idempotent.
  Trips stamped outside their file's month are kept and flagged (`is_out_of_period`).
- **Write mode:** overwrite only the run's partition (`mode="overwrite"` with a
  `source_month = 'YYYY-MM'` predicate, i.e. replaceWhere). Never append.
- **Conformed schema per dataset:** canonical snake_case names, fixed types, schema-version differences resolved
  (rename maps per version, casts, `Airport_fee` → `airport_fee`). Yellow keeps nullable
  `pickup_longitude/latitude` (2009–2010) alongside nullable `pu_location_id` (2011+).
- **Lineage columns:** `source_month`, `source_file`, `schema_version`, `ingested_at`.
- **Row-level quality flags** (computable from the row alone): `is_non_positive_duration`,
  `is_non_positive_fare`, `is_out_of_period`, `is_passenger_count_invalid` (yellow; outside 1–6),
  rolled up into `is_valid_trip`. `<= 0` on purpose: a $0 fare or 0-minute trip is not a real trip.
- **Missing data counts as invalid.** A flag whose input is null is True (`fill_null(True)`), so flags
  are never null and a trip missing fare/passenger_count/timestamps is never `is_valid_trip`. Caveat for
  the site: a rising flag rate can be TLC no longer reporting a field rather than bad trips — yellow
  `passenger_count` is null for 4.7% of 2024-01 rows but 22% of 2025-03. Data-quality charts should split
  "missing" (`<column> is null`) from "out of range".
- Standardize logic lives in pure functions (`raw DataFrame → Silver DataFrame`), separate from Dagster,
  with unit tests on small fixtures.
- Table maintenance: `OPTIMIZE` (compaction) and `VACUUM` as a scheduled Dagster job after backfills.

### Gold (dbt-duckdb, reading Silver)

```
models/
  staging/
    silver/
      _silver__sources.yml          # sources with external_location: delta_scan('data/silver/<dataset>')
      stg_silver__yellow.sql        # thin: select/rename to project conventions, no schema-version logic
      stg_silver__green.sql
      stg_silver__fhv.sql
      stg_silver__fhvhv.sql
    reference/
      stg_taxi_zones.sql
  intermediate/
    int_trips__unioned.sql          # common grain: one row per trip, all services
    int_trips__enriched.sql         # zone/borough/CBD joins, duration, speed, enrichment flags
  marts/
    core/
      dim_zone.sql
      dim_date.sql
      dim_service.sql
      fct_trips_daily_zone.sql      # incremental by source_month; the workhorse aggregate
    stories/
      mart_congestion_pricing__zone_month.sql
      mart_congestion_pricing__hourly.sql
      mart_rideshare_share__borough_month.sql
      mart_covid_recovery__service_week.sql
    ops/
      mart_data_quality__month.sql  # feeds the "How this is built" page
```

Conventions:
- **Flag, don't filter.** Row-level flags are set in Silver; flags that need joins (e.g. unknown zone)
  are added in `int_trips__enriched`. Nothing upstream of marts drops rows; marts decide. (The thesis
  Spark job filtered `passenger_count` to 1–6 in staging, which silently dropped rows — don't repeat that.)
- Large Gold models are `incremental` with the same monthly key as Dagster partitions
  (`--vars '{source_month: 2024-03}'`), using `delete+insert` on the month so reruns are idempotent.
  `delta_scan` filters on `source_month` should prune to one Delta partition — verify in M1.
- `int_trips__unioned` stays a view over Silver; materialize only `fct_trips_daily_zone` and the marts.
  Measure before optimizing.
- Contracts (`contract: enforced`) on all marts consumed by the website.

## 6. Orchestration (Dagster)

- Partition definition: one `MonthlyPartitionsDefinition` per dataset, starting at its first published
  month (yellow 2009-01, green 2014-01, fhv 2015-01, fhvhv 2019-02; `partitions.py`). Decided in M1:
  a shared multi-partition would contain combinations that can never exist (green 2009).
- Assets:
  - `raw_<dataset>` (Bronze, partitioned; `assets/ingest.py`): use the local file or download it,
    verify row count > 0 and record a schema fingerprint, upload to MinIO if missing or a different size.
    A month TLC has not published (HTTP 403) yields no materialization rather than a failure.
  - `silver_<dataset>` (partitioned): Polars conform of one raw file, partition overwrite into the
    Delta table; emit row count, flag rates, Delta version as metadata. Writes are explicit in the
    asset (not via an IO manager) to keep the Delta mechanics visible; revisit `dagster-deltalake-polars` later.
  - dbt assets via `@dbt_assets`, partition-aware for incremental models; dbt sources map to the
    `silver_<dataset>` asset keys so lineage is one graph.
  - `site_exports`: writes Parquet for each website mart, uploads to R2.
  - `silver_maintenance` job: OPTIMIZE + VACUUM per Silver table.
- Concurrency: Silver partitions can run in parallel (disjoint Delta partitions). Confirm delta-rs
  handles concurrent commits on the local filesystem (retry on commit conflict); otherwise cap with a
  Dagster concurrency pool. dbt (DuckDB writer) runs with a pool of 1.
- Asset checks: row counts vs previous month (anomaly band), share of invalid trips per month,
  freshness of latest partition, schema fingerprint unchanged within a schema version, Bronze vs Silver row
  count equality (Silver never drops rows).
- Sensor/schedule: monthly check of TLC for newly published months → materialize new partitions → rebuild exports.
- Deployment: see §10 (`dagster dev` first, Docker Compose from M2). The site does not depend on Dagster being up.

## 7. Data quality

Carry over from the thesis (`nyc_tlc_silver_quality.py`) and translate:

| Thesis check | New home |
|---|---|
| `partition_row_count` | Dagster asset check on raw, Silver and fct partitions |
| `is_valid_trip_derivation` (duration > 0) | Silver flag column (Polars) + asset check on flag rate |
| Pandera schema validation | Silver: strict cast to the target `pl.Schema` per dataset (fails on drift) + Delta schema enforcement on write; Gold: dbt contracts + `not_null`/`accepted_range` |

New: per-month invalid-trip rate published to `mart_data_quality__month` and rendered on the site.

## 8. Repository layout

```
flag-drop/
  pyproject.toml / uv.lock
  README.md
  AGENTS.md                       # agent instructions, like the lakehouse repo
  etl/                            # the whole pipeline: Python package + dbt project
    dg.toml                       # dg project root (`make dev` runs from here)
    flag_drop/                    # Python package; only assets/, resources, partitions, definitions import Dagster
      definitions.py
      assets/{ingest,silver,dbt,exports}.py
      transformation/standardized/
        schemas.py                # Silver contracts: pl.Schema, schema versions, validation
        {yellow,green,fhv,fhvhv}.py  # pure Polars standardize functions + rename maps
      utils/tlc.py                # TLC download + raw file validation
      checks.py
      partitions.py
      resources.py
    dbt/                          # dbt project (Gold), its own root
      dbt_project.yml
      profiles.yml                # duckdb path via env var
      models/ seeds/ macros/ tests/
  tests/                          # pytest: conform functions on small fixtures
  deploy/                         # docker-compose.yaml, dagster.yaml, Dockerfile (phase 2)
  web/                            # site (framework decided at M3)
  scripts/                        # one-off utilities
  docs/                           # this plan, ADRs, write-ups
  data/                           # gitignored
    raw/                          # Bronze archive (symlink to lakehouse/data, read-only)
    landing/                      # Bronze months downloaded by Flag Drop
    silver/                       # Delta tables
    flag_drop.duckdb              # Gold
    exports/                      # site Parquet
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

## 10. Deployment and CI/CD

### Runtime environment

Everything runs on the main workstation (20 cores, 62 GB RAM, ~1.1 TB free as of 2026-11-23).
Shared infra comes from `~/projects/infra-homelab/docker-composes/backend` (canonical; other projects'
MinIO/Postgres containers must not hold its ports):

| Service | Used for | Address (host / in-network) |
|---|---|---|
| MinIO | Bronze copy (`raw` bucket, `data/{year}/…`); later possibly Silver | `localhost:9000` / `minio:9000` |
| Postgres (`postgres_container`) | Dagster run/event/schedule storage (phase 2) | `localhost:5432` / `postgres:5432` |

Prerequisite (one-time, **done 2026-11-24**): remove the orphaned empty `golden-retriever-minio-1` and start the
infra-homelab MinIO:

```bash
docker rm -f golden-retriever-minio-1
cd ~/projects/infra-homelab/docker-composes/backend && docker compose up -d minio postgres
```

All endpoints and credentials come from env vars (`.env`, gitignored; `.env.example` committed). The code
never hardcodes `localhost` vs `minio`, so the same code runs in both phases. Flag Drop gets its own
MinIO access key (read/write on `raw`) and its own Postgres user/database, never the root credentials.

### Phase 1 (M0–M1): `dagster dev`

- `uv run dagster dev` from the repo root (`[tool.dagster]` in `pyproject.toml` points at `flag_drop.definitions`).
- `DAGSTER_HOME=~/.dagster/flag-drop` (persistent, outside the repo) with default SQLite storage.
- Data on local disk under `data/`; MinIO at `localhost:9000`.
- Good enough for iteration and the M1 thin slice; not intended to run unattended for months.

### Phase 2 (M2+): Docker Compose in this repo

`deploy/docker-compose.yaml` with three services, joining the infra-homelab network as external
(network `backend_infra`, verified). **Status 2026-11-24: stack scaffolded and running** with an empty
code location, ahead of M0/M1; see `deploy/README.md`:

```
code_location    image built from this repo (uv sync --group pipeline); gRPC code server;
                 runs execute here as subprocesses (DefaultRunLauncher)
dagster_daemon   schedules, sensors, backfills, QueuedRunCoordinator
webserver        UI on :3000
```

- `dagster.yaml`: Postgres storage (database `dagster` on `postgres_container`), `QueuedRunCoordinator`
  with `max_concurrent_runs: 8`, and pools: `silver` = 6 (one FHVHV month ≈ 2–3 GB in Polars),
  `duckdb` = 1 (single writer), `minio_upload` = 4.
- Run launcher: **DefaultRunLauncher**, not DockerRunLauncher. No Docker socket, no per-run images,
  data volumes mounted into one container only. Per-run isolation isn't worth it for a single-user pipeline.
- Bind mounts into `code_location`: `data/raw` (resolved symlink target, read-only),
  `data/silver`, `data/flag_drop.duckdb` dir, `data/exports`.
- `mem_limit: 40g`, `cpus: 12` on `code_location`, leaving headroom for the desktop and other stacks.
- Backfills: one run per partition (fits Delta partition overwrite, easy retry); revisit
  `BackfillPolicy.single_run` only if per-run overhead dominates.
- Code changes: `docker compose build code_location && docker compose up -d code_location`; the
  webserver and daemon pick up the reloaded location.

One-time Postgres setup for phase 2:

```sql
CREATE USER dagster WITH PASSWORD '...';   -- from .env
CREATE DATABASE dagster OWNER dagster;
```

### CI/CD

- On PR: `ruff check`, `ruff format --check`, `pytest` (conform functions), then build fixture Silver
  Delta tables from 1 sampled month per dataset and run `dbt build` against them; `dbt docs generate`.
- On main: build site from committed/exported marts, deploy to Cloudflare Pages; publish dbt docs as a static subpath.
- Full backfills run locally (too large for CI), triggered via Dagster.

## 11. Milestones

| # | Milestone | Exit criteria |
|---|---|---|
| M0 | Scaffold | Repo with uv/ruff/pytest; empty Dagster code location loads; dbt project compiles; Polars → Delta → `delta_scan` smoke test passes; CI green |
| M1 | Thin slice | `raw_<dataset>` assets top up all datasets to the latest 2026 month (§2 TODO); yellow + fhvhv 2024–2025: raw → Silver (Polars, Delta) → dbt → `fct_trips_daily_zone` via Dagster partitions; tests pass; parallel Silver writes and `delta_scan` partition pruning verified |
| M2 | Full history | Phase 2 Compose deployment live (§10); all datasets, all schema versions from 2011 in Silver; backfill runtime, Silver size and DuckDB size recorded; asset checks live; OPTIMIZE/VACUUM job |
| M3 | First story online | Congestion pricing page deployed; frontend choice made and written up as an ADR |
| M4 | v1 launch | Stories 2 and 3 + "How this is built" page + public dbt docs; README with screenshots |
| M5 | v2 | 2009–2010 coordinate era via spatial join; monthly auto-update sensor; one v2 story |

Keep M1 deliberately small: one dataset pair, two years, end to end to a deployed placeholder
page beats complete models with no site.

## 12. Carry-over from `lakehouse`

- Symlink raw parquet from `lakehouse/data/` as `data/raw` — no need to re-download 68 GB.
- Port the column rename maps in `src/etl/jobs/nyc_tlc_stg_trip_data.py` into `transformation/standardized/*.py`
  (Python dict → Python dict; Spark `withColumnRenamed` → Polars `rename`).
- Translate the Pandera schemas in `src/etl/schemas/nyc_tlc_silver_quality_schemas.py` into target
  `pl.Schema` definitions + Polars check expressions (no Pandera dependency), and check definitions from `src/etl/jobs/nyc_tlc_silver_quality.py`.
- Archive `lakehouse` with a README pointing to the thesis and to this project.

## 13. Open questions

- Include `fhv` (non-HV bases) at all, or only yellow/green/fhvhv? Its schema is thin.
- Machine for full FHVHV backfill: confirm RAM/disk. One FHVHV month is ~2–3 GB in Polars memory;
  this bounds how many Silver partitions can run in parallel.
- Should Silver live on R2 later (shareable dataset, future API)? Delta on object storage needs
  conditional-write support; check delta-rs + R2 at that point.
- MinIO's role beyond a Bronze copy: should Dagster read Bronze from MinIO (`s3://raw/...`) instead of
  local disk, and should Silver Delta tables live on MinIO too? Delta on S3-compatible storage needs
  safe concurrent commits (conditional writes or a locking provider); check delta-rs + MinIO support.
  Same question applies to R2 later.
- Domain name.

## 14. Decision log

- **2026-11-23 — Project name: Flag Drop** (was working name `nyc-mobility`).
- **2026-11-23 — Silver on Delta Lake written by Polars; DuckDB only for Gold.** Replaces "DuckDB +
  dbt for every layer, DuckLake as stretch". Reasons: per-file schema-version conformance is clearer as tested
  Python; Delta partition overwrites make Silver idempotent and allow parallel backfills (single-writer
  DuckDB would serialize them); Delta skills transfer to Databricks. Costs accepted: two engines and
  dialects, Silver tests as Polars expressions/asset checks instead of dbt, Delta table maintenance.
- **2026-11-23 — No Pandera.** Silver validation uses native Polars (strict cast to a target
  `pl.Schema`, expression-based checks) plus Delta schema enforcement, surfaced as Dagster asset checks.
  Trade-off: no built-in failure-case reporting; acceptable since rows are flagged, not dropped.
- **2026-11-23 — Dagster deployment: `dagster dev` for M0–M1, then Docker Compose (code location +
  daemon + webserver, DefaultRunLauncher) on the workstation, using infra-homelab's MinIO and Postgres.**
  Rejected: DockerRunLauncher (socket access, per-run images, volume sprawl for no single-user benefit);
  systemd processes (less reproducible, less portfolio value).
- **2026-11-24 — Raw assets: one asset per dataset (`raw_yellow`, …) with its own monthly partitions;
  new downloads go to `data/landing/`, not into the lakehouse directory.** Keeps impossible partitions
  out of the UI, maps 1:1 to `silver_<dataset>`, and lets Compose mount the old archive read-only.
- **2026-11-24 — `orchestration/` renamed `etl/`, holding both the Python package (`etl/flag_drop`) and the
  dbt project (`etl/dbt`).** The package does far more than orchestration (download, conform, validation);
  one folder for the whole pipeline, with dbt in its own subfolder so dbt's `tests/`, `target/` and
  packages do not mix with Python. Rejected: `worker/` (clashes with Dagster's run worker), dbt files flat in `etl/`.
