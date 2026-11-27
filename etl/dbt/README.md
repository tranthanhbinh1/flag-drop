# dbt project (Gold)

dbt-duckdb reads the Silver Delta tables on MinIO with `delta_scan` (see `models/staging/silver/_silver__sources.yml`)
and writes Gold marts back to `s3://flag-drop/gold`. DuckDB is compute only: `data/flag_drop.duckdb` holds views and
scratch tables and can be deleted at any time.

`profiles.yml` lives here and contains no secrets; it reads the `MINIO_*` and `SILVER_ROOT_URI` env vars.

```bash
make dbt ARGS="build"            # any dbt command, with the repo .env loaded
make dbt ARGS="build -s staging"
```

Layers: `staging/` (thin views over Silver and seeds), `intermediate/` (unions, joins), `marts/` (what the site reads).
