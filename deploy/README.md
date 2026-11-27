# Deployment (Dagster on Docker Compose)

Three services built from one image: `code_location` (gRPC code server; runs execute here),
`dagster_daemon`, `webserver` (http://localhost:3000). MinIO and Postgres are shared infra from
`~/projects/infra-homelab/docker-composes/backend`, reached over the external `backend_infra` network.

## Prerequisites

```bash
# Shared infra (MinIO :9000/:9001, Postgres :5432)
cd ~/projects/infra-homelab/docker-composes/backend && docker compose up -d minio postgres

# Secrets: copy and fill in (MinIO key `flag-drop`, Postgres user/db `dagster`)
cp .env.example .env
```

One-time setup already done on the workstation (2026-11-24); repeat only on a fresh machine:

- MinIO: user `flag-drop` with policy `flag-drop-rw` (read/write on buckets `raw` and `flag-drop`), bucket `flag-drop`.
- Postgres: `CREATE USER dagster ...; CREATE DATABASE dagster OWNER dagster;`
- `data/raw` → symlink to the raw TLC parquet dir; set the same absolute path as `RAW_DATA_DIR` in `.env`.

## Run

From the repo root:

```bash
docker compose -f deploy/docker-compose.yaml --env-file .env up -d --build   # start / redeploy code
docker compose -f deploy/docker-compose.yaml --env-file .env logs -f code_location
docker compose -f deploy/docker-compose.yaml --env-file .env down
```

After a code change, rebuilding `code_location` is enough; the webserver reloads the location.

## Concurrency pools

Stored in Postgres, so set once (re-run after recreating the `dagster` database):

```bash
docker exec flag-drop-code_location-1 dagster instance concurrency set silver 4
docker exec flag-drop-code_location-1 dagster instance concurrency set silver_fhvhv 2  # ~15 GB per fhvhv month
docker exec flag-drop-code_location-1 dagster instance concurrency set duckdb 1
docker exec flag-drop-code_location-1 dagster instance concurrency set minio_upload 4
```

Run-level cap (`max_concurrent_runs: 8`) is in `dagster.yaml`.
