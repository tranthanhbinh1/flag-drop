# One image for all three Dagster services (code location, daemon, webserver).
# Build context is the repo root: docker compose -f deploy/docker-compose.yaml build
FROM python:3.12-slim

COPY --from=ghcr.io/astral-sh/uv:0.7.13 /uv /uvx /bin/

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PROJECT_ENVIRONMENT=/opt/venv \
    PATH=/opt/venv/bin:$PATH \
    DAGSTER_HOME=/opt/dagster/home \
    FLAG_DROP_DATA_DIR=/opt/flag-drop/data

WORKDIR /opt/flag-drop

# Dependencies first (cached layer): pipeline group only, no dev tools.
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --frozen --no-default-groups --group pipeline --no-install-project

# Project code: the flag_drop package and the dbt project (etl/dbt).
COPY etl ./etl
RUN uv sync --frozen --no-default-groups --group pipeline

COPY deploy/dagster.yaml deploy/workspace.yaml /opt/dagster/home/
