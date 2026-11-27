# Flag Drop developer commands. `make` alone lists them.

# Phase 1 Dagster instance: SQLite storage, run history and logs live here, outside the repo.
export DAGSTER_HOME ?= $(HOME)/.dagster/flag-drop
COMPOSE := docker compose -f deploy/docker-compose.yaml --env-file .env

.DEFAULT_GOAL := help
.PHONY: help dev defs dagster-home pools test lint check up down logs

help: ## List targets
	@grep -hE '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk -F ':.*## ' '{printf "  %-14s %s\n", $$1, $$2}'

dagster-home: ## Create DAGSTER_HOME and sync its dagster.yaml from deploy/dagster.dev.yaml
	@mkdir -p "$(DAGSTER_HOME)"
	@cp deploy/dagster.dev.yaml "$(DAGSTER_HOME)/dagster.yaml"

# dg needs the project root (etl/dg.toml) as cwd; Dagster would then look for .env there,
# so pass the repo-root .env explicitly.

# make dagster, reserve `dev` keyword for future backend server
# Named pool limits can't go in dagster.yaml (1.13 only takes default_limit); they live in the instance's
# storage. Memory budget on the 62 GB box: silver 4 x ~4 GB (yellow) + silver_fhvhv 2 x ~15 GB = ~46 GB.
pools: dagster-home ## Set the Dagster pool limits on the Phase 1 instance
	@uv run --frozen dagster instance concurrency set silver 4
	@uv run --frozen dagster instance concurrency set silver_fhvhv 2
	@uv run --frozen dagster instance concurrency set duckdb 1
	@uv run --frozen dagster instance concurrency set minio_upload 4

dagster: pools ## Run `dg dev` (webserver + daemon) on localhost:3004 (fixed)
	cd etl && uv run --frozen --env-file ../.env dg dev -p 3004

defs: ## Check that the Dagster definitions load
	cd etl && uv run --frozen --env-file ../.env dg check defs

test: ## Run pytest
	uv run --frozen pytest -q

lint: ## Run all pre-commit hooks (ruff, pyrefly, ...) on every file
	uv run --frozen pre-commit run --all-files

check: lint test ## Lint + tests

up: ## Build and start the Compose stack (phase 2)
	$(COMPOSE) up -d --build

down: ## Stop the Compose stack
	$(COMPOSE) down

logs: ## Follow code-location logs of the Compose stack
	$(COMPOSE) logs -f code_location
