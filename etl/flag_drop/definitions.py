"""Dagster code location for Flag Drop."""

import os
from pathlib import Path

import dagster as dg
from dagster_deltalake import S3Config
from dagster_deltalake.config import ClientConfig
from dagster_deltalake_polars import DeltaLakePolarsIOManager

from flag_drop.assets.ingest import raw_assets
from flag_drop.assets.silver import IO_MANAGER_KEY, silver_assets
from flag_drop.resources import RawFiles, RawObjectStore, SilverTables, TlcSource

# Set in the Docker image; locally (editable install) default to the repo's data/ directory.
DATA_DIR = os.environ.get("FLAG_DROP_DATA_DIR", str(Path(__file__).resolve().parents[2] / "data"))

# Silver Delta tables on MinIO at {SILVER_ROOT_URI}/silver/<dataset> (e.g. s3://flag-drop/silver/yellow).
# The IO manager writes them; SilverTables lets assets create a table first (see utils/delta.py). Both must
# point at the same root with the same credentials.
silver_io_manager = DeltaLakePolarsIOManager(
    root_uri=dg.EnvVar("SILVER_ROOT_URI"),
    storage_options=S3Config(
        endpoint=dg.EnvVar("MINIO_ENDPOINT"),
        access_key_id=dg.EnvVar("MINIO_ACCESS_KEY"),
        secret_access_key=dg.EnvVar("MINIO_SECRET_KEY"),
        region="us-east-1",
    ),
    client_options=ClientConfig(allow_http=True),  # MinIO serves plain HTTP inside the homelab
)
silver_tables = SilverTables(
    root_uri=dg.EnvVar("SILVER_ROOT_URI"),
    endpoint_url=dg.EnvVar("MINIO_ENDPOINT"),
    access_key=dg.EnvVar("MINIO_ACCESS_KEY"),
    secret_key=dg.EnvVar("MINIO_SECRET_KEY"),
)

defs = dg.Definitions(
    assets=[*raw_assets, *silver_assets],
    resources={
        "raw_files": RawFiles(data_dir=DATA_DIR),
        IO_MANAGER_KEY: silver_io_manager,
        "silver_tables": silver_tables,
        "raw_store": RawObjectStore(
            endpoint_url=dg.EnvVar("MINIO_ENDPOINT"),
            access_key=dg.EnvVar("MINIO_ACCESS_KEY"),
            secret_key=dg.EnvVar("MINIO_SECRET_KEY"),
            bucket=dg.EnvVar("MINIO_RAW_BUCKET"),
            prefix=dg.EnvVar("MINIO_RAW_PREFIX"),
        ),
        "tlc_source": TlcSource(),
    },
)
