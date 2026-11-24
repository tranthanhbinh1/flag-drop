"""Dagster code location for Flag Drop."""

import os
from pathlib import Path

import dagster as dg

from flag_drop.assets.ingest import raw_assets
from flag_drop.resources import RawFiles, RawObjectStore, TlcSource

# Set in the Docker image; locally (editable install) default to the repo's data/ directory.
DATA_DIR = os.environ.get("FLAG_DROP_DATA_DIR", str(Path(__file__).resolve().parents[2] / "data"))

defs = dg.Definitions(
    assets=raw_assets,
    resources={
        "raw_files": RawFiles(data_dir=DATA_DIR),
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
