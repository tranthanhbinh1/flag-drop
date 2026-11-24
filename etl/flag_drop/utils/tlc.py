"""TLC raw file helpers: naming, download, validation. No Dagster here, so it is easy to test."""

import hashlib
import os
from dataclasses import dataclass
from pathlib import Path

import httpx
import polars as pl

TLC_BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"

# CloudFront answers 403 (not 404) for months TLC has not published.
NOT_PUBLISHED = {403, 404}


class RawFileInvalid(ValueError):
    """The file is not a readable, non-empty Parquet file."""


@dataclass(frozen=True)
class ParquetInfo:
    rows: int
    columns: dict[str, str]
    schema_fingerprint: str


def tlc_filename(dataset: str, month: str) -> str:
    """('yellow', '2024-01') -> 'yellow_tripdata_2024-01.parquet'."""
    return f"{dataset}_tripdata_{month}.parquet"


def raw_relpath(dataset: str, month: str) -> str:
    """Relative path used both on disk and in MinIO: '{year}/{filename}'."""
    return f"{month[:4]}/{tlc_filename(dataset, month)}"


def inspect_parquet(path: Path) -> ParquetInfo:
    """Read row count and schema from Parquet metadata. Raises RawFileInvalid if unusable."""
    try:
        lf = pl.scan_parquet(path)
        schema = lf.collect_schema()
        rows = lf.select(pl.len()).collect().item()
    except Exception as exc:  # polars raises several error types for corrupt/non-parquet input
        raise RawFileInvalid(f"{path.name}: not readable as Parquet ({exc})") from exc
    if rows == 0:
        raise RawFileInvalid(f"{path.name}: has 0 rows")
    columns = {name: str(dtype) for name, dtype in schema.items()}
    signature = ",".join(f"{name}:{dtype}" for name, dtype in columns.items())
    fingerprint = hashlib.sha256(signature.encode()).hexdigest()[:12]
    return ParquetInfo(rows=rows, columns=columns, schema_fingerprint=fingerprint)


def fetch(client: httpx.Client, url: str, dest: Path) -> bool:
    """Download url to dest atomically. Returns False if TLC has not published the file.

    Streams to a temp file next to dest, checks the byte count and that it is valid Parquet,
    then renames. On any failure no partial file is left behind.
    """
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_name(dest.name + ".part")
    try:
        with client.stream("GET", url) as response:
            if response.status_code in NOT_PUBLISHED:
                return False
            response.raise_for_status()
            with tmp.open("wb") as f:
                for chunk in response.iter_bytes(chunk_size=8 * 1024 * 1024):
                    f.write(chunk)
            expected = response.headers.get("content-length")
        if expected is not None and tmp.stat().st_size != int(expected):
            raise OSError(f"{dest.name}: got {tmp.stat().st_size} bytes, expected {expected}")
        inspect_parquet(tmp)
        os.replace(tmp, dest)
        return True
    finally:
        tmp.unlink(missing_ok=True)
