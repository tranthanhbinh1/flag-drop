"""
Silver writes go through Dagster's `DeltaLakePolarsIOManager`, which replaces one partition per run
(`mode="overwrite"` + a `source_month = '<month start>'` predicate). It only adds that predicate when it
can open an *existing* table; otherwise (`except Exception: pass` in dagster-deltalake 0.29.31) it does a
plain overwrite, which on a table another run just created would replace every month.

So each Silver asset calls `ensure_table` before returning its frame to enure correctness when writing.
"""

import polars as pl
from deltalake import DeltaTable, write_deltalake
from deltalake.exceptions import CommitFailedError


def ensure_table(
    uri: str, schema: pl.Schema, partition_by: list[str], storage_options: dict[str, str] | None = None
) -> bool:
    """Create an empty Delta table with `schema` if none exists. Returns True if this call created it."""
    if DeltaTable.is_deltatable(uri, storage_options=storage_options):
        return False
    try:
        write_deltalake(
            uri, pl.DataFrame(schema=schema), mode="ignore", partition_by=partition_by, storage_options=storage_options
        )
    except CommitFailedError:
        if DeltaTable.is_deltatable(uri, storage_options=storage_options):  # another writer created it first
            return False
        raise
    return True
