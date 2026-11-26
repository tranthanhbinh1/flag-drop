from datetime import date
from pathlib import Path

import polars as pl
from deltalake import DeltaTable

from flag_drop.utils.delta import ensure_table

SCHEMA = pl.Schema({"x": pl.Int64, "source_month": pl.Date})


def test_creates_an_empty_partitioned_table_once(tmp_path: Path):
    uri = str(tmp_path / "silver" / "t")

    assert ensure_table(uri, SCHEMA, ["source_month"]) is True
    assert ensure_table(uri, SCHEMA, ["source_month"]) is False

    dt = DeltaTable(uri)
    assert dt.version() == 0  # the second call did not commit anything
    assert dt.metadata().partition_columns == ["source_month"]
    df = pl.read_delta(uri)
    assert df.schema == SCHEMA
    assert df.height == 0


def test_table_accepts_a_date_partition_overwrite(tmp_path: Path):
    """The write the IO manager does after ensure_table: predicate on the partition's start date."""
    from deltalake import write_deltalake

    uri = str(tmp_path / "t")
    ensure_table(uri, SCHEMA, ["source_month"])
    for month, rows in [(date(2024, 1, 1), 3), (date(2024, 2, 1), 2), (date(2024, 1, 1), 5)]:
        df = pl.DataFrame({"x": range(rows), "source_month": [month] * rows}, schema=SCHEMA)
        write_deltalake(uri, df, mode="overwrite", predicate=f"source_month = '{month:%Y-%m-%d}'")

    counts = dict(pl.read_delta(uri).group_by("source_month").len().iter_rows())
    assert counts == {date(2024, 1, 1): 5, date(2024, 2, 1): 2}
