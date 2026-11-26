from datetime import UTC, date, datetime

import polars as pl
import pytest

from flag_drop.transformation.silver.schemas import (
    SILVER_FHVHV,
    SILVER_TABLES,
    SILVER_YELLOW,
    SilverTable,
    SilverValidationError,
    flag_rates,
    month_start,
    validate_silver,
)

FILLERS: dict[pl.DataType | type[pl.DataType], object] = {
    pl.Int32: 1,
    pl.Float64: 1.5,
    pl.String: "x",
    pl.Boolean: False,
    pl.Date: date(2024, 1, 1),
    pl.Datetime("us"): datetime(2024, 1, 15, 8, 0),
    pl.Datetime("us", "UTC"): datetime(2024, 2, 1, tzinfo=UTC),
}


def valid_frame(table: SilverTable, month: str = "2024-01", rows: int = 2) -> pl.DataFrame:
    """A frame that satisfies the contract: filler values, valid trips, correct lineage."""
    row = {name: FILLERS[dtype] for name, dtype in table.schema.items()}
    row |= {
        "source_month": month_start(month),
        "schema_version": table.schema_version_for(month),
        "is_valid_trip": True,
    }
    return pl.DataFrame([row] * rows, schema=table.schema)


@pytest.mark.parametrize("table", SILVER_TABLES.values(), ids=SILVER_TABLES.keys())
def test_valid_frame_passes(table: SilverTable):
    validate_silver(valid_frame(table), table, "2024-01")


@pytest.mark.parametrize(
    ("table", "month", "version"),
    [
        (SILVER_YELLOW, "2009-06", "2009-01"),
        (SILVER_YELLOW, "2023-01", "2011-01"),
        (SILVER_YELLOW, "2023-02", "2023-02"),
        (SILVER_YELLOW, "2026-09", "2026-06"),
        (SILVER_FHVHV, "2024-12", "2019-02"),
        (SILVER_FHVHV, "2025-01", "2025-01"),
    ],
)
def test_schema_version_for(table: SilverTable, month: str, version: str):
    assert table.schema_version_for(month) == version


def test_month_before_first_file_is_an_error():
    with pytest.raises(ValueError, match="no schema version"):
        SILVER_FHVHV.schema_version_for("2019-01")


def test_schema_drift_is_reported_by_column():
    df = valid_frame(SILVER_YELLOW).with_columns(pl.col("passenger_count").cast(pl.Int64)).drop("airport_fee")
    with pytest.raises(SilverValidationError) as exc:
        validate_silver(df.with_columns(pl.lit(1).alias("Airport_fee")), SILVER_YELLOW, "2024-01")
    problems = " ".join(exc.value.problems)
    assert "missing columns ['airport_fee']" in problems
    assert "unexpected columns ['Airport_fee']" in problems
    assert "passenger_count: Int64 != Int32" in problems


def test_column_order_matters():
    df = valid_frame(SILVER_FHVHV)
    with pytest.raises(SilverValidationError, match="column order"):
        validate_silver(df.select(reversed(df.columns)), SILVER_FHVHV, "2024-01")


def test_row_level_problems_are_all_reported():
    df = valid_frame(SILVER_FHVHV, rows=3).with_columns(
        pl.when(pl.int_range(pl.len()) == 0).then(None).otherwise(pl.col("pickup_ts")).alias("pickup_ts"),
        pl.lit(date(2024, 2, 1)).alias("source_month"),
        pl.lit(True).alias("is_non_positive_duration"),  # flag set but is_valid_trip still True
    )
    with pytest.raises(SilverValidationError) as exc:
        validate_silver(df, SILVER_FHVHV, "2024-01")
    assert exc.value.problems == [
        "1 null values in pickup_ts",
        "3 rows with a wrong value in source_month",
        "3 rows with a wrong value in is_valid_trip",
    ]


def test_wrong_schema_version_is_reported():
    df = valid_frame(SILVER_YELLOW, month="2025-03").with_columns(pl.lit("2023-02").alias("schema_version"))
    with pytest.raises(SilverValidationError, match="schema_version"):
        validate_silver(df, SILVER_YELLOW, "2025-03")


def test_flag_rates():
    df = valid_frame(SILVER_YELLOW, rows=4).with_columns(
        (pl.int_range(pl.len()) == 0).alias("is_non_positive_fare"),
        (pl.int_range(pl.len()) != 0).alias("is_valid_trip"),
    )
    rates = flag_rates(df, SILVER_YELLOW)
    assert rates["is_non_positive_fare"] == 0.25
    assert rates["is_valid_trip"] == 0.75
    assert rates["is_non_positive_duration"] == 0.0
