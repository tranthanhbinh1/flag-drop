from datetime import UTC, datetime

import polars as pl
import pytest

from flag_drop.transformation.standardized.schemas import (
    STANDARDIZED_FHVHV,
    STANDARDIZED_TABLES,
    STANDARDIZED_YELLOW,
    StandardizedTable,
    StandardizedValidationError,
    flag_rates,
    validate_silver,
)

FILLERS: dict[pl.DataType | type[pl.DataType], object] = {
    pl.Int32: 1,
    pl.Float64: 1.5,
    pl.String: "x",
    pl.Boolean: False,
    pl.Datetime("us"): datetime(2024, 1, 15, 8, 0),
    pl.Datetime("us", "UTC"): datetime(2024, 2, 1, tzinfo=UTC),
}


def valid_frame(table: StandardizedTable, month: str = "2024-01", rows: int = 2) -> pl.DataFrame:
    """A frame that satisfies the contract: filler values, valid trips, correct lineage."""
    row = {name: FILLERS[dtype] for name, dtype in table.schema.items()}
    row |= {"source_month": month, "schema_version": table.schema_version_for(month), "is_valid_trip": True}
    return pl.DataFrame([row] * rows, schema=table.schema)


@pytest.mark.parametrize("table", STANDARDIZED_TABLES.values(), ids=STANDARDIZED_TABLES.keys())
def test_valid_frame_passes(table: StandardizedTable):
    validate_silver(valid_frame(table), table, "2024-01")


@pytest.mark.parametrize(
    ("table", "month", "version"),
    [
        (STANDARDIZED_YELLOW, "2009-06", "2009-01"),
        (STANDARDIZED_YELLOW, "2023-01", "2011-01"),
        (STANDARDIZED_YELLOW, "2023-02", "2023-02"),
        (STANDARDIZED_YELLOW, "2026-09", "2026-06"),
        (STANDARDIZED_FHVHV, "2024-12", "2019-02"),
        (STANDARDIZED_FHVHV, "2025-01", "2025-01"),
    ],
)
def test_schema_version_for(table: StandardizedTable, month: str, version: str):
    assert table.schema_version_for(month) == version


def test_month_before_first_file_is_an_error():
    with pytest.raises(ValueError, match="no schema version"):
        STANDARDIZED_FHVHV.schema_version_for("2019-01")


def test_schema_drift_is_reported_by_column():
    df = valid_frame(STANDARDIZED_YELLOW).with_columns(pl.col("passenger_count").cast(pl.Int64)).drop("airport_fee")
    with pytest.raises(StandardizedValidationError) as exc:
        validate_silver(df.with_columns(pl.lit(1).alias("Airport_fee")), STANDARDIZED_YELLOW, "2024-01")
    problems = " ".join(exc.value.problems)
    assert "missing columns ['airport_fee']" in problems
    assert "unexpected columns ['Airport_fee']" in problems
    assert "passenger_count: Int64 != Int32" in problems


def test_column_order_matters():
    df = valid_frame(STANDARDIZED_FHVHV)
    with pytest.raises(StandardizedValidationError, match="column order"):
        validate_silver(df.select(reversed(df.columns)), STANDARDIZED_FHVHV, "2024-01")


def test_row_level_problems_are_all_reported():
    df = valid_frame(STANDARDIZED_FHVHV, rows=3).with_columns(
        pl.when(pl.int_range(pl.len()) == 0).then(None).otherwise(pl.col("pickup_ts")).alias("pickup_ts"),
        pl.lit("2024-02").alias("source_month"),
        pl.lit(True).alias("is_non_positive_duration"),  # flag set but is_valid_trip still True
    )
    with pytest.raises(StandardizedValidationError) as exc:
        validate_silver(df, STANDARDIZED_FHVHV, "2024-01")
    assert exc.value.problems == [
        "1 null values in pickup_ts",
        "3 rows with a wrong value in source_month",
        "3 rows with a wrong value in is_valid_trip",
    ]


def test_wrong_schema_version_is_reported():
    df = valid_frame(STANDARDIZED_YELLOW, month="2025-03").with_columns(pl.lit("2023-02").alias("schema_version"))
    with pytest.raises(StandardizedValidationError, match="schema_version"):
        validate_silver(df, STANDARDIZED_YELLOW, "2025-03")


def test_flag_rates():
    df = valid_frame(STANDARDIZED_YELLOW, rows=4).with_columns(
        (pl.int_range(pl.len()) == 0).alias("is_non_positive_fare"),
        (pl.int_range(pl.len()) != 0).alias("is_valid_trip"),
    )
    rates = flag_rates(df, STANDARDIZED_YELLOW)
    assert rates["is_non_positive_fare"] == 0.25
    assert rates["is_valid_trip"] == 0.75
    assert rates["is_non_positive_duration"] == 0.0
