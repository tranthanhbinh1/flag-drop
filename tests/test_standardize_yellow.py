"""Yellow standardize on hand-made raw rows: one row per rule, so a failing test names the broken rule."""

from datetime import datetime

import polars as pl
import pytest

from flag_drop.transformation.standardized.schemas import STANDARDIZED_YELLOW, validate_silver
from flag_drop.transformation.standardized.yellow import standardize

MONTH = "2024-01"
FILE = f"yellow_tripdata_{MONTH}.parquet"

# Raw 2023-02+ layout, with TLC's column names and dtypes.
RAW_SCHEMA = pl.Schema(
    {
        "VendorID": pl.Int32,
        "tpep_pickup_datetime": pl.Datetime("us"),
        "tpep_dropoff_datetime": pl.Datetime("us"),
        "passenger_count": pl.Int64,
        "trip_distance": pl.Float64,
        "RatecodeID": pl.Int64,
        "store_and_fwd_flag": pl.String,
        "PULocationID": pl.Int32,
        "DOLocationID": pl.Int32,
        "payment_type": pl.Int64,
        "fare_amount": pl.Float64,
        "extra": pl.Float64,
        "mta_tax": pl.Float64,
        "tip_amount": pl.Float64,
        "tolls_amount": pl.Float64,
        "improvement_surcharge": pl.Float64,
        "total_amount": pl.Float64,
        "congestion_surcharge": pl.Float64,
        "Airport_fee": pl.Float64,
    }
)

# A clean 15-minute trip. Every test starts from this and changes one thing.
GOOD_TRIP = {
    "VendorID": 2,
    "tpep_pickup_datetime": datetime(2024, 1, 15, 8, 0),
    "tpep_dropoff_datetime": datetime(2024, 1, 15, 8, 15),
    "passenger_count": 1,
    "trip_distance": 2.5,
    "RatecodeID": 1,
    "store_and_fwd_flag": "N",
    "PULocationID": 161,
    "DOLocationID": 236,
    "payment_type": 1,
    "fare_amount": 14.2,
    "extra": 1.0,
    "mta_tax": 0.5,
    "tip_amount": 3.0,
    "tolls_amount": 0.0,
    "improvement_surcharge": 1.0,
    "total_amount": 22.2,
    "congestion_surcharge": 2.5,
    "Airport_fee": 0.0,
}


def raw(*changes: dict) -> pl.DataFrame:
    """One raw row per dict: GOOD_TRIP with that dict's values swapped in."""
    return pl.DataFrame([GOOD_TRIP | c for c in changes], schema=RAW_SCHEMA)


def run(*changes: dict) -> pl.DataFrame:
    return standardize(raw(*changes), MONTH, FILE)


def flags_set(row: dict) -> set[str]:
    return {f for f in STANDARDIZED_YELLOW.flags if row[f]}


def test_good_trip_is_valid_and_passes_the_contract():
    df = run({})
    validate_silver(df, STANDARDIZED_YELLOW, MONTH)
    row = df.row(0, named=True)
    assert flags_set(row) == set()
    assert row["is_valid_trip"] is True
    assert row["trip_duration_min"] == 15.0


def test_renames_and_lineage():
    row = run({"Airport_fee": 1.75}).row(0, named=True)
    assert row["pickup_location_id"] == 161
    assert row["airport_fee"] == 1.75
    assert row["source_month"] == MONTH
    assert row["source_file"] == FILE
    assert row["schema_version"] == "2023-02"


def test_columns_that_did_not_exist_yet_are_null():
    row = run({}).row(0, named=True)
    for column in ("cbd_congestion_fee", "request_source", "pickup_longitude", "dropoff_latitude"):
        assert row[column] is None, column


@pytest.mark.parametrize(
    ("raw_value", "expected"),
    [("Y", True), ("N", False), (None, None), ("0", None)],  # "0": 2010-style code, not Y/N
)
def test_store_and_fwd_becomes_boolean(raw_value, expected):
    assert run({"store_and_fwd_flag": raw_value})["store_and_fwd"].item() is expected


@pytest.mark.parametrize(
    ("change", "flag"),
    [
        ({"tpep_dropoff_datetime": GOOD_TRIP["tpep_pickup_datetime"]}, "is_non_positive_duration"),
        ({"tpep_dropoff_datetime": datetime(2024, 1, 15, 7, 50)}, "is_non_positive_duration"),
        ({"fare_amount": 0.0}, "is_non_positive_fare"),
        ({"fare_amount": -14.2}, "is_non_positive_fare"),
        ({"fare_amount": None}, "is_non_positive_fare"),  # missing counts as invalid
        (
            {
                "tpep_pickup_datetime": datetime(2023, 12, 31, 23, 50),
                "tpep_dropoff_datetime": datetime(2024, 1, 1, 0, 5),
            },
            "is_out_of_period",
        ),
        ({"passenger_count": 0}, "is_passenger_count_invalid"),
        ({"passenger_count": 7}, "is_passenger_count_invalid"),
        ({"passenger_count": None}, "is_passenger_count_invalid"),  # missing counts as invalid
    ],
    ids=[
        "zero-duration",
        "dropoff-before-pickup",
        "zero-fare",
        "negative-fare",
        "null-fare",
        "pickup-in-previous-month",
        "zero-passengers",
        "seven-passengers",
        "null-passengers",
    ],
)
def test_each_rule_sets_only_its_own_flag(change: dict, flag: str):
    df = run(change)
    validate_silver(df, STANDARDIZED_YELLOW, MONTH)  # flagged rows still meet the contract
    row = df.row(0, named=True)
    assert flags_set(row) == {flag}
    assert row["is_valid_trip"] is False


@pytest.mark.parametrize("passengers", [1, 6])
def test_passenger_count_bounds_are_inclusive(passengers: int):
    assert run({"passenger_count": passengers})["is_passenger_count_invalid"].item() is False


def test_rows_are_flagged_not_dropped():
    df = run({}, {"fare_amount": -1.0, "passenger_count": 0}, {})
    assert df.height == 3
    assert df["is_valid_trip"].to_list() == [True, False, True]
    assert flags_set(df.row(1, named=True)) == {"is_non_positive_fare", "is_passenger_count_invalid"}
