"""FHVHV standardize on hand-made raw rows: one row per rule, so a failing test names the broken rule.

Runs against the 2019 layout (Int64 IDs, `airport_fee`/`wav_match_flag` as all-null Null columns) and
the 2025 layout (+ cbd_congestion_fee). Tests describe changes with Silver column names.
"""

from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, datetime

import polars as pl
import pytest

from flag_drop.transformation.silver.fhvhv import standardize
from flag_drop.transformation.silver.schemas import SILVER_FHVHV, validate_silver

SILVER_TO_RAW = {
    "pickup_ts": "pickup_datetime",
    "dropoff_ts": "dropoff_datetime",
    "base_passenger_fare": "base_passenger_fare",
    "hvfhs_license_num": "hvfhs_license_num",
    "shared_request": "shared_request_flag",
    "access_a_ride": "access_a_ride_flag",
    "wav_match": "wav_match_flag",
    "cbd_congestion_fee": "cbd_congestion_fee",
}


@dataclass(frozen=True)
class Layout:
    month: str  # GOOD trips start on the 15th at 08:00
    schema: pl.Schema

    @property
    def year(self) -> int:
        return int(self.month[:4])

    def ts(self, day: int, hour: int, minute: int) -> datetime:
        return datetime(self.year, int(self.month[5:]), day, hour, minute)

    @property
    def good_trip(self) -> dict:
        trip = {
            "hvfhs_license_num": "HV0003",
            "dispatching_base_num": "B03404",
            "originating_base_num": "B03404",
            "request_datetime": self.ts(15, 7, 52),
            "on_scene_datetime": self.ts(15, 7, 58),
            "pickup_datetime": self.ts(15, 8, 0),
            "dropoff_datetime": self.ts(15, 8, 15),
            "PULocationID": 161,
            "DOLocationID": 236,
            "trip_miles": 2.5,
            "trip_time": 900,
            "base_passenger_fare": 18.4,
            "tolls": 0.0,
            "bcf": 0.5,
            "sales_tax": 1.6,
            "congestion_surcharge": 2.75,
            "airport_fee": None,
            "tips": 3.0,
            "driver_pay": 14.1,
            "shared_request_flag": "N",
            "shared_match_flag": "N",
            "access_a_ride_flag": "N",
            "wav_request_flag": "N",
            "wav_match_flag": None,
        }
        if "cbd_congestion_fee" in self.schema:
            trip |= {"airport_fee": 0.0, "wav_match_flag": "N", "cbd_congestion_fee": 1.5}
        return trip

    def raw(self, *changes: dict) -> pl.DataFrame:
        rows = [self.good_trip | {SILVER_TO_RAW[k]: v for k, v in c.items()} for c in changes]
        return pl.DataFrame(rows, schema=self.schema)

    def run(self, *changes: dict) -> pl.DataFrame:
        return standardize(self.raw(*changes), self.month, f"fhvhv_tripdata_{self.month}.parquet")


_RAW_2019 = {
    "hvfhs_license_num": pl.String,
    "dispatching_base_num": pl.String,
    "originating_base_num": pl.String,
    "request_datetime": pl.Datetime("us"),
    "on_scene_datetime": pl.Datetime("us"),
    "pickup_datetime": pl.Datetime("us"),
    "dropoff_datetime": pl.Datetime("us"),
    "PULocationID": pl.Int64,
    "DOLocationID": pl.Int64,
    "trip_miles": pl.Float64,
    "trip_time": pl.Int64,
    "base_passenger_fare": pl.Float64,
    "tolls": pl.Float64,
    "bcf": pl.Float64,
    "sales_tax": pl.Float64,
    "congestion_surcharge": pl.Float64,
    "airport_fee": pl.Null,  # not reported until 2019-07
    "tips": pl.Float64,
    "driver_pay": pl.Float64,
    "shared_request_flag": pl.String,
    "shared_match_flag": pl.String,
    "access_a_ride_flag": pl.String,
    "wav_request_flag": pl.String,
    "wav_match_flag": pl.Null,  # not reported until 2019-04
}

F2019 = Layout(month="2019-02", schema=pl.Schema(_RAW_2019))
F2025 = Layout(
    month="2025-03",
    schema=pl.Schema(
        _RAW_2019
        | {
            "PULocationID": pl.Int32,
            "DOLocationID": pl.Int32,
            "airport_fee": pl.Float64,
            "wav_match_flag": pl.String,
            "cbd_congestion_fee": pl.Float64,
        }
    ),
)
LAYOUTS = [F2019, F2025]


@pytest.fixture(params=LAYOUTS, ids=[layout.month[:4] for layout in LAYOUTS])
def layout(request) -> Layout:
    return request.param


def flags_set(row: dict) -> set[str]:
    return {f for f in SILVER_FHVHV.flags if row[f]}


def test_good_trip_is_valid_and_passes_the_contract(layout: Layout):
    df = layout.run({})
    validate_silver(df, SILVER_FHVHV, layout.month)
    row = df.row(0, named=True)
    assert flags_set(row) == set()
    assert row["is_valid_trip"] is True
    assert row["pickup_ts"] == layout.ts(15, 8, 0)
    assert row["request_ts"] == layout.ts(15, 7, 52)
    assert (row["pickup_location_id"], row["dropoff_location_id"]) == (161, 236)
    assert row["trip_time_sec"] == 900
    assert row["trip_duration_min"] == 15.0


def test_lineage(layout: Layout):
    row = layout.run({}).row(0, named=True)
    assert row["source_month"] == date(layout.year, int(layout.month[5:]), 1)
    assert row["source_file"] == f"fhvhv_tripdata_{layout.month}.parquet"
    assert row["schema_version"] == SILVER_FHVHV.schema_version_for(layout.month)


def test_cbd_congestion_fee_is_null_before_2025():
    assert F2019.run({})["cbd_congestion_fee"].item() is None
    assert F2025.run({})["cbd_congestion_fee"].item() == 1.5


def test_null_typed_columns_become_typed_nulls():
    row = F2019.run({}).row(0, named=True)
    assert row["airport_fee"] is None
    assert row["wav_match"] is None


RULES: list[tuple[str, Callable[[Layout], dict], str]] = [
    ("zero-duration", lambda f: {"dropoff_ts": f.ts(15, 8, 0)}, "is_non_positive_duration"),
    ("dropoff-before-pickup", lambda f: {"dropoff_ts": f.ts(15, 7, 50)}, "is_non_positive_duration"),
    ("zero-fare", lambda _: {"base_passenger_fare": 0.0}, "is_non_positive_fare"),
    ("negative-fare", lambda _: {"base_passenger_fare": -5.0}, "is_non_positive_fare"),
    ("null-fare", lambda _: {"base_passenger_fare": None}, "is_non_positive_fare"),  # missing counts as invalid
    (
        "pickup-in-previous-month",
        lambda f: {"pickup_ts": datetime(f.year, int(f.month[5:]) - 1, 28, 23, 50), "dropoff_ts": f.ts(1, 0, 5)},
        "is_out_of_period",
    ),
]


@pytest.mark.parametrize(("change", "flag"), [(c, f) for _, c, f in RULES], ids=[i for i, _, _ in RULES])
def test_each_rule_sets_only_its_own_flag(layout: Layout, change: Callable[[Layout], dict], flag: str):
    df = layout.run(change(layout))
    validate_silver(df, SILVER_FHVHV, layout.month)  # flagged rows still meet the contract
    row = df.row(0, named=True)
    assert flags_set(row) == {flag}
    assert row["is_valid_trip"] is False


def test_rows_are_flagged_not_dropped(layout: Layout):
    df = layout.run({}, {"base_passenger_fare": -1.0}, {})
    assert df.height == 3
    assert df["is_valid_trip"].to_list() == [True, False, True]


@pytest.mark.parametrize(("raw_value", "expected"), [("Y", True), ("N", False), (" ", None), ("", None), (None, None)])
def test_yes_no_flags_become_boolean(layout: Layout, raw_value, expected):
    """Uber left access_a_ride_flag as ' ' in some years; blanks are 'not reported', not 'no'."""
    row = layout.run({"shared_request": raw_value, "access_a_ride": raw_value}).row(0, named=True)
    assert row["shared_request"] is expected
    assert row["access_a_ride"] is expected


def test_missing_license_fails_the_contract():
    with pytest.raises(ValueError, match="hvfhs_license_num"):
        validate_silver(F2025.run({"hvfhs_license_num": None}), SILVER_FHVHV, F2025.month)
