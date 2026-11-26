"""Yellow standardize on hand-made raw rows: one row per rule, so a failing test names the broken rule.

Every rule test runs against each raw layout (2009, 2010, 2011+). Tests describe changes with
Standardized column names; `Layout.raw` translates them to that layout's raw names and encodings.
"""

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime

import polars as pl
import pytest

from flag_drop.transformation.standardized.schemas import STANDARDIZED_YELLOW, validate_silver
from flag_drop.transformation.standardized.yellow import standardize


@dataclass(frozen=True)
class Layout:
    month: str  # the partition every test row belongs to; GOOD trips start on the 15th at 08:00
    schema: pl.Schema  # raw column names and dtypes, as TLC publishes them
    good_trip: dict  # a clean 15-minute trip
    names: dict[str, str]  # Standardized name -> raw name, for the columns tests change

    @property
    def year(self) -> int:
        return int(self.month[:4])

    def ts(self, day: int, hour: int, minute: int) -> datetime:
        return datetime(self.year, int(self.month[5:]), day, hour, minute)

    def raw(self, *changes: dict) -> pl.DataFrame:
        """One raw row per dict: good_trip with that dict's values swapped in."""
        rows = [self.good_trip | {self.names[k]: self._encode(self.names[k], v) for k, v in c.items()} for c in changes]
        return pl.DataFrame(rows, schema=self.schema)

    def _encode(self, raw_name: str, value: object) -> object:
        if isinstance(value, datetime) and self.schema[raw_name] == pl.String:
            return value.strftime("%Y-%m-%d %H:%M:%S")  # 2009/2010 store timestamps as text
        return value

    def run(self, *changes: dict) -> pl.DataFrame:
        return standardize(self.raw(*changes), self.month, f"yellow_tripdata_{self.month}.parquet")


Y2009 = Layout(
    month="2009-01",
    schema=pl.Schema(
        {
            "vendor_name": pl.String,
            "Trip_Pickup_DateTime": pl.String,
            "Trip_Dropoff_DateTime": pl.String,
            "Passenger_Count": pl.Int64,
            "Trip_Distance": pl.Float64,
            "Start_Lon": pl.Float64,
            "Start_Lat": pl.Float64,
            "Rate_Code": pl.Float64,
            "store_and_forward": pl.Float64,
            "End_Lon": pl.Float64,
            "End_Lat": pl.Float64,
            "Payment_Type": pl.String,
            "Fare_Amt": pl.Float64,
            "surcharge": pl.Float64,
            "mta_tax": pl.Float64,
            "Tip_Amt": pl.Float64,
            "Tolls_Amt": pl.Float64,
            "Total_Amt": pl.Float64,
        }
    ),
    good_trip={
        "vendor_name": "CMT",
        "Trip_Pickup_DateTime": "2009-01-15 08:00:00",
        "Trip_Dropoff_DateTime": "2009-01-15 08:15:00",
        "Passenger_Count": 1,
        "Trip_Distance": 2.5,
        "Start_Lon": -73.985,
        "Start_Lat": 40.758,
        "Rate_Code": None,  # never filled in 2009
        "store_and_forward": 0.0,
        "End_Lon": -73.965,
        "End_Lat": 40.776,
        "Payment_Type": "Cash",
        "Fare_Amt": 8.5,
        "surcharge": 0.0,
        "mta_tax": None,  # MTA tax starts 2009-03
        "Tip_Amt": 0.0,
        "Tolls_Amt": 0.0,
        "Total_Amt": 8.5,
    },
    names={
        "vendor_id": "vendor_name",
        "pickup_ts": "Trip_Pickup_DateTime",
        "dropoff_ts": "Trip_Dropoff_DateTime",
        "passenger_count": "Passenger_Count",
        "store_and_fwd": "store_and_forward",
        "payment_type": "Payment_Type",
        "fare_amount": "Fare_Amt",
    },
)

Y2010 = Layout(
    month="2010-01",
    schema=pl.Schema(
        {
            "vendor_id": pl.String,
            "pickup_datetime": pl.String,
            "dropoff_datetime": pl.String,
            "passenger_count": pl.Int64,
            "trip_distance": pl.Float64,
            "pickup_longitude": pl.Float64,
            "pickup_latitude": pl.Float64,
            "rate_code": pl.String,
            "store_and_fwd_flag": pl.String,
            "dropoff_longitude": pl.Float64,
            "dropoff_latitude": pl.Float64,
            "payment_type": pl.String,
            "fare_amount": pl.Float64,
            "surcharge": pl.Float64,
            "mta_tax": pl.Float64,
            "tip_amount": pl.Float64,
            "tolls_amount": pl.Float64,
            "total_amount": pl.Float64,
        }
    ),
    good_trip={
        "vendor_id": "VTS",
        "pickup_datetime": "2010-01-15 08:00:00",
        "dropoff_datetime": "2010-01-15 08:15:00",
        "passenger_count": 1,
        "trip_distance": 2.5,
        "pickup_longitude": -73.985,
        "pickup_latitude": 40.758,
        "rate_code": "1",
        "store_and_fwd_flag": "0",
        "dropoff_longitude": -73.965,
        "dropoff_latitude": 40.776,
        "payment_type": "CRD",
        "fare_amount": 8.1,
        "surcharge": 0.5,
        "mta_tax": 0.5,
        "tip_amount": 2.0,
        "tolls_amount": 0.0,
        "total_amount": 11.1,
    },
    names={
        "vendor_id": "vendor_id",
        "pickup_ts": "pickup_datetime",
        "dropoff_ts": "dropoff_datetime",
        "passenger_count": "passenger_count",
        "store_and_fwd": "store_and_fwd_flag",
        "payment_type": "payment_type",
        "fare_amount": "fare_amount",
        "rate_code_id": "rate_code",
    },
)

Y2024 = Layout(
    month="2024-01",
    schema=pl.Schema(
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
    ),
    good_trip={
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
    },
    names={
        "pickup_ts": "tpep_pickup_datetime",
        "dropoff_ts": "tpep_dropoff_datetime",
        "passenger_count": "passenger_count",
        "store_and_fwd": "store_and_fwd_flag",
        "fare_amount": "fare_amount",
        "airport_fee": "Airport_fee",
    },
)

LAYOUTS = [Y2009, Y2010, Y2024]


@pytest.fixture(params=LAYOUTS, ids=[layout.month[:4] for layout in LAYOUTS])
def layout(request) -> Layout:
    return request.param


def flags_set(row: dict) -> set[str]:
    return {f for f in STANDARDIZED_YELLOW.flags if row[f]}


# --- every layout ---------------------------------------------------------------------------------


def test_good_trip_is_valid_and_passes_the_contract(layout: Layout):
    df = layout.run({})
    validate_silver(df, STANDARDIZED_YELLOW, layout.month)
    row = df.row(0, named=True)
    assert flags_set(row) == set()
    assert row["is_valid_trip"] is True
    assert row["pickup_ts"] == layout.ts(15, 8, 0)
    assert row["trip_duration_min"] == 15.0


def test_lineage(layout: Layout):
    row = layout.run({}).row(0, named=True)
    assert row["source_month"] == layout.month
    assert row["source_file"] == f"yellow_tripdata_{layout.month}.parquet"
    assert row["schema_version"] == STANDARDIZED_YELLOW.schema_version_for(layout.month)


def test_columns_that_did_not_exist_yet_are_null(layout: Layout):
    row = layout.run({}).row(0, named=True)
    missing = ["cbd_congestion_fee", "request_source"]
    if layout.year < 2011:
        missing += ["pickup_location_id", "dropoff_location_id", "airport_fee"]
        missing += ["improvement_surcharge", "congestion_surcharge"]
    else:
        missing += ["pickup_longitude", "pickup_latitude", "dropoff_longitude", "dropoff_latitude"]
    for column in missing:
        assert row[column] is None, column


RULES: list[tuple[str, Callable[[Layout], dict], str]] = [
    ("zero-duration", lambda y: {"dropoff_ts": y.ts(15, 8, 0)}, "is_non_positive_duration"),
    ("dropoff-before-pickup", lambda y: {"dropoff_ts": y.ts(15, 7, 50)}, "is_non_positive_duration"),
    ("zero-fare", lambda _: {"fare_amount": 0.0}, "is_non_positive_fare"),
    ("negative-fare", lambda _: {"fare_amount": -14.2}, "is_non_positive_fare"),
    ("null-fare", lambda _: {"fare_amount": None}, "is_non_positive_fare"),  # missing counts as invalid
    (
        "pickup-in-previous-month",
        lambda y: {"pickup_ts": datetime(y.year - 1, 12, 31, 23, 50), "dropoff_ts": y.ts(1, 0, 5)},
        "is_out_of_period",
    ),
    ("zero-passengers", lambda _: {"passenger_count": 0}, "is_passenger_count_invalid"),
    ("seven-passengers", lambda _: {"passenger_count": 7}, "is_passenger_count_invalid"),
    ("null-passengers", lambda _: {"passenger_count": None}, "is_passenger_count_invalid"),  # missing = invalid
]


@pytest.mark.parametrize(("change", "flag"), [(c, f) for _, c, f in RULES], ids=[i for i, _, _ in RULES])
def test_each_rule_sets_only_its_own_flag(layout: Layout, change: Callable[[Layout], dict], flag: str):
    df = layout.run(change(layout))
    validate_silver(df, STANDARDIZED_YELLOW, layout.month)  # flagged rows still meet the contract
    row = df.row(0, named=True)
    assert flags_set(row) == {flag}
    assert row["is_valid_trip"] is False


@pytest.mark.parametrize("passengers", [1, 6])
def test_passenger_count_bounds_are_inclusive(layout: Layout, passengers: int):
    assert layout.run({"passenger_count": passengers})["is_passenger_count_invalid"].item() is False


def test_rows_are_flagged_not_dropped(layout: Layout):
    df = layout.run({}, {"fare_amount": -1.0, "passenger_count": 0}, {})
    assert df.height == 3
    assert df["is_valid_trip"].to_list() == [True, False, True]
    assert flags_set(df.row(1, named=True)) == {"is_non_positive_fare", "is_passenger_count_invalid"}


# --- per-layout encodings -------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("layout_", "raw_value", "expected"),
    [
        (Y2024, "Y", True),
        (Y2024, "N", False),
        (Y2024, None, None),
        (Y2009, 1.0, True),  # 2009: floats
        (Y2009, 0.0, False),
        (Y2009, None, None),
        (Y2010, "Y", True),  # 2010: every spelling at once
        (Y2010, "1", True),
        (Y2010, "1.0", True),
        (Y2010, "N", False),
        (Y2010, "0", False),
        (Y2010, "0.0", False),
        (Y2010, "nan", None),
        (Y2010, "*", None),
        (Y2010, "", None),
    ],
)
def test_store_and_fwd_becomes_boolean(layout_: Layout, raw_value, expected):
    assert layout_.run({"store_and_fwd": raw_value})["store_and_fwd"].item() is expected


@pytest.mark.parametrize("layout_", [Y2009, Y2010], ids=["2009", "2010"])
@pytest.mark.parametrize(("code", "vendor_id"), [("CMT", 1), ("VTS", 2), ("DDS", 101), ("XYZ", None)])
def test_legacy_vendor_codes(layout_: Layout, code: str, vendor_id: int | None):
    assert layout_.run({"vendor_id": code})["vendor_id"].item() == vendor_id


@pytest.mark.parametrize(
    ("layout_", "code", "payment_type"),
    [
        (Y2009, "CASH", 2),
        (Y2009, "Cash", 2),
        (Y2009, "Credit", 1),
        (Y2009, "CREDIT", 1),
        (Y2009, "No Charge", 3),
        (Y2009, "Dispute", 4),
        (Y2010, "CSH", 2),
        (Y2010, "Cas", 2),
        (Y2010, "CRD", 1),
        (Y2010, "Cre", 1),
        (Y2010, "No", 3),
        (Y2010, "NOC", 3),
        (Y2010, "Dis", 4),
        (Y2010, "NA", None),
    ],
)
def test_legacy_payment_codes(layout_: Layout, code: str, payment_type: int | None):
    assert layout_.run({"payment_type": code})["payment_type"].item() == payment_type


def test_legacy_trips_keep_coordinates():
    for layout_ in (Y2009, Y2010):
        row = layout_.run({}).row(0, named=True)
        assert (row["pickup_longitude"], row["pickup_latitude"]) == (-73.985, 40.758)
        assert (row["dropoff_longitude"], row["dropoff_latitude"]) == (-73.965, 40.776)


def test_2009_surcharge_becomes_extra_and_rate_code_stays_null():
    row = Y2009.run({}).row(0, named=True)
    assert row["extra"] == 0.0
    assert row["rate_code_id"] is None
    assert row["mta_tax"] is None


def test_2010_feb_mar_variant_int_rate_code_and_stray_index():
    """2010-02/03 store rate_code as Int64 and carry pandas' `__index_level_0__`."""
    raw = Y2010.raw({}).with_columns(pl.col("rate_code").cast(pl.Int64), pl.lit(0).alias("__index_level_0__"))
    df = standardize(raw, "2010-02", "yellow_tripdata_2010-02.parquet")
    assert df["rate_code_id"].item() == 1
    assert "__index_level_0__" not in df.columns


def test_2023_airport_fee_rename():
    assert Y2024.run({"airport_fee": 1.75})["airport_fee"].item() == 1.75
