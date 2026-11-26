"""Silver table contracts: target schema, schema versions, and validation for each Silver Delta table.

Standardize functions (`silver/<dataset>.py`, raw DataFrame -> Silver DataFrame) must return a frame
that passes `validate_silver`. This module says *what* Silver looks like; standardize decides *how*.

Conventions (carried over from the lakehouse `YellowTripsPanderaSchema`):
- snake_case names; `_ts` for timestamps, `_location_id` for taxi zones, `_min`/`_sec` unit suffixes.
- Timestamps are naive `Datetime("us")` in NYC local time, as TLC publishes them (`ingested_at` is UTC).
- Money is Float64 (lakehouse used 32-bit float, which loses cents when summing billions of rows).
- Codes and counts are Int32. TLC's "Y"/"N" columns become Boolean.
- Columns that did not exist yet in a schema version are null, never 0 (e.g. `cbd_congestion_fee` before 2025).
- Derived business columns (has_tip, tip_ratio, year/month) moved to Gold; Silver keeps only what the
  quality flags need (`trip_duration_min`).

Quality flags follow "flag, don't filter": every raw row is kept; `is_valid_trip` is True only when
no other flag is set.

Missing data counts as invalid: a flag whose input is null is set to True (`fill_null(True)`), so a
trip with no fare, no passenger_count or no timestamps is never `is_valid_trip`. Flags are therefore
never null, and a high flag rate can mean "TLC stopped reporting this field", not "drivers misbehaved"
(e.g. yellow 2025-03: 22% of rows have no passenger_count). Gold can tell the two apart with
`<column> is null`.
"""

from dataclasses import dataclass
from datetime import date

import polars as pl

PARTITION_COLUMN = "source_month"


def month_start(month: str) -> date:
    """'YYYY-MM' (how code and Dagster name a month) -> the `source_month` value stored in Silver."""
    return date(int(month[:4]), int(month[5:7]), 1)


LINEAGE = pl.Schema(
    {
        # First day of the raw file's month (not of the pickup); Delta partition column. A Date, not
        # 'YYYY-MM' text, because Dagster's Delta IO manager selects a month by its start date.
        "source_month": pl.Date,
        "source_file": pl.String,  # raw filename, e.g. 'yellow_tripdata_2024-01.parquet'
        "schema_version": pl.String,  # first month of the raw layout this row came from, see schema_versions
        "ingested_at": pl.Datetime("us", "UTC"),  # when the Silver partition was written
    }
)


@dataclass(frozen=True)
class SilverTable:
    name: str
    schema: pl.Schema
    # Raw layout changes, as the first month of each layout ('YYYY-MM'), oldest first. A change is any
    # rename/add/remove of a column that conform has to branch on (pure dtype drift is just a cast).
    schema_versions: tuple[str, ...]
    # Row-level quality flags, excluding the `is_valid_trip` roll-up.
    flags: tuple[str, ...]
    # Columns that may never be null (lineage and flags are always added to this list).
    required: tuple[str, ...]

    @property
    def not_null(self) -> tuple[str, ...]:
        return (*self.required, *LINEAGE.names(), *self.flags, "is_valid_trip")

    def schema_version_for(self, source_month: str) -> str:
        """The raw layout in effect for a month: the latest schema version starting on or before it."""
        started = [v for v in self.schema_versions if v <= source_month]
        if not started:
            raise ValueError(f"{self.name}: no schema version covers {source_month}")
        return started[-1]


def _flags(*names: str) -> dict[str, pl.DataType]:
    return {name: pl.Boolean() for name in (*names, "is_valid_trip")}


# --- yellow ---------------------------------------------------------------------------------------
#
# Schema versions (verified against every raw file on 2026-11-24):
#   2009-01  vendor_name, Trip_Pickup_DateTime, Start_Lon/Lat, End_Lon/Lat, Fare_Amt, surcharge, ...
#   2010-01  vendor_id, pickup_datetime (String!), pickup/dropoff_longitude/latitude, rate_code, ...
#   2011-01  VendorID, tpep_*_datetime, PULocationID/DOLocationID (zones replace coordinates), extra, ...
#   2023-02  airport_fee renamed Airport_fee (and IDs become Int32 -- a cast, not a new version)
#   2025-01  + cbd_congestion_fee (Congestion Relief Zone toll, from 2025-01-05)
#   2026-06  + request_source
YELLOW_FLAGS = ("is_non_positive_duration", "is_non_positive_fare", "is_out_of_period", "is_passenger_count_invalid")

SILVER_YELLOW = SilverTable(
    name="silver_yellow",
    schema_versions=("2009-01", "2010-01", "2011-01", "2023-02", "2025-01", "2026-06"),
    flags=YELLOW_FLAGS,
    required=("pickup_ts", "dropoff_ts", "trip_duration_min"),
    schema=pl.Schema(
        {
            "vendor_id": pl.Int32,  # TLC ids; 2009-2010 CMT/VTS -> 1/2, DDS -> 101 (Flag Drop-only)
            "pickup_ts": pl.Datetime("us"),
            "dropoff_ts": pl.Datetime("us"),
            "passenger_count": pl.Int32,
            "trip_distance": pl.Float64,  # miles
            "rate_code_id": pl.Int32,
            "store_and_fwd": pl.Boolean,  # raw store_and_fwd_flag 'Y'/'N'
            "payment_type": pl.Int32,  # 2009-2010 text codes (CASH/CRD/...) map to TLC ids
            # Zones (2011+) and coordinates (2009-2010): each version fills one pair, the other is null.
            "pickup_location_id": pl.Int32,
            "dropoff_location_id": pl.Int32,
            "pickup_longitude": pl.Float64,
            "pickup_latitude": pl.Float64,
            "dropoff_longitude": pl.Float64,
            "dropoff_latitude": pl.Float64,
            "fare_amount": pl.Float64,
            "extra": pl.Float64,  # 2009 calls it surcharge
            "mta_tax": pl.Float64,
            "tip_amount": pl.Float64,
            "tolls_amount": pl.Float64,
            "improvement_surcharge": pl.Float64,
            "congestion_surcharge": pl.Float64,
            "airport_fee": pl.Float64,
            "cbd_congestion_fee": pl.Float64,
            "total_amount": pl.Float64,
            "request_source": pl.String,
            "trip_duration_min": pl.Float64,  # (dropoff_ts - pickup_ts) in minutes; may be <= 0
            **_flags(*YELLOW_FLAGS),
            **LINEAGE,
        }
    ),
)

# --- fhvhv ----------------------------------------------------------------------------------------
#
# Schema versions (verified 2026-11-24):
#   2019-02  first file; one layout until 2025 apart from dtype drift (location IDs Int64 -> Int32 in 2023-02)
#   2025-01  + cbd_congestion_fee
FHVHV_FLAGS = ("is_non_positive_duration", "is_non_positive_fare", "is_out_of_period")

SILVER_FHVHV = SilverTable(
    name="silver_fhvhv",
    schema_versions=("2019-02", "2025-01"),
    flags=FHVHV_FLAGS,
    required=("hvfhs_license_num", "pickup_ts", "dropoff_ts", "trip_duration_min"),
    schema=pl.Schema(
        {
            "hvfhs_license_num": pl.String,  # HV0003 Uber, HV0005 Lyft, ... (seed in Gold)
            "dispatching_base_num": pl.String,
            "originating_base_num": pl.String,
            "request_ts": pl.Datetime("us"),
            "on_scene_ts": pl.Datetime("us"),
            "pickup_ts": pl.Datetime("us"),
            "dropoff_ts": pl.Datetime("us"),
            "pickup_location_id": pl.Int32,
            "dropoff_location_id": pl.Int32,
            "trip_miles": pl.Float64,
            "trip_time_sec": pl.Int32,  # TLC-reported trip time; compare with trip_duration_min
            "base_passenger_fare": pl.Float64,
            "tolls": pl.Float64,
            "bcf": pl.Float64,  # Black Car Fund
            "sales_tax": pl.Float64,
            "congestion_surcharge": pl.Float64,
            "airport_fee": pl.Float64,
            "cbd_congestion_fee": pl.Float64,
            "tips": pl.Float64,
            "driver_pay": pl.Float64,
            # raw *_flag columns 'Y'/'N' -> Boolean (null when blank)
            "shared_request": pl.Boolean,
            "shared_match": pl.Boolean,
            "access_a_ride": pl.Boolean,
            "wav_request": pl.Boolean,
            "wav_match": pl.Boolean,
            "trip_duration_min": pl.Float64,
            **_flags(*FHVHV_FLAGS),
            **LINEAGE,
        }
    ),
)

SILVER_TABLES: dict[str, SilverTable] = {"yellow": SILVER_YELLOW, "fhvhv": SILVER_FHVHV}


# --- validation -----------------------------------------------------------------------------------


class SilverValidationError(ValueError):
    def __init__(self, table: str, problems: list[str]):
        self.problems = problems
        super().__init__(f"{table}: " + "; ".join(problems))


def validate_silver(df: pl.DataFrame, table: SilverTable, source_month: str) -> None:
    """Check a conformed frame before it is written to Delta. Raises with every problem found.

    1. Exact schema: same columns, same order, same dtypes (catches a missed cast or rename).
    2. No nulls in `table.not_null`.
    3. Every row carries this partition's `source_month` and the expected `schema_version`.
    4. `is_valid_trip` is exactly "no other flag set".
    """
    problems: list[str] = []

    if df.schema != table.schema:
        expected, actual = table.schema, df.schema
        missing = [c for c in expected if c not in actual]
        extra = [c for c in actual if c not in expected]
        wrong = [f"{c}: {actual[c]} != {expected[c]}" for c in expected if c in actual and actual[c] != expected[c]]
        if missing:
            problems.append(f"missing columns {missing}")
        if extra:
            problems.append(f"unexpected columns {extra}")
        if wrong:
            problems.append(f"wrong dtypes {wrong}")
        if not (missing or extra or wrong):
            problems.append("column order differs from the table schema")
        # Row checks below assume the right columns; stop here.
        raise SilverValidationError(table.name, problems)

    version = table.schema_version_for(source_month)
    any_flag = pl.any_horizontal(*table.flags)
    counts = df.select(
        *[pl.col(c).null_count().alias(f"null:{c}") for c in table.not_null],
        (pl.col("source_month") != month_start(source_month)).sum().alias("bad:source_month"),
        (pl.col("schema_version") != version).sum().alias("bad:schema_version"),
        (pl.col("is_valid_trip") == any_flag).sum().alias("bad:is_valid_trip"),
    ).row(0, named=True)

    for check, n in counts.items():
        if n:
            kind, column = check.split(":", 1)
            what = "null values" if kind == "null" else "rows with a wrong value"
            problems.append(f"{n} {what} in {column}")
    if problems:
        raise SilverValidationError(table.name, problems)


def flag_rates(df: pl.DataFrame, table: SilverTable) -> dict[str, float]:
    """Share of rows with each flag set (and share of valid trips), for asset metadata."""
    if df.height == 0:
        return {}
    row = df.select(pl.col([*table.flags, "is_valid_trip"]).mean()).row(0, named=True)
    return {name: round(float(rate), 6) for name, rate in row.items()}
