"""Yellow raw -> Silver (standardized schema). One rename map per raw layout; `_decode_legacy` handles 2009-2010.

2009 and 2010 differ from 2011+ in more than names (checked against every 2009/2010 file, 2026-11-26):
- timestamps are strings ('YYYY-MM-DD HH:MM:SS'; all parse),
- vendor and payment type are text codes, not TLC ids,
- coordinates instead of taxi zones (zones stay null, as columns that did not exist yet),
- 2009 has no rate code (all null) and no mta_tax before 2009-03,
- 2010-02/03 store rate_code as Int64 and carry a stray `__index_level_0__` column (dropped by the select).
"""

from datetime import UTC, datetime

import polars as pl

from .schemas import SILVER_YELLOW, month_start

RENAME_2009 = {
    "vendor_name": "vendor_id",
    "Trip_Pickup_DateTime": "pickup_ts",
    "Trip_Dropoff_DateTime": "dropoff_ts",
    "Passenger_Count": "passenger_count",
    "Trip_Distance": "trip_distance",
    "Start_Lon": "pickup_longitude",
    "Start_Lat": "pickup_latitude",
    "Rate_Code": "rate_code_id",
    "store_and_forward": "store_and_fwd",
    "End_Lon": "dropoff_longitude",
    "End_Lat": "dropoff_latitude",
    "Payment_Type": "payment_type",
    "Fare_Amt": "fare_amount",
    "surcharge": "extra",
    "Tip_Amt": "tip_amount",
    "Tolls_Amt": "tolls_amount",
    "Total_Amt": "total_amount",
}

RENAME_2010 = {
    "pickup_datetime": "pickup_ts",
    "dropoff_datetime": "dropoff_ts",
    "rate_code": "rate_code_id",
    "store_and_fwd_flag": "store_and_fwd",
    "surcharge": "extra",
}

RENAME_2011 = {
    "VendorID": "vendor_id",
    "tpep_pickup_datetime": "pickup_ts",
    "tpep_dropoff_datetime": "dropoff_ts",
    "RatecodeID": "rate_code_id",
    "store_and_fwd_flag": "store_and_fwd",
    "PULocationID": "pickup_location_id",
    "DOLocationID": "dropoff_location_id",
    "Airport_fee": "airport_fee",  # 2023-02+ spelling
}

# Schema version -> rename map. Versions after 2011-01 only add columns, so they share RENAME_2011.
RENAMES = {"2009-01": RENAME_2009, "2010-01": RENAME_2010}

# 2009-2010 text codes -> TLC data-dictionary ids. Anything else (e.g. 2010 "NA") becomes null.
# DDS (Digital Dispatch Systems, ~6% of 2009 trips) has no TLC id, and TLC has used 1-7 since 2011, so it gets
# one from Flag Drop's own range (>= 100) that TLC will not collide with. The Gold vendor seed names it.
VENDOR_DDS = 101
VENDOR_IDS = {"CMT": 1, "VTS": 2, "DDS": VENDOR_DDS}  # Creative Mobile Technologies, VeriFone (now Curb)
PAYMENT_TYPES = {  # matched upper-cased; 2009 spells words out, 2010 uses 3-letter codes
    "CREDIT": 1, "CRE": 1, "CRD": 1,
    "CASH": 2, "CAS": 2, "CSH": 2,
    "NO CHARGE": 3, "NO": 3, "NOC": 3,
    "DISPUTE": 4, "DIS": 4,
}  # fmt: skip

# Raw store-and-forward, as text: 2011+ uses Y/N, 2009 floats 0.0/1.0, 2010 a mix of all of them
# plus "nan", "", "2" and "*" (those become null).
STORE_AND_FWD = {"Y": True, "1": True, "1.0": True, "N": False, "0": False, "0.0": False}

LEGACY_TS_FORMAT = "%Y-%m-%d %H:%M:%S"


def _decode_legacy(df: pl.DataFrame) -> pl.DataFrame:
    """2009/2010 only: parse string timestamps and turn text codes into TLC ids."""
    return df.with_columns(
        pl.col("pickup_ts", "dropoff_ts").str.to_datetime(LEGACY_TS_FORMAT, time_unit="us", strict=True),
        pl.col("vendor_id").replace_strict(VENDOR_IDS, default=None, return_dtype=pl.Int32),
        pl.col("payment_type").str.to_uppercase().replace_strict(PAYMENT_TYPES, default=None, return_dtype=pl.Int32),
    )


def standardize(raw: pl.DataFrame, source_month: str, source_file: str) -> pl.DataFrame:
    version = SILVER_YELLOW.schema_version_for(source_month)

    df = raw.rename(RENAMES.get(version, RENAME_2011), strict=False)
    if version in RENAMES:
        df = _decode_legacy(df)
    df = df.with_columns(
        pl.col("store_and_fwd").cast(pl.String).replace_strict(STORE_AND_FWD, default=None, return_dtype=pl.Boolean)
    )

    schema = SILVER_YELLOW.schema
    df = df.with_columns(pl.lit(None, dtype=schema[c]).alias(c) for c in schema if c not in df.columns)

    df = df.cast({c: t for c, t in schema.items() if c in df.columns}, strict=True)

    df = (
        df.with_columns(
            ((pl.col("dropoff_ts") - pl.col("pickup_ts")).dt.total_microseconds() / 60e6).alias("trip_duration_min"),
        )
        .with_columns(
            (pl.col("trip_duration_min") <= 0).fill_null(True).alias("is_non_positive_duration"),
            (pl.col("pickup_ts").dt.strftime("%Y-%m") != source_month).fill_null(True).alias("is_out_of_period"),
            (pl.col("fare_amount") <= 0).fill_null(True).alias("is_non_positive_fare"),
            (~pl.col("passenger_count").is_between(1, 6)).fill_null(True).alias("is_passenger_count_invalid"),
        )
        .with_columns(
            (~pl.any_horizontal(SILVER_YELLOW.flags)).alias("is_valid_trip"),
        )
    )

    df = df.with_columns(
        pl.lit(month_start(source_month)).alias("source_month"),
        pl.lit(source_file).alias("source_file"),
        pl.lit(version).alias("schema_version"),
        pl.lit(datetime.now(UTC), dtype=pl.Datetime("us", "UTC")).alias("ingested_at"),
    )

    return df.select(schema.names())
