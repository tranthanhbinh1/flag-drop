"""FHVHV (Uber/Lyft/Via/Juno) raw -> Silver (standardized schema). One layout since 2019-02, so one rename map.

What the raw files look like (checked against every file 2019-02..2026-09, values profiled in five
months, 2026-11-26):
- the only layout change is 2025-01 adding `cbd_congestion_fee`; everything else is dtype drift
  (location IDs Int64 -> Int32 in 2023-02, and `airport_fee`/`wav_match_flag` stored as an all-null
  Null column in some months), which the strict cast absorbs,
- the *_flag columns are 'Y'/'N', except that Uber (HV0003) left `access_a_ride_flag` as ' ' in some
  years (seen in 2020 and 2022; back to Y/N by 2025); blanks, and anything else, become null,
- `wav_match_flag` is entirely null before 2019-04 and `airport_fee` before 2019-07 (not yet reported),
- `base_passenger_fare <= 0` on ~0.1-0.5% of trips; out-of-period pickups are rare (0 in the sampled months).
"""

from datetime import UTC, datetime

import polars as pl

from .schemas import SILVER_FHVHV, month_start

RENAME = {
    "request_datetime": "request_ts",
    "on_scene_datetime": "on_scene_ts",
    "pickup_datetime": "pickup_ts",
    "dropoff_datetime": "dropoff_ts",
    "PULocationID": "pickup_location_id",
    "DOLocationID": "dropoff_location_id",
    "trip_time": "trip_time_sec",
    "shared_request_flag": "shared_request",
    "shared_match_flag": "shared_match",
    "access_a_ride_flag": "access_a_ride",
    "wav_request_flag": "wav_request",
    "wav_match_flag": "wav_match",
}

FLAG_COLUMNS = ("shared_request", "shared_match", "access_a_ride", "wav_request", "wav_match")
YES_NO = {"Y": True, "N": False}


def standardize(raw: pl.DataFrame, source_month: str, source_file: str) -> pl.DataFrame:
    version = SILVER_FHVHV.schema_version_for(source_month)

    df = raw.rename(RENAME, strict=False)
    # cast first: a month where TLC left a flag empty stores it as a Null column, not String.
    df = df.with_columns(
        pl.col(FLAG_COLUMNS).cast(pl.String).replace_strict(YES_NO, default=None, return_dtype=pl.Boolean)
    )

    schema = SILVER_FHVHV.schema
    df = df.with_columns(pl.lit(None, dtype=schema[c]).alias(c) for c in schema if c not in df.columns)

    df = df.cast({c: t for c, t in schema.items() if c in df.columns}, strict=True)

    df = (
        df.with_columns(
            ((pl.col("dropoff_ts") - pl.col("pickup_ts")).dt.total_microseconds() / 60e6).alias("trip_duration_min"),
        )
        .with_columns(
            (pl.col("trip_duration_min") <= 0).fill_null(True).alias("is_non_positive_duration"),
            (pl.col("pickup_ts").dt.strftime("%Y-%m") != source_month).fill_null(True).alias("is_out_of_period"),
            (pl.col("base_passenger_fare") <= 0).fill_null(True).alias("is_non_positive_fare"),
        )
        .with_columns(
            (~pl.any_horizontal(SILVER_FHVHV.flags)).alias("is_valid_trip"),
        )
    )

    df = df.with_columns(
        pl.lit(month_start(source_month)).alias("source_month"),
        pl.lit(source_file).alias("source_file"),
        pl.lit(version).alias("schema_version"),
        pl.lit(datetime.now(UTC), dtype=pl.Datetime("us", "UTC")).alias("ingested_at"),
    )

    return df.select(schema.names())
