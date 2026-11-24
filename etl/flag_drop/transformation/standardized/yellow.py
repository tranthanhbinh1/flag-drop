from datetime import UTC, datetime

import polars as pl

from .schemas import STANDARDIZED_YELLOW

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


def standardize(raw: pl.DataFrame, source_month: str, source_file: str) -> pl.DataFrame:

    df = raw.rename(RENAME_2011, strict=False).with_columns(
        # default=None: anything other than Y/N (2010 files use "0"/"1") becomes null instead of raising
        pl.col("store_and_fwd").replace_strict({"Y": True, "N": False}, default=None, return_dtype=pl.Boolean)
    )

    schema = STANDARDIZED_YELLOW.schema
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
            (~pl.any_horizontal(STANDARDIZED_YELLOW.flags)).alias("is_valid_trip"),
        )
    )

    df = df.with_columns(
        pl.lit(source_month).alias("source_month"),
        pl.lit(source_file).alias("source_file"),
        pl.lit(STANDARDIZED_YELLOW.schema_version_for(source_month)).alias("schema_version"),
        pl.lit(datetime.now(UTC), dtype=pl.Datetime("us", "UTC")).alias("ingested_at"),
    )

    return df.select(schema.names())
