from collections.abc import Callable

import dagster as dg
import polars as pl

from flag_drop.partitions import monthly_partitions, partition_month
from flag_drop.resources import RawObjectStore, SilverTables
from flag_drop.transformation.silver import fhvhv, yellow
from flag_drop.transformation.silver.schemas import (
    PARTITION_COLUMN,
    SILVER_FHVHV,
    SILVER_YELLOW,
    SilverTable,
    flag_rates,
    validate_silver,
)
from flag_drop.utils.delta import ensure_table
from flag_drop.utils.tlc import raw_relpath, tlc_filename

Standardize = Callable[[pl.DataFrame, str, str], pl.DataFrame]  # (raw, 'YYYY-MM', source_file) -> Silver
IO_MANAGER_KEY = "silver_io_manager"


def build_silver_asset(
    dataset: str, table: SilverTable, standardize: Standardize, pool: str = "silver"
) -> dg.AssetsDefinition:
    @dg.asset(
        key=["silver", dataset],
        group_name="silver",
        partitions_def=monthly_partitions(dataset),
        deps=[dg.AssetKey(f"raw_{dataset}")],
        io_manager_key=IO_MANAGER_KEY,
        metadata={"partition_expr": PARTITION_COLUMN},
        pool=pool,
        kinds={"polars", "delta"},
        description=(
            f"Standardized {dataset} trips: one Delta partition per raw file (`{PARTITION_COLUMN}`), "
            "every raw row kept and flagged. Rerunning a month replaces it atomically."
        ),
    )
    def _silver(
        context: dg.AssetExecutionContext, raw_store: RawObjectStore, silver_tables: SilverTables
    ) -> dg.Output[pl.DataFrame]:
        month = partition_month(context.partition_key)
        raw_key = raw_store.key(raw_relpath(dataset, month))
        try:
            # raw_<dataset> validated the file before uploading it, so the bucket copy is the source of truth.
            raw = raw_store.read_parquet(raw_key)
        except FileNotFoundError:
            raise dg.Failure(
                description=f"{raw_store.uri(raw_key)} not found; materialize raw_{dataset} for {month} first.",
                allow_retries=False,
            ) from None

        df = standardize(raw, month, tlc_filename(dataset, month))
        validate_silver(df, table, month)
        if df.height != raw.height:  # flag, don't filter
            raise dg.Failure(f"standardize changed the row count: {raw.height} raw -> {df.height} Silver")

        # Guard for the IO manager's first write; see utils/delta.py.
        uri = silver_tables.uri(dataset)
        if ensure_table(uri, table.schema, [PARTITION_COLUMN], silver_tables.storage_options()):
            context.log.info(f"Created Delta table {uri}")

        # The IO manager adds table_uri and table_columns (its num_rows/size_bytes stats fail on deltalake 1.x).
        return dg.Output(
            df,
            metadata={
                "raw_uri": dg.MetadataValue.url(raw_store.uri(raw_key)),
                "schema_version": table.schema_version_for(month),
                "dagster/row_count": df.height,
                # One numeric entry per flag so Dagster can plot each rate across partitions.
                **{f"rate_{name}": rate for name, rate in flag_rates(df, table).items()},
            },
        )

    return _silver


silver_assets: list[dg.AssetsDefinition] = [
    build_silver_asset("yellow", SILVER_YELLOW, yellow.standardize),
    # One fhvhv month peaks at ~15 GB RSS (yellow ~4 GB), so it gets its own, smaller pool; limits in `make pools`.
    build_silver_asset("fhvhv", SILVER_FHVHV, fhvhv.standardize, pool="silver_fhvhv"),
]
