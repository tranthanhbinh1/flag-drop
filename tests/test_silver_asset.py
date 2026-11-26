"""Run the Silver assets through Dagster: Bronze from a file-backed fake bucket, Silver to a local Delta table."""

from datetime import date
from pathlib import Path

import dagster as dg
import polars as pl
import pytest
import test_standardize_fhvhv as fhvhv_rows
from dagster_deltalake import LocalConfig
from dagster_deltalake_polars import DeltaLakePolarsIOManager
from fakes import FakeObjectStore
from test_standardize_yellow import Y2009, Y2024, Layout

from flag_drop.assets.silver import IO_MANAGER_KEY, build_silver_asset
from flag_drop.resources import SilverTables
from flag_drop.transformation.silver import fhvhv, yellow
from flag_drop.transformation.silver.schemas import SILVER_FHVHV, SILVER_YELLOW
from flag_drop.utils.tlc import raw_relpath


@pytest.fixture
def lake(tmp_path: Path) -> Path:
    """Stands in for MinIO: `raw/` is the Bronze bucket, `silver/` the Delta tables."""
    return tmp_path / "lake"


def raw_store(lake: Path) -> FakeObjectStore:
    return FakeObjectStore(endpoint_url=str(lake), access_key="k", secret_key="s", bucket="raw")


def upload_raw(lake: Path, layout: Layout | fhvhv_rows.Layout, *changes: dict, dataset: str = "yellow") -> None:
    store = raw_store(lake)
    path = store.path(store.key(raw_relpath(dataset, layout.month)))
    path.parent.mkdir(parents=True, exist_ok=True)
    layout.raw(*changes).write_parquet(path)


ASSETS = {
    "yellow": build_silver_asset("yellow", SILVER_YELLOW, yellow.standardize),
    "fhvhv": build_silver_asset("fhvhv", SILVER_FHVHV, fhvhv.standardize),
}


def materialize(
    lake: Path, month: str, raise_on_error: bool = True, dataset: str = "yellow"
) -> dg.ExecuteInProcessResult:
    return dg.materialize(
        [ASSETS[dataset]],
        partition_key=f"{month}-01",
        resources={
            "raw_store": raw_store(lake),
            "silver_tables": SilverTables(root_uri=str(lake)),
            IO_MANAGER_KEY: DeltaLakePolarsIOManager(root_uri=str(lake), storage_options=LocalConfig()),
        },
        raise_on_error=raise_on_error,
    )


def silver(lake: Path, dataset: str = "yellow") -> pl.DataFrame:
    return pl.read_delta(str(lake / "silver" / dataset))


def test_month_is_written_with_metadata(lake: Path):
    upload_raw(lake, Y2024, {}, {"fare_amount": -3.0}, {}, {})

    [mat] = materialize(lake, "2024-01").asset_materializations_for_node("silver__yellow")

    df = silver(lake)
    assert df.schema == SILVER_YELLOW.schema  # Delta round-trip keeps the contract's dtypes
    assert df.height == 4
    meta = mat.metadata
    assert meta["raw_uri"].value == "s3://raw/data/2024/yellow_tripdata_2024-01.parquet"
    assert meta["dagster/row_count"].value == 4
    assert meta["schema_version"].value == "2023-02"
    assert meta["rate_is_non_positive_fare"].value == 0.25
    assert meta["rate_is_valid_trip"].value == 0.75
    # Added by the IO manager. (Its num_rows/size_bytes stats fail on deltalake 1.x in dagster-deltalake
    # 0.29.31 -- "Cannot convert arro3...Table to pyarrow RecordBatch" -- and are skipped with a warning.)
    assert meta["table_uri"].value == str(lake / "silver" / "yellow")
    assert "table_columns" in meta


def test_rerun_replaces_the_month_and_keeps_others(lake: Path):
    upload_raw(lake, Y2009, {}, {})
    upload_raw(lake, Y2024, {}, {}, {})
    materialize(lake, "2009-01")
    materialize(lake, "2024-01")

    upload_raw(lake, Y2024, {})  # the January file changed upstream
    materialize(lake, "2024-01")

    counts = dict(silver(lake).group_by("source_month").len().iter_rows())
    assert counts == {date(2009, 1, 1): 2, date(2024, 1, 1): 1}


def test_missing_bronze_file_fails_without_writing(lake: Path):
    result = materialize(lake, "2024-01", raise_on_error=False)

    assert not result.success
    assert not (lake / "silver").exists()


def test_fhvhv_month_is_written_next_to_yellow(lake: Path):
    upload_raw(lake, Y2024, {})
    upload_raw(lake, fhvhv_rows.F2025, {}, {"base_passenger_fare": 0.0}, dataset="fhvhv")
    materialize(lake, "2024-01")

    [mat] = materialize(lake, "2025-03", dataset="fhvhv").asset_materializations_for_node("silver__fhvhv")

    df = silver(lake, "fhvhv")
    assert df.schema == SILVER_FHVHV.schema
    assert df["is_valid_trip"].to_list() == [True, False]
    assert mat.metadata["schema_version"].value == "2025-01"
    assert mat.metadata["rate_is_non_positive_fare"].value == 0.5
    assert silver(lake).height == 1  # yellow untouched
