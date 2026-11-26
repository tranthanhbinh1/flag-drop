"""Run the raw assets through Dagster with file-backed fakes for MinIO and TLC."""

from pathlib import Path

import dagster as dg
import polars as pl
import pytest
from fakes import FakeObjectStore, FakeTlc

from flag_drop.assets.ingest import build_raw_asset
from flag_drop.resources import RawFiles


@pytest.fixture
def env(tmp_path: Path):
    data, s3, upstream = tmp_path / "data", tmp_path / "s3", tmp_path / "tlc"
    for d in (data / "raw", s3, upstream):
        d.mkdir(parents=True)
    resources = {
        "raw_files": RawFiles(data_dir=str(data)),
        "raw_store": FakeObjectStore(endpoint_url=str(s3), access_key="k", secret_key="s", bucket="raw"),
        "tlc_source": FakeTlc(base_url=str(upstream)),
    }
    return data, s3, upstream, resources


def write_parquet(path: Path, rows: int = 2) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({"x": list(range(rows))}).write_parquet(path)


def materialize(resources, partition: str) -> dg.ExecuteInProcessResult:
    return dg.materialize([build_raw_asset("yellow")], partition_key=partition, resources=resources)


def test_local_file_is_validated_and_uploaded(env):
    data, s3, _, resources = env
    write_parquet(data / "raw/2024/yellow_tripdata_2024-01.parquet", rows=5)

    result = materialize(resources, "2024-01-01")

    [mat] = result.asset_materializations_for_node("raw_yellow")
    assert mat.metadata["source"].value == "local"
    assert mat.metadata["uploaded"].value is True
    assert mat.metadata["dagster/row_count"].value == 5
    assert (s3 / "raw/data/2024/yellow_tripdata_2024-01.parquet").is_file()

    # Second run: same size already in the bucket, so no re-upload.
    [again] = materialize(resources, "2024-01-01").asset_materializations_for_node("raw_yellow")
    assert again.metadata["uploaded"].value is False


def test_missing_month_is_downloaded_into_landing(env):
    data, s3, upstream, resources = env
    write_parquet(upstream / "yellow_tripdata_2026-09.parquet")

    [mat] = materialize(resources, "2026-09-01").asset_materializations_for_node("raw_yellow")

    assert mat.metadata["source"].value == "downloaded"
    assert (data / "landing/2026/yellow_tripdata_2026-09.parquet").is_file()
    assert (s3 / "raw/data/2026/yellow_tripdata_2026-09.parquet").is_file()


def test_unpublished_month_succeeds_without_materializing(env):
    _, s3, _, resources = env

    result = materialize(resources, "2026-10-01")

    assert result.success
    assert result.asset_materializations_for_node("raw_yellow") == []
    assert not any(s3.iterdir())


def test_broken_archive_copy_is_replaced_from_tlc(env):
    data, s3, upstream, resources = env
    broken = data / "raw/2015/yellow_tripdata_2015-07.parquet"
    broken.parent.mkdir(parents=True)
    broken.write_text("<HTML><H1>403 ERROR</H1></HTML>")
    stale = s3 / "raw/data/2015/yellow_tripdata_2015-07.parquet"
    stale.parent.mkdir(parents=True)
    stale.write_text("<HTML><H1>403 ERROR</H1></HTML>")
    write_parquet(upstream / "yellow_tripdata_2015-07.parquet", rows=4)

    [mat] = materialize(resources, "2015-07-01").asset_materializations_for_node("raw_yellow")

    assert mat.metadata["source"].value == "downloaded"
    assert mat.metadata["ignored_broken_copies"].value == [str(broken)]
    assert mat.metadata["uploaded"].value is True
    assert stale.stat().st_size == (data / "landing/2015/yellow_tripdata_2015-07.parquet").stat().st_size


def test_broken_copy_with_nothing_upstream_fails(env):
    data, _, _, resources = env
    broken = data / "raw/2015/yellow_tripdata_2015-08.parquet"
    broken.parent.mkdir(parents=True)
    broken.write_text("not parquet")

    result = dg.materialize(
        [build_raw_asset("yellow")],
        partition_key="2015-08-01",
        resources=resources,
        raise_on_error=False,
    )

    assert not result.success
