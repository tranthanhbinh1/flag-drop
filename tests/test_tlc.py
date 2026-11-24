import io
from pathlib import Path

import httpx
import polars as pl
import pytest

from flag_drop.partitions import monthly_partitions, partition_month
from orchestration.flag_drop.utils.tlc import (
    RawFileInvalid,
    fetch,
    inspect_parquet,
    raw_relpath,
    tlc_filename,
)


def parquet_bytes(rows: int = 3) -> bytes:
    buf = io.BytesIO()
    pl.DataFrame({"VendorID": list(range(rows)), "fare_amount": [1.5] * rows}).write_parquet(buf)
    return buf.getvalue()


def client_returning(status: int, body: bytes = b"") -> httpx.Client:
    return httpx.Client(
        transport=httpx.MockTransport(lambda _req: httpx.Response(status, content=body))
    )


def test_naming_matches_tlc_and_local_layout():
    assert tlc_filename("fhvhv", "2024-01") == "fhvhv_tripdata_2024-01.parquet"
    assert raw_relpath("yellow", "2009-04") == "2009/yellow_tripdata_2009-04.parquet"


def test_partitions_start_at_first_published_month():
    keys = monthly_partitions("fhvhv").get_partition_keys()
    assert keys[0] == "2019-02-01"
    assert partition_month(keys[0]) == "2019-02"


def test_fetch_writes_valid_file_atomically(tmp_path: Path):
    dest = tmp_path / "2024" / "yellow_tripdata_2024-01.parquet"
    assert fetch(client_returning(200, parquet_bytes()), "https://x/f.parquet", dest) is True
    assert inspect_parquet(dest).rows == 3
    assert list(dest.parent.iterdir()) == [dest]  # no .part left over


@pytest.mark.parametrize("status", [403, 404])
def test_fetch_unpublished_month_returns_false(tmp_path: Path, status: int):
    dest = tmp_path / "f.parquet"
    assert (
        fetch(client_returning(status, b"<Error>AccessDenied</Error>"), "https://x", dest) is False
    )
    assert list(tmp_path.iterdir()) == []


def test_fetch_rejects_non_parquet_body(tmp_path: Path):
    dest = tmp_path / "f.parquet"
    with pytest.raises(RawFileInvalid):
        fetch(client_returning(200, b"<html>oops</html>"), "https://x", dest)
    assert list(tmp_path.iterdir()) == []


def test_fetch_raises_on_server_error(tmp_path: Path):
    with pytest.raises(httpx.HTTPStatusError):
        fetch(client_returning(500), "https://x", tmp_path / "f.parquet")
    assert list(tmp_path.iterdir()) == []


def test_inspect_rejects_empty_file(tmp_path: Path):
    path = tmp_path / "empty.parquet"
    pl.DataFrame({"a": []}, schema={"a": pl.Int64}).write_parquet(path)
    with pytest.raises(RawFileInvalid, match="0 rows"):
        inspect_parquet(path)


def test_schema_fingerprint_is_stable_and_schema_sensitive(tmp_path: Path):
    a, b = tmp_path / "a.parquet", tmp_path / "b.parquet"
    a.write_bytes(parquet_bytes(3))
    pl.DataFrame({"VendorID": [1], "fare_amount": [1]}).write_parquet(b)  # fare as int
    assert inspect_parquet(a).schema_fingerprint == inspect_parquet(a).schema_fingerprint
    assert inspect_parquet(a).schema_fingerprint != inspect_parquet(b).schema_fingerprint
