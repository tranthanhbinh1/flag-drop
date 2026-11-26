"""File-backed stand-ins for MinIO and TLC, shared by the asset tests."""

import shutil
from pathlib import Path

import polars as pl

from flag_drop.resources import RawObjectStore, TlcSource


class FakeObjectStore(RawObjectStore):
    """Stores objects as files under endpoint_url (a local directory): `{endpoint_url}/{bucket}/{key}`."""

    def path(self, key: str) -> Path:
        return Path(self.endpoint_url) / self.bucket / key

    def object_size(self, key: str) -> int | None:
        path = self.path(key)
        return path.stat().st_size if path.is_file() else None

    def upload(self, path: Path, key: str) -> None:
        target = self.path(key)
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(path, target)

    def read_parquet(self, key: str) -> pl.DataFrame:
        if not self.path(key).is_file():
            raise FileNotFoundError(self.uri(key))
        return pl.read_parquet(self.path(key))


class FakeTlc(TlcSource):
    """Serves files from base_url (a local directory); anything missing is 'not published'."""

    def fetch(self, filename: str, dest: Path) -> bool:
        src = Path(self.base_url) / filename
        if not src.is_file():
            return False
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(src, dest)
        return True
