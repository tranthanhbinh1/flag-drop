"""Dagster resources: local raw files, the MinIO raw bucket, and the TLC download source."""

from pathlib import Path

import boto3
import dagster as dg
import httpx
from boto3.s3.transfer import TransferConfig
from botocore.config import Config
from botocore.exceptions import ClientError

from flag_drop.utils import tlc


class RawFiles(dg.ConfigurableResource):
    """Bronze files on local disk.

    `data/raw` is the existing archive (symlink into the lakehouse project, mounted read-only in
    Compose). Months missing or broken there are downloaded into `data/landing`, which Flag Drop
    owns. Both use the same `{year}/{filename}` layout; `landing` wins when both exist.
    """

    data_dir: str

    @property
    def landing_dir(self) -> Path:
        return Path(self.data_dir) / "landing"

    @property
    def archive_dir(self) -> Path:
        return Path(self.data_dir) / "raw"

    def candidates(self, relpath: str) -> list[Path]:
        """Existing local copies, in order of preference."""
        paths = (self.landing_dir / relpath, self.archive_dir / relpath)
        return [p for p in paths if p.is_file()]

    def landing_path(self, relpath: str) -> Path:
        return self.landing_dir / relpath


class RawObjectStore(dg.ConfigurableResource):
    """The MinIO bucket holding the full raw archive, keyed `{prefix}/{year}/{filename}`."""

    endpoint_url: str
    access_key: str
    secret_key: str
    bucket: str
    prefix: str = "data"

    def _client(self):
        return boto3.client(
            "s3",
            endpoint_url=self.endpoint_url,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            region_name="us-east-1",
            config=Config(s3={"addressing_style": "path"}, retries={"max_attempts": 5}),
        )

    def key(self, relpath: str) -> str:
        return f"{self.prefix}/{relpath}"

    def object_size(self, key: str) -> int | None:
        try:
            return self._client().head_object(Bucket=self.bucket, Key=key)["ContentLength"]
        except ClientError as exc:
            if exc.response.get("Error", {}).get("Code") in {"404", "NoSuchKey", "NotFound"}:
                return None
            raise

    def upload(self, path: Path, key: str) -> None:
        transfer = TransferConfig(multipart_chunksize=64 * 1024 * 1024, max_concurrency=4)
        self._client().upload_file(str(path), self.bucket, key, Config=transfer)


class TlcSource(dg.ConfigurableResource):
    """TLC's public CloudFront bucket."""

    base_url: str = tlc.TLC_BASE_URL
    timeout_seconds: float = 300.0

    def fetch(self, filename: str, dest: Path) -> bool:
        timeout = httpx.Timeout(self.timeout_seconds, connect=30.0)
        with httpx.Client(timeout=timeout, follow_redirects=True) as client:
            return tlc.fetch(client, f"{self.base_url}/{filename}", dest)
