"""Dagster resources: local raw files, Silver Delta tables, the MinIO raw bucket, and the TLC download source."""

from pathlib import Path

import boto3
import dagster as dg
import httpx
import polars as pl
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


class SilverTables(dg.ConfigurableResource):
    """Where the Silver Delta tables live: `{root_uri}/silver/{dataset}`, e.g. `s3://flag-drop/silver/yellow`.

    Must mirror `silver_io_manager` (same root and MinIO credentials; it stores asset `silver/<dataset>`
    at `{root_uri}/silver/<dataset>`). Assets use this to create a table before the IO manager writes.
    Without `endpoint_url` the root is a local directory (tests).
    """

    root_uri: str
    endpoint_url: str | None = None
    access_key: str | None = None
    secret_key: str | None = None

    def uri(self, dataset: str) -> str:
        return f"{self.root_uri.rstrip('/')}/silver/{dataset}"

    def storage_options(self) -> dict[str, str]:
        """delta-rs storage options (same keys as dagster-deltalake's S3Config + ClientConfig)."""
        if self.endpoint_url is None:
            return {}
        options = {"endpoint": self.endpoint_url, "region": "us-east-1", "allow_http": "true"}
        if self.access_key and self.secret_key:
            options |= {"access_key_id": self.access_key, "secret_access_key": self.secret_key}
        return options


class RawObjectStore(dg.ConfigurableResource):
    """The MinIO bucket holding the full raw archive (Bronze), keyed `{prefix}/{year}/{filename}`.

    `raw_<dataset>` puts files here; Silver reads them from here, never from local disk.
    """

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

    def uri(self, key: str) -> str:
        return f"s3://{self.bucket}/{key}"

    def read_parquet(self, key: str) -> pl.DataFrame:
        """Read one Bronze file straight from the bucket. Raises FileNotFoundError if the key is missing."""
        options = {
            "aws_endpoint_url": self.endpoint_url,
            "aws_access_key_id": self.access_key,
            "aws_secret_access_key": self.secret_key,
            "aws_region": "us-east-1",
            "aws_allow_http": str(self.endpoint_url.startswith("http://")).lower(),
        }
        return pl.read_parquet(self.uri(key), storage_options=options)

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
