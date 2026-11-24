"""Bronze: one raw TLC Parquet file per dataset x month, on local disk and in MinIO."""

from collections.abc import Iterator

import dagster as dg

from flag_drop.partitions import DATASETS, monthly_partitions, partition_month
from flag_drop.resources import RawFiles, RawObjectStore, TlcSource
from flag_drop.utils.tlc import (
    RawFileInvalid,
    inspect_parquet,
    raw_relpath,
    tlc_filename,
)


def build_raw_asset(dataset: str) -> dg.AssetsDefinition:
    @dg.asset(
        name=f"raw_{dataset}",
        group_name="bronze",
        partitions_def=monthly_partitions(dataset),
        pool="minio_upload",
        # Downloads from CloudFront occasionally drop mid-stream (connection reset).
        retry_policy=dg.RetryPolicy(max_retries=3, delay=20, backoff=dg.Backoff.EXPONENTIAL),
        kinds={"parquet", "s3"},
        # A month TLC has not published yet produces no materialization instead of a failure.
        output_required=False,
        description=(
            f"Raw TLC {dataset} trip file for the month. Uses the local copy if present, otherwise "
            "downloads it; validates it and makes sure MinIO holds the same file."
        ),
    )
    def _raw(
        context: dg.AssetExecutionContext,
        raw_files: RawFiles,
        raw_store: RawObjectStore,
        tlc_source: TlcSource,
    ) -> Iterator[dg.MaterializeResult]:
        month = partition_month(context.partition_key)
        relpath = raw_relpath(dataset, month)

        path, info, source = None, None, "local"
        broken: list[str] = []
        for candidate in raw_files.candidates(relpath):
            try:
                info, path = inspect_parquet(candidate), candidate
                break
            except RawFileInvalid as exc:
                # e.g. an HTTP error page saved as .parquet by an older downloader
                context.log.warning(f"Ignoring invalid local copy: {exc}")
                broken.append(str(candidate))

        if path is None or info is None:
            path = raw_files.landing_path(relpath)
            if not tlc_source.fetch(tlc_filename(dataset, month), path):
                if broken:
                    # Retrying cannot fix this; fail now instead of after the retry backoff.
                    raise dg.Failure(
                        description=f"{relpath}: local copy broken and TLC has no file",
                        metadata={"ignored_broken_copies": dg.MetadataValue.json(broken)},
                        allow_retries=False,
                    )
                context.log.info(f"{dataset} {month} is not published by TLC yet; skipping")
                return
            info, source = inspect_parquet(path), "downloaded"

        size = path.stat().st_size

        key = raw_store.key(relpath)
        uploaded = raw_store.object_size(key) != size
        if uploaded:
            context.log.info(f"Uploading {relpath} ({size / 1e6:.0f} MB) to {raw_store.bucket}")
            raw_store.upload(path, key)

        yield dg.MaterializeResult(
            metadata={
                "path": dg.MetadataValue.path(str(path)),
                "s3_uri": dg.MetadataValue.url(f"s3://{raw_store.bucket}/{key}"),
                "source": source,
                "ignored_broken_copies": dg.MetadataValue.json(broken),
                "uploaded": uploaded,
                "size_mb": round(size / 1e6, 1),
                "dagster/row_count": info.rows,
                "schema_fingerprint": info.schema_fingerprint,
                "dagster/column_schema": dg.TableSchema(
                    columns=[dg.TableColumn(name=name, type=dtype) for name, dtype in info.columns.items()]
                ),
            }
        )

    return _raw


raw_assets: list[dg.AssetsDefinition] = [build_raw_asset(d) for d in DATASETS]
