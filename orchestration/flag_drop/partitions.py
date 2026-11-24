"""Partition definitions: one monthly partition set per TLC dataset."""

import dagster as dg

# First month TLC publishes for each dataset (verified against the CloudFront bucket 2026-11-24:
# the month before each of these returns 403).
DATASET_START: dict[str, str] = {
    "yellow": "2009-01-01",
    "green": "2014-01-01",
    "fhv": "2015-01-01",
    "fhvhv": "2019-02-01",
}

DATASETS: tuple[str, ...] = tuple(DATASET_START)


def monthly_partitions(dataset: str) -> dg.MonthlyPartitionsDefinition:
    """Months from the dataset's first file up to the last complete month.

    TLC publishes with a ~2-month lag, so the newest partitions may not exist upstream yet;
    the raw asset skips those without failing.
    """
    return dg.MonthlyPartitionsDefinition(start_date=DATASET_START[dataset])


def partition_month(partition_key: str) -> str:
    """'2024-01-01' -> '2024-01' (the month format TLC uses in filenames)."""
    return partition_key[:7]
