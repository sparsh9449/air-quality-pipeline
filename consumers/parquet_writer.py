"""Consume measurements from Kafka and write them to partitioned Parquet. (stub)"""

from __future__ import annotations


def write_batch(records: list[dict], parquet_dir: str) -> str:
    """Write a batch of measurement records to Parquet, returning the output path.

    Stub: implement Kafka consumption + pyarrow write in a later slice.
    """
    raise NotImplementedError
