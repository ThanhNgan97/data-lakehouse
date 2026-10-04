# -*- coding: utf-8 -*-
"""Shared Bronze Parquet writer for small/medium ingestion batches.

Keeps the current project convention of writing one Parquet object to MinIO.
The writer is source-agnostic: document/KPI and API adapters can share it.
"""

from __future__ import annotations

import io
from typing import Any

import boto3
import pandas as pd

from env_config import (
    MINIO_ACCESS_KEY,
    MINIO_BUCKET_NAME,
    MINIO_ENDPOINT,
    MINIO_SECRET_KEY,
)


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )


def _to_pandas(df: Any) -> pd.DataFrame:
    """Accept a pandas or Spark DataFrame without making the writer source-specific."""
    if isinstance(df, pd.DataFrame):
        return df
    if hasattr(df, "toPandas"):
        return df.toPandas()
    raise TypeError("Bronze writer expects a pandas or Spark DataFrame")


def write_parquet_object(df: Any, object_key: str, *, s3_client=None) -> tuple[str, int]:
    """Write one Parquet object to MinIO and return (object_key, row_count)."""
    pandas_df = _to_pandas(df)
    row_count = len(pandas_df.index)
    if row_count == 0:
        raise ValueError("Refusing to write an empty Bronze batch")

    parquet_buffer = io.BytesIO()
    pandas_df.to_parquet(
        parquet_buffer,
        index=False,
        engine="pyarrow",
        coerce_timestamps="us",
        allow_truncated_timestamps=True,
    )

    client = s3_client or get_s3_client()
    client.put_object(
        Bucket=MINIO_BUCKET_NAME,
        Key=object_key,
        Body=parquet_buffer.getvalue(),
        ContentType="application/octet-stream",
    )
    return object_key, row_count


def read_parquet_object(object_key: str, *, s3_client=None) -> pd.DataFrame:
    """Read one Parquet object back from MinIO for Bronze verification."""
    client = s3_client or get_s3_client()
    body = client.get_object(Bucket=MINIO_BUCKET_NAME, Key=object_key)["Body"].read()
    return pd.read_parquet(io.BytesIO(body), engine="pyarrow")
