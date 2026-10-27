"""Read and write run-scoped control artifacts through Ozone's S3 API."""

from __future__ import annotations

import json
import tempfile
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import boto3

from env_config import S3_ACCESS_KEY, S3_BUCKET_NAME, S3_ENDPOINT, S3_SECRET_KEY


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
        verify=False,
    )


def object_uri(key: str, *, scheme: str = "s3") -> str:
    return f"{scheme}://{S3_BUCKET_NAME}/{key.lstrip('/')}"


def parse_object_uri(uri: str) -> tuple[str, str]:
    parsed = urlparse(uri)
    if parsed.scheme not in {"s3", "s3a"} or not parsed.netloc or not parsed.path:
        raise ValueError(f"Expected an s3:// or s3a:// object URI, got: {uri}")
    return parsed.netloc, parsed.path.lstrip("/")


def read_json_artifact(location: str) -> dict[str, Any]:
    if location.startswith(("s3://", "s3a://")):
        bucket, key = parse_object_uri(location)
        body = get_s3_client().get_object(Bucket=bucket, Key=key)["Body"].read()
        return json.loads(body.decode("utf-8"))
    with open(location, "r", encoding="utf-8") as handle:
        return json.load(handle)


def write_json_artifact(location: str, payload: Any) -> str:
    if location.startswith(("s3://", "s3a://")):
        bucket, key = parse_object_uri(location)
        get_s3_client().put_object(
            Bucket=bucket,
            Key=key,
            Body=json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8"),
            ContentType="application/json",
        )
        return object_uri(key)
    with open(location, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)
    return location


def upload_file(source: str, key: str) -> str:
    get_s3_client().upload_file(source, S3_BUCKET_NAME, key)
    return object_uri(key, scheme="s3a")


def materialize_file(location: str) -> str:
    if not location.startswith(("s3://", "s3a://")):
        return location
    bucket, key = parse_object_uri(location)
    suffix = Path(key).suffix
    target = Path(tempfile.mkdtemp(prefix="lakehouse-input-")) / f"input{suffix}"
    get_s3_client().download_file(bucket, key, str(target))
    return str(target)
