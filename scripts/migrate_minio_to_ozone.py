"""Copy one S3-compatible bucket from MinIO to Apache Ozone with MD5 verification."""

from __future__ import annotations

import hashlib
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3
from botocore.config import Config


BUCKET = os.environ["S3_BUCKET_NAME"]
SOURCE_ENDPOINT = os.environ["SOURCE_S3_ENDPOINT"]
SOURCE_ACCESS_KEY = os.environ["SOURCE_S3_ACCESS_KEY"]
SOURCE_SECRET_KEY = os.environ["SOURCE_S3_SECRET_KEY"]
TARGET_ENDPOINT = os.environ["TARGET_S3_ENDPOINT"]
TARGET_ACCESS_KEY = os.environ["TARGET_S3_ACCESS_KEY"]
TARGET_SECRET_KEY = os.environ["TARGET_S3_SECRET_KEY"]
WORKERS = int(os.environ.get("MIGRATION_WORKERS", "8"))


def client(endpoint: str, access_key: str, secret_key: str):
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="us-east-1",
        config=Config(
            signature_version="s3v4",
            s3={"addressing_style": "path"},
            max_pool_connections=max(16, WORKERS * 2),
        ),
    )


source = client(SOURCE_ENDPOINT, SOURCE_ACCESS_KEY, SOURCE_SECRET_KEY)
target = client(TARGET_ENDPOINT, TARGET_ACCESS_KEY, TARGET_SECRET_KEY)


def list_objects(s3):
    rows = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET):
        rows.extend(page.get("Contents", []))
    return rows


def copy_and_verify(item):
    key = item["Key"]
    if key.endswith("/") and item["Size"] == 0:
        return "marker", 0

    response = source.get_object(Bucket=BUCKET, Key=key)
    payload = response["Body"].read()
    digest = hashlib.md5(payload).hexdigest()  # nosec B324 - transfer integrity only

    put_args = {"Bucket": BUCKET, "Key": key, "Body": payload}
    for source_name, target_name in (
        ("ContentType", "ContentType"),
        ("ContentEncoding", "ContentEncoding"),
        ("ContentLanguage", "ContentLanguage"),
        ("CacheControl", "CacheControl"),
        ("ContentDisposition", "ContentDisposition"),
    ):
        if response.get(source_name):
            put_args[target_name] = response[source_name]
    if response.get("Metadata"):
        put_args["Metadata"] = response["Metadata"]

    target.put_object(**put_args)
    copied = target.get_object(Bucket=BUCKET, Key=key)["Body"].read()
    if len(copied) != len(payload) or hashlib.md5(copied).hexdigest() != digest:  # nosec B324
        raise RuntimeError(f"Checksum mismatch for {key}")
    return "object", len(payload)


def main() -> None:
    source_items = list_objects(source)
    source_objects = [item for item in source_items if not (item["Key"].endswith("/") and item["Size"] == 0)]
    source_bytes = sum(item["Size"] for item in source_objects)
    marker_count = len(source_items) - len(source_objects)

    copied_objects = 0
    copied_bytes = 0
    with ThreadPoolExecutor(max_workers=WORKERS) as executor:
        futures = [executor.submit(copy_and_verify, item) for item in source_items]
        for index, future in enumerate(as_completed(futures), 1):
            kind, size = future.result()
            if kind == "object":
                copied_objects += 1
                copied_bytes += size
            if index % 250 == 0 or index == len(futures):
                print(f"MIGRATION_PROGRESS={index}/{len(futures)}", flush=True)

    target_items = list_objects(target)
    target_objects = [item for item in target_items if not (item["Key"].endswith("/") and item["Size"] == 0)]
    target_bytes = sum(item["Size"] for item in target_objects)

    print(f"SOURCE_OBJECTS={len(source_objects)}")
    print(f"SOURCE_BYTES={source_bytes}")
    print(f"SOURCE_DIRECTORY_MARKERS={marker_count}")
    print(f"COPIED_OBJECTS={copied_objects}")
    print(f"COPIED_BYTES={copied_bytes}")
    print(f"TARGET_OBJECTS={len(target_objects)}")
    print(f"TARGET_BYTES={target_bytes}")

    expected_keys = {item["Key"] for item in source_objects}
    actual_keys = {item["Key"] for item in target_objects}
    if (
        copied_objects != len(source_objects)
        or copied_bytes != source_bytes
        or actual_keys != expected_keys
        or target_bytes != source_bytes
    ):
        raise RuntimeError("Post-migration inventory mismatch")

    print("MIGRATION_VERIFY=PASS")


if __name__ == "__main__":
    main()
