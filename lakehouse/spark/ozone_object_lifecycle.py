"""Safe lifecycle operations for source objects stored in Apache Ozone."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable


@dataclass(frozen=True)
class ArchiveResult:
    source_key: str
    archive_key: str
    content_length: int
    deleted_source: bool


def archive_key_for(source_key: str, archive_prefix: str = "archive/processed") -> str:
    source_key = source_key.lstrip("/")
    relative = source_key.replace("staging/", "", 1) if source_key.startswith("staging/") else source_key
    return f"{archive_prefix.strip('/')}/{relative}"


def archive_object(
    client: Any,
    *,
    bucket: str,
    source_key: str,
    archive_prefix: str = "archive/processed",
    delete_source: bool = True,
) -> ArchiveResult:
    """Copy one object, verify its size, and only then delete its source."""
    source = client.head_object(Bucket=bucket, Key=source_key)
    source_size = int(source["ContentLength"])
    archive_key = archive_key_for(source_key, archive_prefix)
    client.copy_object(
        Bucket=bucket,
        CopySource={"Bucket": bucket, "Key": source_key},
        Key=archive_key,
    )
    archived = client.head_object(Bucket=bucket, Key=archive_key)
    archive_size = int(archived["ContentLength"])
    if archive_size != source_size:
        raise RuntimeError(
            f"Archive verification failed for {source_key}: "
            f"source={source_size}, archive={archive_size}."
        )
    if delete_source:
        client.delete_object(Bucket=bucket, Key=source_key)
    return ArchiveResult(source_key, archive_key, archive_size, delete_source)


def list_expired_objects(
    client: Any,
    *,
    bucket: str,
    prefix: str,
    retention_days: int,
    now: datetime | None = None,
) -> list[dict[str, Any]]:
    if retention_days < 1:
        raise ValueError("retention_days must be at least 1")
    current = now or datetime.now(timezone.utc)
    candidates: list[dict[str, Any]] = []
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix.strip("/") + "/"):
        for item in page.get("Contents", []):
            modified = item["LastModified"]
            age_days = (current - modified).total_seconds() / 86400
            if age_days >= retention_days:
                candidates.append(
                    {
                        "key": item["Key"],
                        "size": int(item.get("Size", 0)),
                        "last_modified": modified.isoformat(),
                        "age_days": int(age_days),
                    }
                )
    return candidates


def delete_objects(client: Any, *, bucket: str, keys: Iterable[str]) -> int:
    objects = [{"Key": key} for key in keys]
    deleted = 0
    for start in range(0, len(objects), 1000):
        batch = objects[start : start + 1000]
        if not batch:
            continue
        response = client.delete_objects(Bucket=bucket, Delete={"Objects": batch, "Quiet": True})
        errors = response.get("Errors", [])
        if errors:
            raise RuntimeError(f"Ozone archive cleanup failed: {errors}")
        deleted += len(batch)
    return deleted
