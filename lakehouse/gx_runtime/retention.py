"""Bounded cleanup for GX-owned object-store artifacts."""

from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from .errors import ValidationConfigurationError, ValidationPersistenceError


@dataclass(frozen=True)
class CleanupReport:
    bucket: str
    prefixes: tuple[str, ...]
    cutoff: str
    dry_run: bool
    scanned: int
    eligible: int
    deleted: int
    capped: bool

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def configured_prefixes() -> tuple[str, ...]:
    raw = os.getenv(
        "GX_RETENTION_PREFIXES",
        "gx/validation-results,gx/revocations,gx/quarantine,gx/onboarding-proposals,gx/receipt-consumption",
    )
    prefixes = tuple(sorted({item.strip().strip("/") for item in raw.split(",") if item.strip()}))
    if not prefixes or any(not item.startswith("gx/") or ".." in item for item in prefixes):
        raise ValidationConfigurationError("GX retention prefixes must stay below gx/")
    return prefixes


def cleanup(
    client,
    *,
    bucket: str,
    retention_days: int,
    dry_run: bool = True,
    max_delete: int = 1000,
    now: datetime | None = None,
) -> CleanupReport:
    if retention_days < 1 or max_delete < 1 or max_delete > 10000:
        raise ValidationConfigurationError("Invalid GX cleanup bounds")
    prefixes = configured_prefixes()
    current = now or datetime.now(timezone.utc)
    cutoff = current - timedelta(days=retention_days)
    try:
        active_quarantine_days = int(
            os.getenv("GX_QUARANTINE_ACTIVE_RETENTION_DAYS", "365")
        )
    except ValueError as exc:
        raise ValidationConfigurationError(
            "GX_QUARANTINE_ACTIVE_RETENTION_DAYS must be an integer"
        ) from exc
    if active_quarantine_days < retention_days or active_quarantine_days > 3650:
        raise ValidationConfigurationError(
            "Active quarantine retention must be between result retention and 3650 days"
        )
    active_quarantine_cutoff = current - timedelta(days=active_quarantine_days)
    eligible: list[dict[str, str]] = []
    scanned = 0
    try:
        paginator = client.get_paginator("list_objects_v2")
        for prefix in prefixes:
            for page in paginator.paginate(Bucket=bucket, Prefix=prefix + "/"):
                for item in page.get("Contents", []):
                    scanned += 1
                    modified = item.get("LastModified")
                    key = str(item["Key"])
                    if modified is None:
                        continue
                    item_cutoff = cutoff
                    if key.startswith("gx/quarantine/"):
                        response = client.get_object(Bucket=bucket, Key=key)
                        payload = json.loads(response["Body"].read())
                        status = str(payload.get("status") or "")
                        if status in {"QUARANTINED", "REVALIDATION_REQUESTED"}:
                            item_cutoff = active_quarantine_cutoff
                        elif status not in {"APPROVED_FOR_RELEASE", "DISCARDED"}:
                            continue
                    if modified.astimezone(timezone.utc) < item_cutoff:
                        eligible.append({"Key": key})
        selected = eligible[:max_delete]
        if selected and not dry_run:
            for offset in range(0, len(selected), 1000):
                client.delete_objects(
                    Bucket=bucket,
                    Delete={"Objects": selected[offset : offset + 1000], "Quiet": True},
                )
    except Exception as exc:
        raise ValidationPersistenceError("GX retention cleanup failed") from exc
    return CleanupReport(
        bucket=bucket,
        prefixes=prefixes,
        cutoff=cutoff.isoformat(),
        dry_run=dry_run,
        scanned=scanned,
        eligible=len(eligible),
        deleted=0 if dry_run else len(selected),
        capped=len(eligible) > max_delete,
    )


def main() -> None:
    import boto3
    bucket = os.getenv("GX_RESULT_STORE_BUCKET", "").strip()
    if not bucket:
        raise ValidationConfigurationError("GX_RESULT_STORE_BUCKET is required")
    client = boto3.client(
        "s3",
        endpoint_url=os.getenv("S3_ENDPOINT") or None,
        region_name=os.getenv("S3_REGION") or None,
        aws_access_key_id=os.getenv("S3_ACCESS_KEY") or None,
        aws_secret_access_key=os.getenv("S3_SECRET_KEY") or None,
    )
    report = cleanup(
        client,
        bucket=bucket,
        retention_days=int(os.getenv("GX_RESULT_RETENTION_DAYS", "90")),
        dry_run=os.getenv("GX_RETENTION_DRY_RUN", "true").lower() == "true",
        max_delete=int(os.getenv("GX_RETENTION_MAX_DELETE", "1000")),
    )
    print(json.dumps(report.as_dict(), sort_keys=True))


if __name__ == "__main__":
    main()
