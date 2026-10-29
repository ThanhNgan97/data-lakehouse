"""Durable, create-only audit records for consumed GX publication receipts."""

from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime, timezone
from typing import Any


class ReceiptAuditError(RuntimeError):
    """A publication receipt could not be durably audited."""


def _client():
    import boto3

    return boto3.client(
        "s3",
        endpoint_url=os.getenv("S3_ENDPOINT") or None,
        region_name=os.getenv("S3_REGION") or None,
        aws_access_key_id=os.getenv("S3_ACCESS_KEY") or None,
        aws_secret_access_key=os.getenv("S3_SECRET_KEY") or None,
    )


def record_receipt_consumption(
    approval: dict[str, Any],
    *,
    publication_hash: str,
    status: str,
    client=None,
) -> str | None:
    """Persist one immutable audit event; retries return the same object URI."""
    if os.getenv("GX_RECEIPT_AUDIT_ENABLED", "false").lower() != "true":
        return None
    bucket = os.getenv("GX_RESULT_STORE_BUCKET", "").strip()
    if not bucket:
        raise ReceiptAuditError("GX_RESULT_STORE_BUCKET is required for receipt audit")
    identity = "|".join(
        str(approval.get(field) or "")
        for field in (
            "validation_id",
            "dataset_id",
            "data_layer",
            "run_id",
            "validated_branch",
            "validated_snapshot_id",
            "validated_commit_hash",
            "signature",
        )
    )
    receipt_id = hashlib.sha256(identity.encode("utf-8")).hexdigest()
    prefix = os.getenv("GX_RECEIPT_AUDIT_PREFIX", "gx/receipt-consumption").strip("/")
    key = f"{prefix}/{approval['dataset_id']}/{receipt_id}.json"
    payload = {
        "receipt_id": receipt_id,
        "validation_id": approval["validation_id"],
        "dataset_id": approval["dataset_id"],
        "data_layer": approval["data_layer"],
        "run_id": approval["run_id"],
        "candidate_branch": approval["validated_branch"],
        "candidate_snapshot_id": approval["validated_snapshot_id"],
        "candidate_hash": approval["validated_commit_hash"],
        "publication_hash": publication_hash,
        "publication_status": status,
        "consumed_at": datetime.now(timezone.utc).isoformat(),
        "key_id": approval["key_id"],
    }
    s3 = client or _client()
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return f"s3://{bucket}/{key}"
    except Exception as exc:
        response = getattr(exc, "response", {})
        code = str(response.get("Error", {}).get("Code", ""))
        status_code = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if code not in {"NoSuchKey", "404", "NotFound"} and status_code != 404:
            raise ReceiptAuditError("Unable to check GX receipt audit") from exc
    try:
        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=json.dumps(payload, sort_keys=True).encode("utf-8"),
            ContentType="application/json",
            Tagging="gx-artifact=receipt-consumption&gx-status=published",
        )
    except Exception as exc:
        raise ReceiptAuditError("Unable to persist GX receipt audit") from exc
    return f"s3://{bucket}/{key}"
