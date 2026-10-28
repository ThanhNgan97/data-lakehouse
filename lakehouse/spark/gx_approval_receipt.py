"""Signed, expiring GX publication approvals shared by GX and publishers."""

from __future__ import annotations

import hashlib
import hmac
import json
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping


class GXApprovalReceiptError(ValueError):
    """The approval is unsigned, expired, malformed, or bound elsewhere."""


def _secret() -> bytes:
    value = os.getenv("GX_APPROVAL_SECRET", "").strip()
    if len(value) < 32:
        raise GXApprovalReceiptError(
            "GX_APPROVAL_SECRET must contain at least 32 characters"
        )
    return value.encode("utf-8")


def _canonical(payload: Mapping[str, Any]) -> bytes:
    unsigned = {key: value for key, value in payload.items() if key != "signature"}
    return json.dumps(
        unsigned, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")


def sign_approval_receipt(
    claims: Mapping[str, Any],
    *,
    now: datetime | None = None,
    ttl_seconds: int | None = None,
) -> dict[str, Any]:
    issued = now or datetime.now(timezone.utc)
    if issued.tzinfo is None:
        raise GXApprovalReceiptError("Receipt time must be timezone-aware")
    ttl = ttl_seconds
    if ttl is None:
        try:
            ttl = int(os.getenv("GX_APPROVAL_TTL_SECONDS", "900"))
        except ValueError as exc:
            raise GXApprovalReceiptError(
                "GX_APPROVAL_TTL_SECONDS must be an integer"
            ) from exc
    if ttl < 1 or ttl > 86400:
        raise GXApprovalReceiptError("GX approval TTL must be between 1 and 86400 seconds")
    payload = dict(claims)
    payload.update(
        {
            "receipt_version": "gx-approval/v1",
            "issued_at": issued.astimezone(timezone.utc).isoformat(),
            "expires_at": (issued + timedelta(seconds=ttl))
            .astimezone(timezone.utc)
            .isoformat(),
        }
    )
    required = {
        "validation_id",
        "dataset_id",
        "data_layer",
        "contract_version",
        "run_id",
        "source_kind",
        "data_asset_ref",
        "validated_branch",
        "validated_snapshot_id",
        "validated_commit_hash",
        "candidate_hash",
        "audience",
    }
    missing = sorted(key for key in required if not str(payload.get(key) or "").strip())
    if missing:
        raise GXApprovalReceiptError("Receipt claims are missing: " + ", ".join(missing))
    payload["merge_allowed"] = True
    payload["signature"] = hmac.new(
        _secret(), _canonical(payload), hashlib.sha256
    ).hexdigest()
    return payload


def verify_approval_receipt(
    receipt: Mapping[str, Any],
    *,
    expected: Mapping[str, Any],
    now: datetime | None = None,
) -> dict[str, Any]:
    payload = dict(receipt)
    signature = str(payload.get("signature") or "")
    calculated = hmac.new(_secret(), _canonical(payload), hashlib.sha256).hexdigest()
    if not signature or not hmac.compare_digest(signature, calculated):
        raise GXApprovalReceiptError("GX approval signature is invalid")
    if payload.get("receipt_version") != "gx-approval/v1" or payload.get("merge_allowed") is not True:
        raise GXApprovalReceiptError("GX approval receipt is not publishable")
    try:
        expires_at = datetime.fromisoformat(str(payload["expires_at"]))
    except (KeyError, TypeError, ValueError) as exc:
        raise GXApprovalReceiptError("GX approval expiration is invalid") from exc
    current = now or datetime.now(timezone.utc)
    if expires_at.tzinfo is None or current >= expires_at:
        raise GXApprovalReceiptError("GX approval receipt has expired")
    mismatches = [
        key
        for key, value in expected.items()
        if str(payload.get(key) or "") != str(value)
    ]
    if mismatches:
        raise GXApprovalReceiptError(
            "GX approval does not match publication: " + ", ".join(sorted(mismatches))
        )
    return payload
