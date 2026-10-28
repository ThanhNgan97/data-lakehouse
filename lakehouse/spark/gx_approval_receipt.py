"""Ed25519-signed, expiring GX publication approvals."""
from __future__ import annotations
import base64, json, os
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping
from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey, Ed25519PublicKey

class GXApprovalReceiptError(ValueError):
    pass

def _decode_key(value: str, *, name: str) -> bytes:
    try: decoded = base64.b64decode(value, validate=True)
    except Exception as exc: raise GXApprovalReceiptError(f"{name} must be valid base64") from exc
    if len(decoded) != 32: raise GXApprovalReceiptError(f"{name} must encode exactly 32 bytes")
    return decoded

def _signing_key():
    key_id = os.getenv("GX_APPROVAL_SIGNING_KEY_ID", "").strip()
    encoded = os.getenv("GX_APPROVAL_PRIVATE_KEY", "").strip()
    if not key_id or not encoded: raise GXApprovalReceiptError("GX approval signing key is not configured")
    return key_id, Ed25519PrivateKey.from_private_bytes(_decode_key(encoded, name="GX_APPROVAL_PRIVATE_KEY"))

def _verification_keys():
    try:
        values = json.loads(os.getenv("GX_APPROVAL_PUBLIC_KEYS", "{}"))
        if not isinstance(values, dict) or not values: raise TypeError("empty keyring")
        return {str(k): Ed25519PublicKey.from_public_bytes(_decode_key(str(v), name=f"public key {k}")) for k, v in values.items()}
    except (json.JSONDecodeError, TypeError) as exc: raise GXApprovalReceiptError("GX_APPROVAL_PUBLIC_KEYS is invalid") from exc

def _revoked_key_ids():
    return frozenset(x.strip() for x in os.getenv("GX_APPROVAL_REVOKED_KEY_IDS", "").split(",") if x.strip())

def _canonical(payload: Mapping[str, Any]) -> bytes:
    return json.dumps({k: v for k, v in payload.items() if k != "signature"}, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()

def sign_approval_receipt(claims: Mapping[str, Any], *, now: datetime | None = None, ttl_seconds: int | None = None) -> dict[str, Any]:
    issued = now or datetime.now(timezone.utc)
    if issued.tzinfo is None: raise GXApprovalReceiptError("Receipt time must be timezone-aware")
    try: ttl = ttl_seconds if ttl_seconds is not None else int(os.getenv("GX_APPROVAL_TTL_SECONDS", "900"))
    except ValueError as exc: raise GXApprovalReceiptError("GX_APPROVAL_TTL_SECONDS must be an integer") from exc
    if ttl < 1 or ttl > 86400: raise GXApprovalReceiptError("GX approval TTL must be between 1 and 86400 seconds")
    key_id, private_key = _signing_key()
    if key_id in _revoked_key_ids(): raise GXApprovalReceiptError("GX approval signing key is revoked")
    payload = dict(claims)
    payload.update({"receipt_version":"gx-approval/v2", "algorithm":"Ed25519", "key_id":key_id, "issued_at":issued.astimezone(timezone.utc).isoformat(), "expires_at":(issued+timedelta(seconds=ttl)).astimezone(timezone.utc).isoformat(), "merge_allowed":True})
    required = {"validation_id","dataset_id","data_layer","contract_version","run_id","source_kind","data_asset_ref","validated_branch","validated_snapshot_id","validated_commit_hash","candidate_hash","audience"}
    missing = sorted(k for k in required if not str(payload.get(k) or "").strip())
    if missing: raise GXApprovalReceiptError("Receipt claims are missing: " + ", ".join(missing))
    payload["signature"] = base64.b64encode(private_key.sign(_canonical(payload))).decode()
    return payload

def verify_approval_receipt(receipt: Mapping[str, Any], *, expected: Mapping[str, Any], now: datetime | None = None) -> dict[str, Any]:
    payload = dict(receipt)
    if payload.get("receipt_version") != "gx-approval/v2" or payload.get("algorithm") != "Ed25519" or payload.get("merge_allowed") is not True: raise GXApprovalReceiptError("GX approval receipt is not publishable")
    key_id = str(payload.get("key_id") or "")
    if not key_id or key_id in _revoked_key_ids(): raise GXApprovalReceiptError("GX approval key is missing or revoked")
    public_key = _verification_keys().get(key_id)
    if public_key is None: raise GXApprovalReceiptError("GX approval key_id is not trusted")
    try:
        signature = base64.b64decode(str(payload.get("signature") or ""), validate=True)
        public_key.verify(signature, _canonical(payload))
    except (ValueError, InvalidSignature) as exc: raise GXApprovalReceiptError("GX approval signature is invalid") from exc
    try: expires_at = datetime.fromisoformat(str(payload["expires_at"]))
    except (KeyError, TypeError, ValueError) as exc: raise GXApprovalReceiptError("GX approval expiration is invalid") from exc
    current = now or datetime.now(timezone.utc)
    if expires_at.tzinfo is None or current >= expires_at: raise GXApprovalReceiptError("GX approval receipt has expired")
    mismatches = [k for k, v in expected.items() if str(payload.get(k) or "") != str(v)]
    if mismatches: raise GXApprovalReceiptError("GX approval does not match publication: " + ", ".join(sorted(mismatches)))
    return payload
