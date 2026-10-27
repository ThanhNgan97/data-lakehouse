"""Durable audit markers for validation identities invalidated by data changes."""

from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone

from .errors import ValidationPersistenceError


@dataclass(frozen=True)
class SnapshotRevocation:
    dataset_id: str
    data_layer: str
    nessie_branch: str
    revoked_snapshot_id: str
    revoked_commit_hash: str
    current_snapshot_id: str
    current_commit_hash: str

    def as_dict(self) -> dict[str, str]:
        return {
            **self.__dict__,
            "reason": "SNAPSHOT_MISMATCH",
            "revoked_at": datetime.now(timezone.utc).isoformat(),
        }


class S3SnapshotRevocationStore:
    def __init__(self, bucket: str, prefix: str = "gx/revocations") -> None:
        self.bucket = bucket
        self.prefix = prefix.strip("/")

    def save(self, revocation: SnapshotRevocation) -> str:
        identity = "|".join(
            (
                revocation.dataset_id,
                revocation.data_layer,
                revocation.nessie_branch,
                revocation.revoked_snapshot_id,
                revocation.revoked_commit_hash,
            )
        )
        marker = hashlib.sha256(identity.encode("utf-8")).hexdigest()
        key = f"{self.prefix}/{revocation.dataset_id}/{revocation.data_layer}/{marker}.json"
        try:
            import boto3

            boto3.client(
                "s3",
                endpoint_url=os.getenv("S3_ENDPOINT") or None,
                region_name=os.getenv("S3_REGION") or None,
                aws_access_key_id=os.getenv("S3_ACCESS_KEY") or None,
                aws_secret_access_key=os.getenv("S3_SECRET_KEY") or None,
            ).put_object(
                Bucket=self.bucket,
                Key=key,
                Body=json.dumps(revocation.as_dict(), sort_keys=True).encode("utf-8"),
                ContentType="application/json",
                Tagging="gx-artifact=validation-revocation",
            )
        except Exception as exc:
            raise ValidationPersistenceError(
                f"Unable to persist snapshot revocation s3://{self.bucket}/{key}"
            ) from exc
        return f"s3://{self.bucket}/{key}"


def revocation_store_from_env() -> S3SnapshotRevocationStore:
    bucket = os.getenv("GX_RESULT_STORE_BUCKET", "").strip()
    if not bucket:
        raise ValidationPersistenceError(
            "GX_RESULT_STORE_BUCKET is required for revocation audit"
        )
    return S3SnapshotRevocationStore(
        bucket,
        os.getenv("GX_REVOCATION_PREFIX", "gx/revocations"),
    )
