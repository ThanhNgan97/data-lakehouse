"""Durable candidate-level quarantine evidence for rejected GX validations."""

from __future__ import annotations

import hashlib
import json
import os
from dataclasses import asdict, dataclass, replace
from datetime import datetime, timezone
from enum import Enum
from typing import Any

from .errors import ValidationConfigurationError, ValidationPersistenceError
from .models import ValidationSummary
try:
    from gx_approval_receipt import GXApprovalReceiptError, verify_approval_receipt
except ModuleNotFoundError:
    from spark.gx_approval_receipt import GXApprovalReceiptError, verify_approval_receipt


class QuarantineStatus(str, Enum):
    QUARANTINED = "QUARANTINED"
    REVALIDATION_REQUESTED = "REVALIDATION_REQUESTED"
    APPROVED_FOR_RELEASE = "APPROVED_FOR_RELEASE"
    DISCARDED = "DISCARDED"


@dataclass(frozen=True)
class QuarantineManifest:
    quarantine_id: str
    status: QuarantineStatus
    dataset_id: str
    data_layer: str
    source_kind: str | None
    data_asset_ref: str
    run_id: str
    contract_version: str
    candidate_branch: str | None
    candidate_snapshot_id: str | None
    candidate_hash: str | None
    validation_id: str
    validation_uri: str
    reason: str
    critical_failure_count: int
    created_at: str
    updated_at: str
    requested_by: str | None = None

    @classmethod
    def from_summary(
        cls, summary: ValidationSummary, validation_uri: str, *, reason: str
    ) -> "QuarantineManifest":
        identity = "|".join(
            (
                summary.dataset_id,
                summary.data_layer,
                summary.run_id,
                summary.nessie_branch or "",
                summary.snapshot_id or "",
                summary.nessie_commit_hash or "",
            )
        )
        now = datetime.now(timezone.utc).isoformat()
        return cls(
            quarantine_id=hashlib.sha256(identity.encode("utf-8")).hexdigest(),
            status=QuarantineStatus.QUARANTINED,
            dataset_id=summary.dataset_id,
            data_layer=summary.data_layer,
            source_kind=summary.source_kind.value if summary.source_kind else None,
            data_asset_ref=summary.data_asset_ref,
            run_id=summary.run_id,
            contract_version=summary.contract_version,
            candidate_branch=summary.nessie_branch,
            candidate_snapshot_id=summary.snapshot_id,
            candidate_hash=summary.nessie_commit_hash,
            validation_id=summary.validation_id,
            validation_uri=validation_uri,
            reason=reason,
            critical_failure_count=summary.critical_failure_count,
            created_at=now,
            updated_at=now,
        )

    @classmethod
    def from_bronze_result(
        cls, result, validation_uri: str
    ) -> "QuarantineManifest":
        now = datetime.now(timezone.utc).isoformat()
        identity = "|".join(
            (result.dataset_id, "bronze", result.run_id, result.artifact_ref)
        )
        return cls(
            quarantine_id=hashlib.sha256(identity.encode("utf-8")).hexdigest(),
            status=QuarantineStatus.QUARANTINED,
            dataset_id=result.dataset_id,
            data_layer="bronze",
            source_kind=result.source_kind,
            data_asset_ref=result.data_asset_ref,
            run_id=result.run_id,
            contract_version=result.contract_version,
            candidate_branch=None,
            candidate_snapshot_id=None,
            candidate_hash=None,
            validation_id=result.validation_id,
            validation_uri=validation_uri,
            reason="BRONZE_DATA_QUALITY_FAILED",
            critical_failure_count=sum(
                1 for failure in result.failures if failure.get("severity") == "critical"
            ),
            created_at=now,
            updated_at=now,
        )

    def request_revalidation(self, caller: str) -> "QuarantineManifest":
        if self.status is not QuarantineStatus.QUARANTINED:
            raise ValidationConfigurationError("Only quarantined candidates may be revalidated")
        if self.data_layer == "bronze":
            raise ValidationConfigurationError(
                "Bronze artifacts must be corrected and re-ingested before revalidation"
            )
        return replace(
            self,
            status=QuarantineStatus.REVALIDATION_REQUESTED,
            requested_by=caller,
            updated_at=datetime.now(timezone.utc).isoformat(),
        )

    def approve_for_release(
        self, caller: str, approval: dict[str, Any]
    ) -> "QuarantineManifest":
        if self.status is not QuarantineStatus.REVALIDATION_REQUESTED:
            raise ValidationConfigurationError(
                "Release requires a quarantined candidate awaiting revalidation"
            )
        expected = {
            "dataset_id": self.dataset_id,
            "data_layer": self.data_layer,
            "contract_version": self.contract_version,
            "run_id": self.run_id,
            "source_kind": self.source_kind,
            "data_asset_ref": self.data_asset_ref,
            "validated_branch": self.candidate_branch,
            "validated_snapshot_id": self.candidate_snapshot_id,
            "validated_commit_hash": self.candidate_hash,
            "candidate_hash": self.candidate_hash,
            "audience": f"{self.data_layer}-publisher",
        }
        try:
            verify_approval_receipt(approval, expected=expected)
        except GXApprovalReceiptError as exc:
            raise ValidationConfigurationError(str(exc)) from exc
        return replace(
            self,
            status=QuarantineStatus.APPROVED_FOR_RELEASE,
            requested_by=caller,
            validation_id=str(approval["validation_id"]),
            updated_at=datetime.now(timezone.utc).isoformat(),
        )

    def revalidation_failed(self, caller: str) -> "QuarantineManifest":
        if self.status is not QuarantineStatus.REVALIDATION_REQUESTED:
            raise ValidationConfigurationError(
                "Only a pending revalidation may return to quarantine"
            )
        return replace(
            self,
            status=QuarantineStatus.QUARANTINED,
            requested_by=caller,
            updated_at=datetime.now(timezone.utc).isoformat(),
        )

    def discard(self, caller: str) -> "QuarantineManifest":
        if self.status is QuarantineStatus.APPROVED_FOR_RELEASE:
            raise ValidationConfigurationError("Approved candidate cannot be discarded")
        return replace(
            self,
            status=QuarantineStatus.DISCARDED,
            requested_by=caller,
            updated_at=datetime.now(timezone.utc).isoformat(),
        )

    def as_dict(self) -> dict[str, Any]:
        value = asdict(self)
        value["status"] = self.status.value
        return value


class S3QuarantineStore:
    def __init__(self, bucket: str, prefix: str = "gx/quarantine") -> None:
        self.bucket = bucket
        self.prefix = prefix.strip("/")

    def _client(self):
        import boto3
        return boto3.client(
            "s3",
            endpoint_url=os.getenv("S3_ENDPOINT") or None,
            region_name=os.getenv("S3_REGION") or None,
            aws_access_key_id=os.getenv("S3_ACCESS_KEY") or None,
            aws_secret_access_key=os.getenv("S3_SECRET_KEY") or None,
        )

    def save(self, manifest: QuarantineManifest, *, if_match: str | None = None) -> str:
        key = f"{self.prefix}/{manifest.dataset_id}/{manifest.quarantine_id}.json"
        try:
            arguments = dict(
                Bucket=self.bucket,
                Key=key,
                Body=json.dumps(manifest.as_dict(), sort_keys=True).encode("utf-8"),
                ContentType="application/json",
                Tagging=f"gx-artifact=quarantine-manifest&gx-status={manifest.status.value.lower()}",
            )
            if if_match:
                arguments["IfMatch"] = if_match
            else:
                arguments["IfNoneMatch"] = "*"
            self._client().put_object(**arguments)
        except Exception as exc:
            raise ValidationPersistenceError("Unable to persist quarantine manifest") from exc
        return f"s3://{self.bucket}/{key}"

    def load(self, dataset_id: str, quarantine_id: str) -> tuple[QuarantineManifest, str]:
        key = f"{self.prefix}/{dataset_id}/{quarantine_id}.json"
        try:
            response = self._client().get_object(Bucket=self.bucket, Key=key)
            payload = json.loads(response["Body"].read())
            payload["status"] = QuarantineStatus(payload["status"])
            return QuarantineManifest(**payload), str(response["ETag"])
        except Exception as exc:
            raise ValidationPersistenceError("Unable to load quarantine manifest") from exc


def quarantine_store_from_env() -> S3QuarantineStore:
    bucket = os.getenv("GX_RESULT_STORE_BUCKET", "").strip()
    if not bucket:
        raise ValidationConfigurationError("GX_RESULT_STORE_BUCKET is required")
    return S3QuarantineStore(bucket, os.getenv("GX_QUARANTINE_PREFIX", "gx/quarantine"))
