"""Persistent validation-result storage implementations."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from typing import Protocol

from .errors import ValidationConfigurationError, ValidationPersistenceError
from .models import ValidationSummary


class ResultStore(Protocol):
    def save(self, summary: ValidationSummary) -> str:
        """Persist a summary and return its durable URI."""


@dataclass
class InMemoryResultStore:
    """Test-only store; production construction never selects this backend."""

    records: dict[str, dict] = field(default_factory=dict)

    def save(self, summary: ValidationSummary) -> str:
        self.records[summary.validation_id] = summary.as_dict(
            redact_observed_values=_redaction_enabled()
        )
        return f"memory://{summary.validation_id}"


@dataclass(frozen=True)
class S3ResultStore:
    bucket: str
    prefix: str = "gx/validation-results"
    endpoint_url: str | None = None
    region_name: str | None = None
    access_key_id: str | None = None
    secret_access_key: str | None = None

    def save(self, summary: ValidationSummary) -> str:
        import boto3

        key = "/".join(
            part.strip("/")
            for part in (
                self.prefix,
                summary.dataset_id,
                summary.data_layer,
                summary.run_id,
                f"{summary.validation_id}.json",
            )
        )
        try:
            client = boto3.client(
                "s3",
                endpoint_url=self.endpoint_url,
                region_name=self.region_name,
                aws_access_key_id=self.access_key_id,
                aws_secret_access_key=self.secret_access_key,
            )
            client.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=json.dumps(
                    summary.as_dict(redact_observed_values=_redaction_enabled()),
                    sort_keys=True,
                ).encode("utf-8"),
                ContentType="application/json",
                Metadata={
                    "gx-suite-revision": summary.suite_revision or "unknown",
                    "gx-retention-days": os.getenv("GX_RESULT_RETENTION_DAYS", "90"),
                },
                Tagging=(
                    "gx-artifact=validation-result&gx-retention-days="
                    + os.getenv("GX_RESULT_RETENTION_DAYS", "90")
                ),
            )
        except Exception as exc:
            raise ValidationPersistenceError(
                f"Unable to persist validation summary to s3://{self.bucket}/{key}"
            ) from exc
        return f"s3://{self.bucket}/{key}"


def result_store_from_env() -> ResultStore:
    backend = os.getenv("GX_RESULT_STORE_BACKEND", "s3").strip().lower()
    if backend != "s3":
        raise ValidationConfigurationError(
            "GX_RESULT_STORE_BACKEND must be 's3'; local filesystem results "
            "are not allowed for the standalone runtime"
        )
    bucket = os.getenv("GX_RESULT_STORE_BUCKET", "").strip()
    if not bucket:
        raise ValidationConfigurationError("GX_RESULT_STORE_BUCKET is required")
    return S3ResultStore(
        bucket=bucket,
        prefix=os.getenv("GX_RESULT_STORE_PREFIX", "gx/validation-results"),
        endpoint_url=os.getenv("S3_ENDPOINT") or None,
        region_name=os.getenv("S3_REGION") or None,
        access_key_id=os.getenv("S3_ACCESS_KEY") or None,
        secret_access_key=os.getenv("S3_SECRET_KEY") or None,
    )


def _redaction_enabled() -> bool:
    return os.getenv("GX_REDACT_OBSERVED_VALUES", "true").strip().lower() != "false"
