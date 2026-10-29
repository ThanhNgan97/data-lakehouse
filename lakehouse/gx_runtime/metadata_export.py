"""OpenMetadata-facing validation contract without an OpenMetadata dependency."""

from __future__ import annotations

import json
import logging
import os
from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any, Callable, Protocol
from urllib.parse import quote
from urllib.request import Request, urlopen

from .errors import MetadataExportError, ValidationConfigurationError
from .models import ValidationStatus, ValidationSummary


@dataclass(frozen=True)
class ValidationMetadataEvent:
    dataset: str
    layer: str
    run_id: str
    suite_name: str
    suite_version: str
    status: str
    branch: str
    snapshot_id: str
    commit_hash: str
    data_docs_url: str | None
    validation_timestamp: str
    validation_id: str
    validation_uri: str

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


class MetadataExporter(Protocol):
    def publish(self, event: ValidationMetadataEvent) -> str: ...


def events_from_summary(
    summary: ValidationSummary,
    validation_uri: str,
) -> tuple[ValidationMetadataEvent, ...]:
    identity = {
        "branch": summary.validated_branch,
        "snapshot_id": summary.validated_snapshot_id,
        "commit_hash": summary.validated_commit_hash,
        "validation_uri": validation_uri,
    }
    missing = [name for name, value in identity.items() if not value]
    if missing:
        raise ValidationConfigurationError(
            "Metadata export requires: " + ", ".join(missing)
        )
    return tuple(
        ValidationMetadataEvent(
            dataset=summary.dataset_id,
            layer=summary.data_layer,
            run_id=summary.run_id,
            suite_name=name,
            suite_version=summary.suite_versions[name],
            status=summary.status.value,
            branch=str(summary.validated_branch),
            snapshot_id=str(summary.validated_snapshot_id),
            commit_hash=str(summary.validated_commit_hash),
            data_docs_url=summary.data_docs_url,
            validation_timestamp=summary.validation_timestamp,
            validation_id=summary.validation_id,
            validation_uri=validation_uri,
        )
        for name in summary.suite_names
    )


class OpenMetadataTestCaseAdapter:
    """Publish GX metadata using OpenMetadata's test-case-result HTTP API."""

    STATUS_MAP = {
        ValidationStatus.PASSED.value: "Success",
        ValidationStatus.FAILED.value: "Failed",
        # OpenMetadata has no warning state. Aborted is neutral and preserves
        # the original GX status in testResultValue.
        ValidationStatus.WARNING.value: "Aborted",
    }

    def __init__(
        self,
        host_port: str,
        jwt_token: str,
        *,
        service_name: str = "lakehouse-trino",
        api_style: str = "current",
        timeout_seconds: float = 10,
        opener: Callable[..., Any] = urlopen,
    ) -> None:
        if not host_port.strip() or not jwt_token.strip():
            raise ValidationConfigurationError(
                "OpenMetadata host and JWT token are required"
            )
        self.host_port = host_port.rstrip("/")
        self.jwt_token = jwt_token
        self.service_name = service_name
        if api_style not in {"current", "legacy"}:
            raise ValidationConfigurationError(
                "OpenMetadata API style must be current or legacy"
            )
        self.api_style = api_style
        self.timeout_seconds = timeout_seconds
        self.opener = opener

    def test_case_fqn(self, event: ValidationMetadataEvent) -> str:
        dataset_path = event.dataset.replace(".", "_")
        return (
            f"{self.service_name}.lakehouse.{event.layer}.{dataset_path}."
            f"gx_{event.suite_name}_{event.suite_version.replace('.', '_')}"
        )

    @staticmethod
    def _payload(event: ValidationMetadataEvent) -> dict[str, Any]:
        try:
            timestamp_ms = int(
                datetime.fromisoformat(
                    event.validation_timestamp.replace("Z", "+00:00")
                ).timestamp()
                * 1000
            )
        except (TypeError, ValueError) as exc:
            raise MetadataExportError("Invalid validation timestamp") from exc
        values = {
            "dataset": event.dataset,
            "layer": event.layer,
            "run_id": event.run_id,
            "suite_name": event.suite_name,
            "suite_version": event.suite_version,
            "gx_status": event.status,
            "branch": event.branch,
            "snapshot_id": event.snapshot_id,
            "commit_hash": event.commit_hash,
            "data_docs_url": event.data_docs_url or "",
            "validation_id": event.validation_id,
            "validation_uri": event.validation_uri,
        }
        return {
            "timestamp": timestamp_ms,
            "testCaseStatus": OpenMetadataTestCaseAdapter.STATUS_MAP[event.status],
            "result": f"GX {event.status}: {event.suite_name}@{event.suite_version}",
            "testResultValue": [
                {"name": name, "value": str(value)}
                for name, value in values.items()
            ],
        }

    def publish(self, event: ValidationMetadataEvent) -> str:
        fqn = self.test_case_fqn(event)
        if self.api_style == "current":
            path = f"testCaseResults/{quote(fqn, safe='')}"
            method = "POST"
        else:
            path = f"{quote(fqn, safe='')}/testCaseResult"
            method = "PUT"
        url = f"{self.host_port}/api/v1/dataQuality/testCases/{path}"
        call = Request(
            url,
            data=json.dumps(self._payload(event)).encode("utf-8"),
            headers={
                "Authorization": f"Bearer {self.jwt_token}",
                "Content-Type": "application/json",
            },
            method=method,
        )
        try:
            with self.opener(call, timeout=self.timeout_seconds):
                return fqn
        except Exception as exc:
            raise MetadataExportError(
                f"Unable to publish GX metadata for {fqn}"
            ) from exc


@dataclass(frozen=True)
class MetadataExportOutcome:
    enabled: bool
    published: int
    failed: int
    references: tuple[str, ...] = ()


def export_validation_metadata(
    summary: ValidationSummary,
    validation_uri: str,
    exporter: MetadataExporter,
    *,
    required: bool = False,
) -> MetadataExportOutcome:
    references: list[str] = []
    failed = 0
    for event in events_from_summary(summary, validation_uri):
        try:
            references.append(exporter.publish(event))
        except Exception as exc:
            failed += 1
            if required:
                if isinstance(exc, MetadataExportError):
                    raise
                raise MetadataExportError("Metadata export failed") from exc
            logging.warning("OpenMetadata export failed: %s", exc)
    return MetadataExportOutcome(True, len(references), failed, tuple(references))


def openmetadata_exporter_from_env() -> OpenMetadataTestCaseAdapter | None:
    enabled = os.getenv("GX_OPENMETADATA_ENABLED", "false").lower() == "true"
    if not enabled:
        return None
    return OpenMetadataTestCaseAdapter(
        os.getenv("OPENMETADATA_HOST_PORT", ""),
        os.getenv("OPENMETADATA_JWT_TOKEN", ""),
        service_name=os.getenv("OPENMETADATA_SERVICE_NAME", "lakehouse-trino"),
        api_style=os.getenv("GX_OPENMETADATA_API_STYLE", "current"),
        timeout_seconds=float(os.getenv("GX_OPENMETADATA_TIMEOUT_SECONDS", "10")),
    )
