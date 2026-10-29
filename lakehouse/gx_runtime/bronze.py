"""Contract-routed technical validation for raw Bronze ingestion evidence."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from .errors import ValidationConfigurationError
from .models import ExecutionEngine, SourceKind


_SAFE_ID = re.compile(r"^[A-Za-z0-9_][A-Za-z0-9_.:-]{0,255}$")
_OPERATORS = {
    "eq": lambda observed, expected: observed == expected,
    "gte": lambda observed, expected: observed >= expected,
    "lte": lambda observed, expected: observed <= expected,
}


@dataclass(frozen=True)
class BronzeValidationResult:
    validation_id: str
    dataset_id: str
    contract_version: str
    data_asset_ref: str
    source_kind: str
    run_id: str
    artifact_ref: str
    status: str
    merge_allowed: bool
    evaluated_expectations: int
    failures: tuple[dict[str, Any], ...]
    metrics: dict[str, Any]
    validated_at: str

    def as_dict(self) -> dict[str, Any]:
        return {
            **self.__dict__,
            "failures": list(self.failures),
        }


def persist_bronze_result(result: BronzeValidationResult) -> str:
    import boto3
    import os

    bucket = os.getenv("GX_RESULT_STORE_BUCKET", "").strip()
    if not bucket:
        raise ValidationConfigurationError("GX_RESULT_STORE_BUCKET is required")
    prefix = os.getenv("GX_RESULT_STORE_PREFIX", "gx/validation-results").strip("/")
    key = (
        f"{prefix}/{result.dataset_id}/bronze/{result.run_id}/"
        f"{result.validation_id}.json"
    )
    client = boto3.client(
        "s3",
        endpoint_url=os.getenv("S3_ENDPOINT") or None,
        region_name=os.getenv("S3_REGION") or None,
        aws_access_key_id=os.getenv("S3_ACCESS_KEY") or None,
        aws_secret_access_key=os.getenv("S3_SECRET_KEY") or None,
    )
    try:
        client.put_object(
            Bucket=bucket,
            Key=key,
            Body=json.dumps(result.as_dict(), sort_keys=True).encode("utf-8"),
            ContentType="application/json",
            Tagging="gx-artifact=validation-result&gx-layer=bronze",
        )
    except Exception as exc:
        from .errors import ValidationPersistenceError

        raise ValidationPersistenceError("Unable to persist Bronze GX result") from exc
    return f"s3://{bucket}/{key}"


def validate_bronze_evidence(
    body: Mapping[str, Any], *, routes, suites
) -> BronzeValidationResult:
    dataset_id = str(body.get("dataset_id") or "").strip()
    contract_version = str(body.get("contract_version") or "").strip() or None
    source_kind_value = str(body.get("source_kind") or "").strip().lower()
    run_id = str(body.get("run_id") or "").strip()
    artifact_ref = str(body.get("artifact_ref") or "").strip()
    metrics = body.get("metrics")
    if not dataset_id or not run_id or not artifact_ref or not isinstance(metrics, dict):
        raise ValidationConfigurationError(
            "Bronze validation requires dataset_id, run_id, artifact_ref and metrics"
        )
    if not _SAFE_ID.fullmatch(dataset_id) or len(artifact_ref) > 2048:
        raise ValidationConfigurationError("Bronze validation identity is unsafe")
    try:
        source_kind = SourceKind(source_kind_value)
    except ValueError as exc:
        raise ValidationConfigurationError("Unsupported Bronze source_kind") from exc
    for name in ("row_count", "duplicate_ingestion_count"):
        value = metrics.get(name)
        if not isinstance(value, int) or isinstance(value, bool) or value < 0:
            raise ValidationConfigurationError(
                f"Bronze metric {name!r} must be a non-negative integer"
            )
    corrupt_ratio = metrics.get("corrupt_record_ratio")
    if (
        not isinstance(corrupt_ratio, (int, float))
        or isinstance(corrupt_ratio, bool)
        or not 0 <= corrupt_ratio <= 1
    ):
        raise ValidationConfigurationError(
            "Bronze metric 'corrupt_record_ratio' must be between 0 and 1"
        )
    plan = routes.resolve(
        dataset_id=dataset_id,
        data_layer="bronze",
        contract_version=contract_version,
        execution_engine=ExecutionEngine.SQL,
        source_kind=source_kind,
    )
    failures: list[dict[str, Any]] = []
    evaluated = 0
    evaluated_metrics: dict[str, Any] = {}
    for suite in plan.suites:
        metadata = suites.resolve(suite, ExecutionEngine.SQL)
        try:
            definition = json.loads(Path(metadata.definition_path).read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise ValidationConfigurationError("Unable to load Bronze suite") from exc
        if definition.get("format") != "lakehouse-bronze-suite/v1":
            raise ValidationConfigurationError("Unsupported Bronze suite format")
        for expectation in definition.get("expectations", []):
            supported = expectation.get("source_kinds")
            if supported and source_kind.value not in supported:
                continue
            metric = str(expectation.get("metric") or "")
            if metric not in metrics:
                raise ValidationConfigurationError(
                    f"Bronze evidence is missing required metric {metric!r}"
                )
            success = expectation.get("success") or {}
            operator = str(success.get("operator") or "")
            if operator not in _OPERATORS or "value" not in success:
                raise ValidationConfigurationError("Invalid Bronze expectation policy")
            observed = metrics[metric]
            expected = success["value"]
            if not isinstance(observed, (bool, int, float)) or not isinstance(
                expected, (bool, int, float)
            ):
                raise ValidationConfigurationError(
                    f"Bronze metric {metric!r} must be boolean or numeric"
                )
            evaluated_metrics[metric] = observed
            evaluated += 1
            try:
                passed = bool(_OPERATORS[operator](observed, expected))
            except TypeError as exc:
                raise ValidationConfigurationError(
                    f"Bronze metric {metric!r} has an invalid type"
                ) from exc
            if not passed:
                failures.append(
                    {
                        "expectation_id": expectation.get("id"),
                        "metric": metric,
                        "severity": expectation.get("severity", "critical"),
                        "observed": observed,
                        "operator": operator,
                        "expected": expected,
                    }
                )
    blocking = any(item["severity"] == "critical" for item in failures)
    identity = "|".join(
        (dataset_id, plan.contract_version, source_kind.value, run_id, artifact_ref)
    )
    return BronzeValidationResult(
        validation_id=hashlib.sha256(identity.encode("utf-8")).hexdigest(),
        dataset_id=dataset_id,
        contract_version=plan.contract_version,
        data_asset_ref=plan.data_asset_ref,
        source_kind=source_kind.value,
        run_id=run_id,
        artifact_ref=artifact_ref,
        status="FAILED" if blocking else ("WARNING" if failures else "PASSED"),
        merge_allowed=not blocking,
        evaluated_expectations=evaluated,
        failures=tuple(failures),
        metrics=evaluated_metrics,
        validated_at=datetime.now(timezone.utc).isoformat(),
    )
