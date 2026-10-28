"""Production-only guardrails configured without process-local state."""

from __future__ import annotations

import os
from dataclasses import dataclass

from .errors import ValidationConfigurationError
from .models import ExecutionMode


def _boolean(name: str, default: bool) -> bool:
    value = os.getenv(name, str(default)).strip().lower()
    if value not in {"true", "false"}:
        raise ValidationConfigurationError(f"{name} must be true or false")
    return value == "true"


@dataclass(frozen=True)
class ProductionSettings:
    environment: str
    suite_revision: str | None
    result_retention_days: int
    redact_observed_values: bool

    @property
    def production(self) -> bool:
        return self.environment == "production"

    @classmethod
    def from_env(cls) -> "ProductionSettings":
        environment = os.getenv("GX_ENVIRONMENT", "development").strip().lower()
        revision = os.getenv("GX_SUITE_REVISION", "").strip() or None
        try:
            retention = int(os.getenv("GX_RESULT_RETENTION_DAYS", "90"))
        except ValueError as exc:
            raise ValidationConfigurationError(
                "GX_RESULT_RETENTION_DAYS must be an integer"
            ) from exc
        if retention < 1 or retention > 3650:
            raise ValidationConfigurationError(
                "GX_RESULT_RETENTION_DAYS must be between 1 and 3650"
            )
        redaction = _boolean("GX_REDACT_OBSERVED_VALUES", True)
        if environment == "production":
            if not revision:
                raise ValidationConfigurationError(
                    "GX_SUITE_REVISION is required in production"
                )
            if not redaction:
                raise ValidationConfigurationError(
                    "GX_REDACT_OBSERVED_VALUES must be true in production"
                )
            approval_secret = os.getenv("GX_APPROVAL_SECRET", "").strip()
            if (
                len(approval_secret) < 32
                or approval_secret == "development-only-gx-approval-secret-change-me"
            ):
                raise ValidationConfigurationError(
                    "Production requires a non-default GX_APPROVAL_SECRET "
                    "with at least 32 characters"
                )
        return cls(environment, revision, retention, redaction)

    def validate_mode(self, mode: ExecutionMode) -> None:
        if self.production and mode is not ExecutionMode.ENFORCE:
            raise ValidationConfigurationError(
                "Production validation must use execution_mode=enforce"
            )
