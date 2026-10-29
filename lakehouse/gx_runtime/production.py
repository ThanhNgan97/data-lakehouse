"""Production-only guardrails configured without process-local state."""

from __future__ import annotations

import os
import base64
import json
from dataclasses import dataclass

from .errors import ValidationConfigurationError
from .models import ExecutionMode
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat


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
            signing_key_id = os.getenv("GX_APPROVAL_SIGNING_KEY_ID", "").strip()
            private_key = os.getenv("GX_APPROVAL_PRIVATE_KEY", "").strip()
            public_keys = os.getenv("GX_APPROVAL_PUBLIC_KEYS", "").strip()
            if (
                not signing_key_id
                or signing_key_id == "development"
                or not private_key
                or not public_keys
            ):
                raise ValidationConfigurationError(
                    "Production requires non-development Ed25519 approval keys"
                )
            try:
                private = Ed25519PrivateKey.from_private_bytes(
                    base64.b64decode(private_key, validate=True)
                )
                keyring = json.loads(public_keys)
                configured_public = base64.b64decode(
                    str(keyring[signing_key_id]), validate=True
                )
                derived_public = private.public_key().public_bytes(
                    Encoding.Raw, PublicFormat.Raw
                )
            except Exception as exc:
                raise ValidationConfigurationError(
                    "Production Ed25519 approval key material is invalid"
                ) from exc
            if configured_public != derived_public:
                raise ValidationConfigurationError(
                    "Production approval private/public keys do not match"
                )
            revoked = {
                item.strip()
                for item in os.getenv("GX_APPROVAL_REVOKED_KEY_IDS", "").split(",")
                if item.strip()
            }
            if signing_key_id in revoked:
                raise ValidationConfigurationError(
                    "Production approval signing key is revoked"
                )
            if os.getenv("GX_SERVICE_TOKEN", "").strip():
                raise ValidationConfigurationError(
                    "GX_SERVICE_TOKEN is not allowed in production; configure "
                    "per-caller GX_CALLER_CREDENTIALS"
                )
            from .http_security import HTTPBoundarySecurity

            HTTPBoundarySecurity.from_env()
            if _boolean("GX_ALLOW_PILOT_CONTRACTS", True):
                raise ValidationConfigurationError(
                    "GX_ALLOW_PILOT_CONTRACTS must be false in production"
                )
        return cls(environment, revision, retention, redaction)

    def validate_mode(self, mode: ExecutionMode) -> None:
        if self.production and mode is not ExecutionMode.ENFORCE:
            raise ValidationConfigurationError(
                "Production validation must use execution_mode=enforce"
            )
