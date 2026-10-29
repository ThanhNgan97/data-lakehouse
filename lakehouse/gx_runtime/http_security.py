"""Authentication and authorization policy for the GX HTTP boundary."""

from __future__ import annotations

import hmac
import json
import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Mapping

from .errors import ValidationConfigurationError


_CALLER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$")


class GXAuthenticationError(PermissionError):
    pass


class GXAuthorizationError(PermissionError):
    pass


@dataclass(frozen=True)
class CallerCredential:
    token: str
    expires_at: datetime | None

    @property
    def active(self) -> bool:
        return self.expires_at is None or self.expires_at > datetime.now(timezone.utc)


@dataclass(frozen=True)
class HTTPBoundarySecurity:
    caller_tokens: Mapping[str, tuple[CallerCredential, ...]]
    allowed_callers: frozenset[str]
    max_request_bytes: int
    governance_callers: frozenset[str]

    @classmethod
    def from_env(cls) -> "HTTPBoundarySecurity":
        raw_credentials = os.getenv("GX_CALLER_CREDENTIALS", "").strip()
        if not raw_credentials:
            raise ValidationConfigurationError("GX_CALLER_CREDENTIALS is required")
        try:
            configured = json.loads(raw_credentials)
        except (TypeError, ValueError) as exc:
            raise ValidationConfigurationError(
                "GX_CALLER_CREDENTIALS must be a JSON object"
            ) from exc
        if not isinstance(configured, dict) or not configured:
            raise ValidationConfigurationError(
                "GX_CALLER_CREDENTIALS must be a non-empty JSON object"
            )
        credentials: dict[str, tuple[CallerCredential, ...]] = {}
        token_owners: dict[str, str] = {}
        for caller, raw_tokens in configured.items():
            if not isinstance(caller, str) or not _CALLER.fullmatch(caller):
                raise ValidationConfigurationError(
                    "GX_CALLER_CREDENTIALS contains an invalid caller"
                )
            values = [raw_tokens] if isinstance(raw_tokens, (str, dict)) else raw_tokens
            if not isinstance(values, list) or not values:
                raise ValidationConfigurationError(
                    f"GX caller {caller!r} must have at least one credential"
                )
            tokens: list[CallerCredential] = []
            for value in values:
                expires_at = None
                if isinstance(value, dict):
                    token = str(value.get("token") or "").strip()
                    raw_expiry = str(value.get("expires_at") or "").strip()
                    if raw_expiry:
                        try:
                            expires_at = datetime.fromisoformat(
                                raw_expiry.replace("Z", "+00:00")
                            )
                        except ValueError as exc:
                            raise ValidationConfigurationError(
                                f"GX caller {caller!r} credential expiry is invalid"
                            ) from exc
                        if expires_at.tzinfo is None:
                            raise ValidationConfigurationError(
                                f"GX caller {caller!r} credential expiry must include a timezone"
                            )
                        expires_at = expires_at.astimezone(timezone.utc)
                else:
                    token = value.strip() if isinstance(value, str) else ""
                if len(token) < 32:
                    raise ValidationConfigurationError(
                        f"GX caller {caller!r} credentials must contain at least 32 characters"
                    )
                owner = token_owners.get(token)
                if owner is not None and owner != caller:
                    raise ValidationConfigurationError(
                        "GX caller credentials must be unique per caller"
                    )
                token_owners[token] = caller
                tokens.append(CallerCredential(token, expires_at))
            credentials[caller] = tuple(tokens)
        if os.getenv("GX_ENVIRONMENT", "development").strip().lower() == "production":
            if any(
                credential.expires_at is None
                for values in credentials.values()
                for credential in values
            ):
                raise ValidationConfigurationError(
                    "Production GX caller credentials require expires_at"
                )
            if any(not any(credential.active for credential in values) for values in credentials.values()):
                raise ValidationConfigurationError(
                    "Every production GX caller requires an active credential"
                )
        callers = frozenset(credentials)
        try:
            maximum = int(os.getenv("GX_MAX_REQUEST_BYTES", "65536"))
        except ValueError as exc:
            raise ValidationConfigurationError(
                "GX_MAX_REQUEST_BYTES must be an integer"
            ) from exc
        if maximum < 1024 or maximum > 1048576:
            raise ValidationConfigurationError(
                "GX_MAX_REQUEST_BYTES must be between 1024 and 1048576"
            )
        governance = frozenset(
            value.strip()
            for value in os.getenv("GX_GOVERNANCE_CALLERS", "gx-admin").split(",")
            if value.strip()
        )
        if not governance or not governance <= callers:
            raise ValidationConfigurationError(
                "GX_GOVERNANCE_CALLERS must be a non-empty subset of configured callers"
            )
        return cls(credentials, callers, maximum, governance)

    def authenticate(self, headers: Mapping[str, str]) -> str:
        authorization = str(headers.get("Authorization") or "")
        scheme, separator, supplied = authorization.partition(" ")
        if separator != " " or scheme.lower() != "bearer" or not supplied:
            raise GXAuthenticationError("Missing or invalid GX service credential")
        authenticated_caller = None
        for caller, credentials in self.caller_tokens.items():
            if any(
                hmac.compare_digest(supplied, credential.token) and credential.active
                for credential in credentials
            ):
                authenticated_caller = caller
        if authenticated_caller is None:
            raise GXAuthenticationError("Missing or invalid GX service credential")
        claimed_caller = str(headers.get("X-GX-Caller") or "").strip()
        if claimed_caller and claimed_caller != authenticated_caller:
            raise GXAuthorizationError(
                "GX caller header does not match the authenticated identity"
            )
        return authenticated_caller

    def authorize_governance(self, caller: str) -> None:
        if caller not in self.governance_callers:
            raise GXAuthorizationError("GX caller cannot manage governance resources")
