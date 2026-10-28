"""Authentication and authorization policy for the GX HTTP boundary."""

from __future__ import annotations

import hmac
import os
import re
from dataclasses import dataclass
from typing import Mapping

from .errors import ValidationConfigurationError


_CALLER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$")


class GXAuthenticationError(PermissionError):
    pass


class GXAuthorizationError(PermissionError):
    pass


@dataclass(frozen=True)
class HTTPBoundarySecurity:
    token: str
    allowed_callers: frozenset[str]
    max_request_bytes: int

    @classmethod
    def from_env(cls) -> "HTTPBoundarySecurity":
        token = os.getenv("GX_SERVICE_TOKEN", "").strip()
        if len(token) < 32:
            raise ValidationConfigurationError(
                "GX_SERVICE_TOKEN must contain at least 32 characters"
            )
        callers = frozenset(
            value.strip()
            for value in os.getenv("GX_ALLOWED_CALLERS", "airflow").split(",")
            if value.strip()
        )
        if not callers or any(not _CALLER.fullmatch(value) for value in callers):
            raise ValidationConfigurationError("GX_ALLOWED_CALLERS is invalid")
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
        return cls(token, callers, maximum)

    def authenticate(self, headers: Mapping[str, str]) -> str:
        authorization = str(headers.get("Authorization") or "")
        scheme, separator, supplied = authorization.partition(" ")
        if separator != " " or scheme.lower() != "bearer" or not hmac.compare_digest(
            supplied, self.token
        ):
            raise GXAuthenticationError("Missing or invalid GX service credential")
        caller = str(headers.get("X-GX-Caller") or "").strip()
        if not _CALLER.fullmatch(caller) or caller not in self.allowed_callers:
            raise GXAuthorizationError("GX caller is not authorized")
        return caller
