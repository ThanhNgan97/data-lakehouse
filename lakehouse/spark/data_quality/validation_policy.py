"""Policy separating GX results from pipeline blocking decisions."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ValidationPolicy:
    """Define which failed Expectation severities block publication."""

    blocking_severities: frozenset[str] = frozenset({"critical"})
    default_severity: str = "critical"

    def normalize_severity(self, severity: object | None) -> str:
        value = getattr(severity, "value", severity)
        normalized = str(value or self.default_severity).strip().lower()
        return normalized or self.default_severity

    def is_blocking(self, severity: object | None) -> bool:
        return self.normalize_severity(severity) in self.blocking_severities
