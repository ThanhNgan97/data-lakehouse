"""Fail-closed publish authorization bound to a Nessie/Iceberg identity."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from typing import Any, Callable, Protocol
from urllib.parse import quote, urlencode
from urllib.request import urlopen

from .errors import QualityGateRejectedError, SnapshotMismatchError
from .models import ValidationSummary


@dataclass(frozen=True)
class SnapshotIdentity:
    data_asset_ref: str
    nessie_branch: str
    snapshot_id: str
    nessie_commit_hash: str

    def __post_init__(self) -> None:
        values = asdict(self)
        missing = [name for name, value in values.items() if not str(value).strip()]
        if missing:
            raise ValueError(
                "Snapshot identity fields must be non-empty: " + ", ".join(missing)
            )


class SnapshotResolver(Protocol):
    def resolve(self, data_asset_ref: str, branch: str) -> SnapshotIdentity: ...


@dataclass(frozen=True)
class QualityGateApproval:
    validation_id: str
    data_asset_ref: str
    validated_branch: str
    validated_snapshot_id: str
    validated_commit_hash: str
    merge_allowed: bool = True

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


class SnapshotAwareQualityGate:
    """Authorize publish only for the exact identity validated by GX."""

    @staticmethod
    def verify(
        summary: ValidationSummary,
        current: SnapshotIdentity,
    ) -> QualityGateApproval:
        if not summary.executed:
            raise QualityGateRejectedError(
                "Validation was not executed; publish requires a fresh GX result"
            )
        if not summary.merge_allowed:
            raise QualityGateRejectedError(
                f"Validation {summary.validation_id} does not allow publish"
            )
        validated = {
            "data_asset_ref": summary.data_asset_ref,
            "nessie_branch": summary.nessie_branch,
            "snapshot_id": summary.snapshot_id,
            "nessie_commit_hash": summary.nessie_commit_hash,
        }
        absent = [name for name, value in validated.items() if not value]
        if absent:
            raise QualityGateRejectedError(
                "Validation summary lacks snapshot identity: " + ", ".join(absent)
            )
        actual = asdict(current)
        mismatches = {
            name: {"validated": str(value), "current": str(actual[name])}
            for name, value in validated.items()
            if str(value) != str(actual[name])
        }
        if mismatches:
            fields = ", ".join(sorted(mismatches))
            raise SnapshotMismatchError(
                f"SNAPSHOT_MISMATCH ({fields}); run GX again before publish"
            )
        return QualityGateApproval(
            validation_id=summary.validation_id,
            data_asset_ref=current.data_asset_ref,
            validated_branch=current.nessie_branch,
            validated_snapshot_id=current.snapshot_id,
            validated_commit_hash=current.nessie_commit_hash,
        )

    def verify_current(
        self,
        summary: ValidationSummary,
        resolver: SnapshotResolver,
    ) -> QualityGateApproval:
        if not summary.nessie_branch:
            raise QualityGateRejectedError(
                "Validation summary lacks the branch required for publish"
            )
        current = resolver.resolve(summary.data_asset_ref, summary.nessie_branch)
        return self.verify(summary, current)


class NessieSnapshotResolver:
    """Resolve an Iceberg content pointer at one immutable Nessie commit."""

    def __init__(
        self,
        nessie_api_url: str,
        *,
        timeout_seconds: float = 15,
        opener: Callable[..., Any] = urlopen,
    ) -> None:
        self.nessie_api_url = nessie_api_url.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self.opener = opener

    def _json(self, url: str) -> dict[str, Any]:
        with self.opener(url, timeout=self.timeout_seconds) as response:
            return json.load(response)

    @staticmethod
    def _content_key(data_asset_ref: str) -> str:
        parts = data_asset_ref.split(".")
        if len(parts) != 3 or not all(parts):
            raise QualityGateRejectedError(
                "data_asset_ref must be catalog.schema.table for Nessie resolution"
            )
        return ".".join(parts[1:])

    def resolve(self, data_asset_ref: str, branch: str) -> SnapshotIdentity:
        encoded_branch = quote(branch, safe="")
        reference = self._json(
            f"{self.nessie_api_url}/trees/tree/{encoded_branch}"
        )
        if reference.get("type") != "BRANCH" or reference.get("name") != branch:
            raise QualityGateRejectedError(
                f"Nessie did not resolve requested branch {branch!r}"
            )
        commit_hash = reference.get("hash")
        if not commit_hash:
            raise QualityGateRejectedError("Nessie branch response lacks commit hash")
        key = quote(self._content_key(data_asset_ref), safe=".")
        query = urlencode({"ref": branch, "hashOnRef": commit_hash})
        content = self._json(
            f"{self.nessie_api_url}/contents/{key}?{query}"
        )
        if content.get("type") != "ICEBERG_TABLE" or content.get("snapshotId") is None:
            raise QualityGateRejectedError(
                f"Nessie content {key!r} is not an Iceberg table with a snapshot"
            )
        return SnapshotIdentity(
            data_asset_ref=data_asset_ref,
            nessie_branch=branch,
            snapshot_id=str(content["snapshotId"]),
            nessie_commit_hash=str(commit_hash),
        )
