"""Canonical, dependency-free HTTP contract for GX table validation."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Mapping


class GXValidationRequestError(ValueError):
    """A table-validation request is incomplete, unsafe, or inconsistent."""


_ASSET_PATTERN = re.compile(
    r"^lakehouse\.(silver|gold)\.[A-Za-z_][A-Za-z0-9_]*$"
)
_DATASET_PATTERN = re.compile(r"^[A-Za-z0-9_][A-Za-z0-9_.-]*$")
_LAYERS = frozenset({"silver", "gold", "reconciliation"})
_SOURCES = frozenset({"file", "api", "database", "streaming"})
_MODES = frozenset({"off", "report", "enforce"})
_COVERAGE = frozenset({"BASELINE", "CONTRACT"})


def _required_text(name: str, value: object) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        raise GXValidationRequestError(f"{name} must be a non-empty string")
    return normalized


@dataclass(frozen=True)
class GXTableValidationRequest:
    dataset_id: str
    data_layer: str
    source_kind: str
    run_id: str
    data_asset_ref: str
    nessie_branch: str
    snapshot_id: str
    nessie_commit_hash: str
    contract_version: str | None = None
    execution_engine: str = "sql"
    execution_mode: str = "enforce"
    coverage: str = "CONTRACT"

    def __post_init__(self) -> None:
        text_fields = (
            "dataset_id", "data_layer", "source_kind", "run_id",
            "data_asset_ref", "nessie_branch", "snapshot_id",
            "nessie_commit_hash", "execution_engine", "execution_mode",
            "coverage",
        )
        for name in text_fields:
            object.__setattr__(self, name, _required_text(name, getattr(self, name)))
        if self.contract_version is not None:
            object.__setattr__(
                self,
                "contract_version",
                _required_text("contract_version", self.contract_version),
            )
        object.__setattr__(self, "coverage", self.coverage.upper())
        object.__setattr__(self, "execution_engine", self.execution_engine.lower())
        object.__setattr__(self, "execution_mode", self.execution_mode.lower())
        object.__setattr__(self, "source_kind", self.source_kind.lower())
        object.__setattr__(self, "data_layer", self.data_layer.lower())

        if not _DATASET_PATTERN.fullmatch(self.dataset_id):
            raise GXValidationRequestError(f"Unsafe dataset_id {self.dataset_id!r}")
        if not _ASSET_PATTERN.fullmatch(self.data_asset_ref):
            raise GXValidationRequestError(
                f"Unsafe GX data_asset_ref {self.data_asset_ref!r}"
            )
        if self.data_layer not in _LAYERS:
            raise GXValidationRequestError(f"Unsupported data_layer {self.data_layer!r}")
        if self.source_kind not in _SOURCES:
            raise GXValidationRequestError(f"Unsupported source_kind {self.source_kind!r}")
        if self.execution_engine != "sql":
            raise GXValidationRequestError("Table validation requires execution_engine=sql")
        if self.execution_mode not in _MODES:
            raise GXValidationRequestError(
                f"Unsupported execution_mode {self.execution_mode!r}"
            )
        if self.coverage not in _COVERAGE:
            raise GXValidationRequestError(f"Unsupported coverage {self.coverage!r}")
        if self.coverage == "BASELINE":
            if self.data_layer != "silver":
                raise GXValidationRequestError("Baseline coverage supports Silver only")
            if self.contract_version != "baseline-1.0.0":
                raise GXValidationRequestError(
                    "Baseline coverage requires contract_version=baseline-1.0.0"
                )
        elif self.contract_version == "baseline-1.0.0":
            raise GXValidationRequestError(
                "Contract coverage cannot use the baseline contract version"
            )

    @classmethod
    def from_mapping(cls, payload: Mapping[str, Any]) -> "GXTableValidationRequest":
        if not isinstance(payload, Mapping):
            raise GXValidationRequestError("GX validation request must be an object")
        return cls(
            dataset_id=payload.get("dataset_id"),
            data_layer=payload.get("data_layer"),
            source_kind=payload.get("source_kind"),
            run_id=payload.get("run_id"),
            data_asset_ref=payload.get("data_asset_ref"),
            nessie_branch=payload.get("nessie_branch"),
            snapshot_id=payload.get("snapshot_id"),
            nessie_commit_hash=payload.get("nessie_commit_hash"),
            contract_version=payload.get("contract_version"),
            execution_engine=payload.get("execution_engine", "sql"),
            execution_mode=payload.get("execution_mode", "enforce"),
            coverage=payload.get("coverage", "CONTRACT"),
        )

    def as_payload(self) -> dict[str, Any]:
        return {
            "dataset_id": self.dataset_id,
            "data_layer": self.data_layer,
            "contract_version": self.contract_version,
            "source_kind": self.source_kind,
            "run_id": self.run_id,
            "data_asset_ref": self.data_asset_ref,
            "nessie_branch": self.nessie_branch,
            "snapshot_id": self.snapshot_id,
            "nessie_commit_hash": self.nessie_commit_hash,
            "execution_engine": self.execution_engine,
            "execution_mode": self.execution_mode,
            "coverage": self.coverage,
        }
