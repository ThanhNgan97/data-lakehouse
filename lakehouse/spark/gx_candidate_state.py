"""Durable candidate-state contract shared by writers, GX, and publishers."""

from __future__ import annotations

import hashlib
import json
import os
import re
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence


SCHEMA_VERSION = 1
_REF_PATTERN = re.compile(r"^[A-Za-z0-9_][A-Za-z0-9_.-]*$")
_ASSET_PATTERN = re.compile(
    r"^lakehouse\.(silver|gold)\.[A-Za-z_][A-Za-z0-9_]*$"
)
_SOURCES = frozenset({"file", "api", "database", "streaming"})


class CandidateStateError(ValueError):
    """Candidate state is missing, unsafe, stale, or internally inconsistent."""


def _text(name: str, value: object) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        raise CandidateStateError(f"{name} must be a non-empty string")
    return normalized


@dataclass(frozen=True)
class CandidateAssetIdentity:
    data_asset_ref: str
    snapshot_id: str

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "data_asset_ref", _text("data_asset_ref", self.data_asset_ref)
        )
        object.__setattr__(self, "snapshot_id", _text("snapshot_id", self.snapshot_id))
        if not _ASSET_PATTERN.fullmatch(self.data_asset_ref):
            raise CandidateStateError(
                f"Unsafe candidate data_asset_ref {self.data_asset_ref!r}"
            )

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "CandidateAssetIdentity":
        if not isinstance(value, Mapping):
            raise CandidateStateError("candidate asset identity must be an object")
        return cls(
            data_asset_ref=value.get("data_asset_ref"),
            snapshot_id=value.get("snapshot_id"),
        )


@dataclass(frozen=True)
class CandidateState:
    dataset_id: str
    source_kind: str
    run_id: str
    candidate_branch: str
    base_reference: str
    base_hash: str
    candidate_hash: str
    assets: tuple[CandidateAssetIdentity, ...]
    schema_version: int = SCHEMA_VERSION

    def __post_init__(self) -> None:
        for name in (
            "dataset_id", "source_kind", "run_id", "candidate_branch",
            "base_reference", "base_hash", "candidate_hash",
        ):
            object.__setattr__(self, name, _text(name, getattr(self, name)))
        object.__setattr__(self, "source_kind", self.source_kind.lower())
        object.__setattr__(self, "assets", tuple(self.assets))
        if self.schema_version != SCHEMA_VERSION:
            raise CandidateStateError(
                f"Unsupported candidate-state schema_version {self.schema_version!r}"
            )
        if self.source_kind not in _SOURCES:
            raise CandidateStateError(f"Unsupported source_kind {self.source_kind!r}")
        for name in ("dataset_id", "candidate_branch", "base_reference"):
            if not _REF_PATTERN.fullmatch(getattr(self, name)):
                raise CandidateStateError(f"Unsafe {name} {getattr(self, name)!r}")
        if self.candidate_branch == self.base_reference:
            raise CandidateStateError("candidate_branch must differ from base_reference")
        if self.candidate_branch == "main":
            raise CandidateStateError("candidate state cannot target the main branch")
        if not self.assets:
            raise CandidateStateError("candidate state must contain at least one asset")
        refs = [asset.data_asset_ref for asset in self.assets]
        if len(refs) != len(set(refs)):
            raise CandidateStateError("candidate state contains duplicate assets")

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "CandidateState":
        if not isinstance(value, Mapping):
            raise CandidateStateError("candidate state must be an object")
        assets = value.get("assets")
        if not isinstance(assets, Sequence) or isinstance(assets, (str, bytes)):
            raise CandidateStateError("candidate state assets must be an array")
        return cls(
            schema_version=value.get("schema_version", SCHEMA_VERSION),
            dataset_id=value.get("dataset_id"),
            source_kind=value.get("source_kind"),
            run_id=value.get("run_id"),
            candidate_branch=value.get("candidate_branch"),
            base_reference=value.get("base_reference"),
            base_hash=value.get("base_hash"),
            candidate_hash=value.get("candidate_hash"),
            assets=tuple(CandidateAssetIdentity.from_mapping(item) for item in assets),
        )

    def as_payload(self) -> dict[str, Any]:
        return asdict(self)

    def asset(self, data_asset_ref: str) -> CandidateAssetIdentity:
        for identity in self.assets:
            if identity.data_asset_ref == data_asset_ref:
                return identity
        raise CandidateStateError(
            f"Candidate state does not contain asset {data_asset_ref!r}"
        )


def retry_stable_branch_name(prefix: str, dataset_id: str, run_id: str) -> str:
    """Return the same safe candidate branch for every retry of one run."""
    safe_prefix = re.sub(r"[^A-Za-z0-9_.-]+", "_", _text("prefix", prefix)).strip("_.-")
    if not safe_prefix:
        raise CandidateStateError("prefix has no safe branch-name characters")
    identity = f"{_text('dataset_id', dataset_id)}\0{_text('run_id', run_id)}"
    suffix = hashlib.sha256(identity.encode("utf-8")).hexdigest()[:16]
    return f"{safe_prefix[:80]}_{suffix}"


def candidate_state_path(root: str | Path, dataset_id: str, run_id: str) -> Path:
    """Build a bounded path without embedding caller-controlled identifiers."""
    digest = hashlib.sha256(
        f"{_text('dataset_id', dataset_id)}\0{_text('run_id', run_id)}".encode("utf-8")
    ).hexdigest()
    return Path(root).resolve() / digest[:2] / f"{digest}.json"


def save_candidate_state(state: CandidateState, path: str | Path) -> Path:
    """Atomically persist immutable state; retries may only replay it exactly."""
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists():
        existing = load_candidate_state(destination)
        if existing != state:
            raise CandidateStateError(
                "Candidate state already exists with a different identity"
            )
        return destination
    temporary = destination.with_name(f".{destination.name}.{os.getpid()}.tmp")
    try:
        temporary.write_text(
            json.dumps(state.as_payload(), indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        os.replace(temporary, destination)
    finally:
        temporary.unlink(missing_ok=True)
    return destination


def load_candidate_state(path: str | Path) -> CandidateState:
    try:
        value = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise CandidateStateError(f"Unable to load candidate state {path}") from exc
    return CandidateState.from_mapping(value)
