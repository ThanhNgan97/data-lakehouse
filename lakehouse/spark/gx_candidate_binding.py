"""Deterministic authorization binding for dbt Gold candidate aliases."""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass


_ASSET = re.compile(r"^lakehouse\.gold\.([A-Za-z_][A-Za-z0-9_]*)$")


class CandidateBindingError(ValueError):
    pass


@dataclass(frozen=True)
class DbtGoldCandidateBinding:
    candidate_branch: str
    canonical_asset_ref: str
    physical_asset_ref: str


def dbt_gold_candidate_binding(
    dataset_id: str, run_id: str, canonical_asset_ref: str
) -> DbtGoldCandidateBinding:
    dataset = str(dataset_id or "").strip()
    run = str(run_id or "").strip()
    match = _ASSET.fullmatch(str(canonical_asset_ref or "").strip())
    if not dataset or not run or not match:
        raise CandidateBindingError("Invalid dbt Gold candidate binding identity")
    digest = hashlib.sha256(f"{dataset}\n{run}".encode("utf-8")).hexdigest()[:24]
    return DbtGoldCandidateBinding(
        candidate_branch=f"dbt_gold_{digest}",
        canonical_asset_ref=canonical_asset_ref,
        physical_asset_ref=f"lakehouse.gold.{match.group(1)}_{digest}",
    )
