"""CAS publisher for GX-approved Nessie candidate branches."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Callable
from urllib import error, request
from urllib.parse import quote

from gx_candidate_state import CandidateState, candidate_state_path, load_candidate_state
from gx_route_resolver import resolve_route
from gx_universal_gate import resolve_snapshot_identity


class CandidatePublishError(RuntimeError):
    """Publication lacks approval, is stale, conflicts, or fails verification."""


def _state_root() -> Path:
    return Path(
        os.getenv("GX_CANDIDATE_STATE_ROOT", "/opt/airflow/logs/gx-candidates")
    )


def _all_assets_match(state: CandidateState, branch: str) -> tuple[bool, str]:
    hashes = set()
    for asset in state.assets:
        try:
            current = resolve_snapshot_identity(asset.data_asset_ref, branch)
        except Exception as exc:
            raise CandidatePublishError(
                f"Unable to resolve {asset.data_asset_ref!r} on {branch!r}"
            ) from exc
        hashes.add(current.commit_hash)
        if current.snapshot_id != asset.snapshot_id:
            return False, current.commit_hash
    if len(hashes) != 1:
        raise CandidatePublishError(f"Assets on {branch!r} did not resolve at one head")
    return True, next(iter(hashes))


def _require_approval(
    result: dict[str, Any], state: CandidateState, primary_asset: str
) -> dict[str, Any]:
    if result.get("merge_allowed") is not True:
        raise CandidatePublishError("GX result does not allow candidate publication")
    approval = result.get("approval")
    if not isinstance(approval, dict) or approval.get("merge_allowed") is not True:
        raise CandidatePublishError("GX approval receipt is missing or denied")
    if str(result.get("validation_id") or "") != str(
        approval.get("validation_id") or ""
    ):
        raise CandidatePublishError("GX result and approval validation_id differ")
    expected = {
        "data_asset_ref": primary_asset,
        "validated_branch": state.candidate_branch,
        "validated_snapshot_id": state.asset(primary_asset).snapshot_id,
        "validated_commit_hash": state.candidate_hash,
    }
    mismatches = [
        key for key, value in expected.items() if str(approval.get(key) or "") != value
    ]
    if mismatches:
        raise CandidatePublishError(
            "GX approval does not match candidate state: " + ", ".join(mismatches)
        )
    if not str(approval.get("validation_id") or "").strip():
        raise CandidatePublishError("GX approval lacks validation_id")
    return approval


def _merge_candidate(
    state: CandidateState,
    *,
    opener: Callable[..., Any] = request.urlopen,
) -> dict[str, Any]:
    configured = os.getenv(
        "NESSIE_API_URL", "http://nessie:19120/api/v1"
    ).rstrip("/")
    base = configured[:-7] + "/api/v2" if configured.endswith("/api/v1") else configured
    target = quote(f"{state.base_reference}@{state.base_hash}", safe="@")
    url = f"{base}/trees/{target}/history/merge"
    payload = {
        "fromRefName": state.candidate_branch,
        "fromHash": state.candidate_hash,
        "isReturnConflictAsResult": True,
    }
    call = request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json", "Accept": "application/json"},
        method="POST",
    )
    try:
        with opener(call, timeout=30) as response:
            value = json.load(response)
    except error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise CandidatePublishError(
            f"Nessie rejected candidate merge with HTTP {exc.code}: {detail[:500]}"
        ) from exc
    if value.get("wasApplied") is False or value.get("wasSuccessful") is False:
        raise CandidatePublishError("Nessie candidate merge reported a conflict")
    return value


def publish_api_candidate(
    *,
    dataset_id: str,
    run_id: str,
    gx_result: dict[str, Any],
    opener: Callable[..., Any] = request.urlopen,
) -> dict[str, Any]:
    """Publish exactly one approved API Silver candidate using optimistic CAS."""
    route = resolve_route(
        dataset_id=dataset_id,
        data_layer="silver",
        source_kind="api",
        execution_engine="sql",
    )
    state_file = candidate_state_path(_state_root(), dataset_id, run_id)
    state = load_candidate_state(state_file)
    if (state.dataset_id, state.run_id, state.source_kind) != (
        dataset_id, run_id, "api"
    ):
        raise CandidatePublishError("Candidate state does not match dataset/run/source")
    approval = _require_approval(gx_result, state, route.data_asset_ref)

    candidate_matches, candidate_hash = _all_assets_match(
        state, state.candidate_branch
    )
    if not candidate_matches or candidate_hash != state.candidate_hash:
        raise CandidatePublishError("Candidate changed after GX approval")

    main_matches, current_main_hash = _all_assets_match(state, state.base_reference)
    if main_matches:
        return {
            "status": "ALREADY_PUBLISHED",
            "dataset_id": dataset_id,
            "candidate_branch": state.candidate_branch,
            "candidate_hash": state.candidate_hash,
            "publication_hash": current_main_hash,
            "validation_id": approval["validation_id"],
            "candidate_state_path": str(state_file),
        }
    if current_main_hash != state.base_hash:
        raise CandidatePublishError(
            "Main changed after candidate creation; rebuild and validate a new candidate"
        )

    _merge_candidate(state, opener=opener)
    published, publication_hash = _all_assets_match(state, state.base_reference)
    if not published:
        raise CandidatePublishError("Post-merge verification did not match candidate assets")
    return {
        "status": "PUBLISHED",
        "dataset_id": dataset_id,
        "candidate_branch": state.candidate_branch,
        "candidate_hash": state.candidate_hash,
        "publication_hash": publication_hash,
        "validation_id": approval["validation_id"],
        "candidate_state_path": str(state_file),
    }
