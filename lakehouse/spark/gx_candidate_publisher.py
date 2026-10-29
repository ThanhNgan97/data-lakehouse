"""CAS publisher for GX-approved Nessie candidate branches."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Callable
from urllib import error, request
from urllib.parse import quote

from gx_candidate_state import CandidateState, candidate_state_path, load_candidate_state
from gx_approval_receipt import GXApprovalReceiptError, verify_approval_receipt
from gx_route_resolver import resolve_route
from gx_universal_gate import resolve_snapshot_identity
from gx_receipt_audit import ReceiptAuditError, record_receipt_consumption


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
    result: dict[str, Any], state: CandidateState, primary_asset: str,
    contract_version: str,
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
        "validation_id": result.get("validation_id"),
        "dataset_id": state.dataset_id,
        "data_layer": "silver",
        "contract_version": contract_version,
        "run_id": state.run_id,
        "source_kind": state.source_kind,
        "audience": "silver-publisher",
        "data_asset_ref": primary_asset,
        "validated_branch": state.candidate_branch,
        "validated_snapshot_id": state.asset(primary_asset).snapshot_id,
        "validated_commit_hash": state.candidate_hash,
        "candidate_hash": state.candidate_hash,
    }
    try:
        return verify_approval_receipt(approval, expected=expected)
    except GXApprovalReceiptError as exc:
        raise CandidatePublishError(str(exc)) from exc


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


def _delete_candidate_branch(
    state: CandidateState,
    *,
    opener: Callable[..., Any] = request.urlopen,
) -> None:
    if os.getenv("GX_DELETE_PUBLISHED_CANDIDATE_BRANCH", "false").lower() != "true":
        return
    configured = os.getenv("NESSIE_API_URL", "http://nessie:19120/api/v1").rstrip("/")
    base = configured[:-7] + "/api/v2" if configured.endswith("/api/v1") else configured
    branch = quote(state.candidate_branch, safe="")
    commit_hash = quote(state.candidate_hash, safe="")
    call = request.Request(
        f"{base}/trees/branch/{branch}/{commit_hash}",
        headers={"Accept": "application/json"},
        method="DELETE",
    )
    try:
        with opener(call, timeout=30):
            return
    except error.HTTPError as exc:
        if exc.code != 404:
            raise CandidatePublishError(
                f"Published candidate branch cleanup failed with HTTP {exc.code}"
            ) from exc


def publish_candidate(
    *,
    dataset_id: str,
    run_id: str,
    source_kind: str,
    gx_result: dict[str, Any],
    opener: Callable[..., Any] = request.urlopen,
) -> dict[str, Any]:
    """Publish one approved Silver candidate using optimistic CAS."""
    route = resolve_route(
        dataset_id=dataset_id,
        data_layer="silver",
        source_kind=source_kind,
        execution_engine="sql",
    )
    state_file = candidate_state_path(_state_root(), dataset_id, run_id)
    state = load_candidate_state(state_file)
    if (state.dataset_id, state.run_id, state.source_kind) != (
        dataset_id, run_id, source_kind
    ):
        raise CandidatePublishError("Candidate state does not match dataset/run/source")
    approval = _require_approval(
        gx_result, state, route.data_asset_ref, route.contract_version
    )

    main_matches, current_main_hash = _all_assets_match(state, state.base_reference)
    if main_matches:
        try:
            audit_uri = record_receipt_consumption(
                approval,
                publication_hash=current_main_hash,
                status="ALREADY_PUBLISHED",
            )
        except ReceiptAuditError as exc:
            raise CandidatePublishError(str(exc)) from exc
        _delete_candidate_branch(state, opener=opener)
        return {
            "status": "ALREADY_PUBLISHED",
            "dataset_id": dataset_id,
            "candidate_branch": state.candidate_branch,
            "candidate_hash": state.candidate_hash,
            "publication_hash": current_main_hash,
            "validation_id": approval["validation_id"],
            "candidate_state_path": str(state_file),
            "receipt_audit_uri": audit_uri,
        }
    if current_main_hash != state.base_hash:
        raise CandidatePublishError(
            "Main changed after candidate creation; rebuild and validate a new candidate"
        )

    candidate_matches, candidate_hash = _all_assets_match(
        state, state.candidate_branch
    )
    if not candidate_matches or candidate_hash != state.candidate_hash:
        raise CandidatePublishError("Candidate changed after GX approval")

    _merge_candidate(state, opener=opener)
    published, publication_hash = _all_assets_match(state, state.base_reference)
    if not published:
        raise CandidatePublishError("Post-merge verification did not match candidate assets")
    try:
        audit_uri = record_receipt_consumption(
            approval, publication_hash=publication_hash, status="PUBLISHED"
        )
    except ReceiptAuditError as exc:
        raise CandidatePublishError(str(exc)) from exc
    _delete_candidate_branch(state, opener=opener)
    return {
        "status": "PUBLISHED",
        "dataset_id": dataset_id,
        "candidate_branch": state.candidate_branch,
        "candidate_hash": state.candidate_hash,
        "publication_hash": publication_hash,
        "validation_id": approval["validation_id"],
        "candidate_state_path": str(state_file),
        "receipt_audit_uri": audit_uri,
    }


def publish_api_candidate(
    *, dataset_id: str, run_id: str, gx_result: dict[str, Any], opener=request.urlopen
) -> dict[str, Any]:
    return publish_candidate(
        dataset_id=dataset_id,
        run_id=run_id,
        source_kind="api",
        gx_result=gx_result,
        opener=opener,
    )


def publish_file_candidate(
    *, dataset_id: str, run_id: str, gx_result: dict[str, Any], opener=request.urlopen
) -> dict[str, Any]:
    return publish_candidate(
        dataset_id=dataset_id,
        run_id=run_id,
        source_kind="file",
        gx_result=gx_result,
        opener=opener,
    )
