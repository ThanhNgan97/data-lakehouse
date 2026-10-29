"""Snapshot-aware GX gate for the registered API dataset pipeline."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from gx_airflow_adapter import execute_quality_gate
from gx_candidate_state import candidate_state_path, load_candidate_state
from gx_route_resolver import resolve_route
from gx_universal_gate import resolve_snapshot_identity
from gx_validation_request import GXTableValidationRequest


def _candidate_state_root() -> Path:
    return Path(
        os.getenv("GX_CANDIDATE_STATE_ROOT", "/opt/airflow/logs/gx-candidates")
    )


def validate_api_candidate_dataset(
    *, dataset_id: str, run_id: str
) -> dict[str, Any]:
    """Validate the immutable Silver candidate recorded by its writer."""
    route = resolve_route(
        dataset_id=dataset_id,
        data_layer="silver",
        source_kind="api",
        execution_engine="sql",
    )
    state_file = candidate_state_path(_candidate_state_root(), dataset_id, run_id)
    state = load_candidate_state(state_file)
    if (
        state.dataset_id != dataset_id
        or state.run_id != run_id
        or state.source_kind != "api"
    ):
        raise RuntimeError("GX candidate state does not match dataset/run/source")
    recorded_asset = state.asset(route.data_asset_ref)
    current = resolve_snapshot_identity(route.data_asset_ref, state.candidate_branch)
    if (
        current.snapshot_id != recorded_asset.snapshot_id
        or current.commit_hash != state.candidate_hash
    ):
        raise RuntimeError(
            "GX candidate state is stale; the candidate branch changed after writing"
        )
    result = execute_quality_gate(
        GXTableValidationRequest(
            dataset_id=dataset_id,
            data_layer="silver",
            source_kind="api",
            run_id=run_id,
            contract_version=route.contract_version,
            data_asset_ref=route.data_asset_ref,
            nessie_branch=state.candidate_branch,
            snapshot_id=recorded_asset.snapshot_id,
            nessie_commit_hash=state.candidate_hash,
            execution_mode=os.getenv("GX_API_GATE_MODE", "enforce"),
        ).as_payload(),
        service_url=os.getenv("GX_SERVICE_URL", "http://gx-validator:8090"),
        timeout_seconds=float(os.getenv("GX_API_GATE_TIMEOUT_SECONDS", "300")),
        snapshot_refreshes=0,
    )
    result.update(
        {
            "integration_status": "PASSED",
            "dataset_id": dataset_id,
            "data_asset_ref": route.data_asset_ref,
            "source_kind": "api",
            "candidate_state_path": str(state_file),
            "candidate_branch": state.candidate_branch,
            "candidate_hash": state.candidate_hash,
            "base_hash": state.base_hash,
        }
    )
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return result


def validate_api_dataset(*, dataset_id: str, run_id: str) -> dict[str, Any]:
    """Validate the API-produced Silver snapshot before Gold processing."""
    route = resolve_route(
        dataset_id=dataset_id,
        data_layer="silver",
        source_kind="api",
        execution_engine="sql",
    )

    branch = os.getenv("GX_API_NESSIE_BRANCH", "main")
    identity = resolve_snapshot_identity(route.data_asset_ref, branch)
    result = execute_quality_gate(
        GXTableValidationRequest(
            dataset_id=dataset_id,
            data_layer="silver",
            source_kind="api",
            run_id=run_id,
            contract_version=route.contract_version,
            data_asset_ref=route.data_asset_ref,
            nessie_branch=identity.branch,
            snapshot_id=identity.snapshot_id,
            nessie_commit_hash=identity.commit_hash,
            execution_mode=os.getenv("GX_API_GATE_MODE", "enforce"),
        ).as_payload(),
        service_url=os.getenv("GX_SERVICE_URL", "http://gx-validator:8090"),
        timeout_seconds=float(os.getenv("GX_API_GATE_TIMEOUT_SECONDS", "300")),
        snapshot_refreshes=1,
    )
    result.update({
        "integration_status": "PASSED",
        "dataset_id": dataset_id,
        "data_asset_ref": route.data_asset_ref,
        "source_kind": "api",
    })
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return result
