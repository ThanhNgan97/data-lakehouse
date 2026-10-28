"""Fail-closed GX gate for registered file-upload Silver candidates."""

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


def _state_root() -> Path:
    return Path(os.getenv("GX_CANDIDATE_STATE_ROOT", "/opt/airflow/logs/gx-candidates"))


def validate_file_candidate(*, dataset_id: str, run_id: str) -> dict[str, Any]:
    route = resolve_route(
        dataset_id=dataset_id,
        data_layer="silver",
        source_kind="file",
        execution_engine="sql",
    )
    state_file = candidate_state_path(_state_root(), dataset_id, run_id)
    state = load_candidate_state(state_file)
    if (state.dataset_id, state.run_id, state.source_kind) != (
        dataset_id,
        run_id,
        "file",
    ):
        raise RuntimeError("File candidate state does not match dataset/run/source")
    recorded = state.asset(route.data_asset_ref)
    current = resolve_snapshot_identity(route.data_asset_ref, state.candidate_branch)
    if current.snapshot_id != recorded.snapshot_id or current.commit_hash != state.candidate_hash:
        raise RuntimeError("File candidate changed after it was sealed")
    result = execute_quality_gate(
        GXTableValidationRequest(
            dataset_id=dataset_id,
            data_layer="silver",
            source_kind="file",
            run_id=run_id,
            contract_version=route.contract_version,
            data_asset_ref=route.data_asset_ref,
            nessie_branch=state.candidate_branch,
            snapshot_id=recorded.snapshot_id,
            nessie_commit_hash=state.candidate_hash,
            execution_mode=os.getenv("GX_FILE_GATE_MODE", "enforce"),
        ).as_payload(),
        service_url=os.getenv("GX_SERVICE_URL", "http://gx-validator:8090"),
        timeout_seconds=float(os.getenv("GX_FILE_GATE_TIMEOUT_SECONDS", "300")),
        snapshot_refreshes=0,
    )
    result.update({
        "integration_status": "PASSED",
        "dataset_id": dataset_id,
        "data_asset_ref": route.data_asset_ref,
        "source_kind": "file",
        "candidate_branch": state.candidate_branch,
        "candidate_hash": state.candidate_hash,
        "candidate_state_path": str(state_file),
    })
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return result
