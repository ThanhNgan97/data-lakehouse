"""GX Gold and reconciliation gates for sealed dbt candidate bundles."""

from __future__ import annotations

import json
import os
from typing import Any

from api_dataset_registry import get_dataset_config
from dbt_gold_runtime import load as load_gold_state
from gx_airflow_adapter import execute_quality_gate
from gx_candidate_binding import dbt_gold_candidate_binding
from gx_route_resolver import resolve_route
from gx_universal_gate import resolve_snapshot_identity
from gx_validation_request import GXTableValidationRequest


def _validate_candidate(
    *, dataset_id: str, run_id: str, data_layer: str, source_kind: str
) -> dict[str, Any]:
    if data_layer not in {"gold", "reconciliation"}:
        raise ValueError("dbt candidate GX supports Gold or reconciliation only")
    config = get_dataset_config(dataset_id)
    state = load_gold_state(config, run_id)
    if state.get("phase") != "validated" or not state.get("validated_hash"):
        raise RuntimeError("dbt Gold candidate has not completed sealed validation")
    route = resolve_route(
        dataset_id=dataset_id,
        data_layer=data_layer,
        source_kind=source_kind,
        execution_engine="sql",
    )
    binding = dbt_gold_candidate_binding(dataset_id, run_id, route.data_asset_ref)
    if state.get("ref") != binding.candidate_branch:
        raise RuntimeError("dbt Gold state branch does not match authorized binding")
    canonical_name = route.data_asset_ref.rsplit(".", 1)[-1]
    entry = (state.get("outputs") or {}).get(canonical_name)
    candidate = (state.get("candidate_contents") or {}).get(canonical_name)
    if (
        not isinstance(entry, dict)
        or entry.get("alias") != binding.physical_asset_ref.rsplit(".", 1)[-1]
        or not isinstance(candidate, dict)
        or candidate.get("snapshotId") is None
    ):
        raise RuntimeError("dbt Gold candidate state lacks the canonical output binding")
    identity = resolve_snapshot_identity(
        binding.physical_asset_ref, binding.candidate_branch
    )
    if (
        identity.snapshot_id != str(candidate["snapshotId"])
        or identity.commit_hash != str(state["validated_hash"])
    ):
        raise RuntimeError("dbt Gold candidate changed after sealed validation")
    result = execute_quality_gate(
        GXTableValidationRequest(
            dataset_id=dataset_id,
            data_layer=data_layer,
            source_kind=source_kind,
            run_id=run_id,
            contract_version=route.contract_version,
            data_asset_ref=route.data_asset_ref,
            physical_data_asset_ref=binding.physical_asset_ref,
            nessie_branch=binding.candidate_branch,
            snapshot_id=identity.snapshot_id,
            nessie_commit_hash=identity.commit_hash,
            execution_mode=os.getenv("GX_DBT_CANDIDATE_GATE_MODE", "enforce"),
        ).as_payload(),
        service_url=os.getenv("GX_SERVICE_URL", "http://gx-validator:8090"),
        timeout_seconds=float(
            os.getenv("GX_DBT_CANDIDATE_GATE_TIMEOUT_SECONDS", "300")
        ),
        snapshot_refreshes=0,
    )
    result.update({
        "integration_status": "PASSED",
        "dataset_id": dataset_id,
        "data_layer": data_layer,
        "data_asset_ref": route.data_asset_ref,
        "physical_data_asset_ref": binding.physical_asset_ref,
        "candidate_branch": binding.candidate_branch,
        "candidate_hash": identity.commit_hash,
        "source_kind": source_kind,
    })
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return result


def validate_dbt_gold_candidate(
    *, dataset_id: str, run_id: str, source_kind: str = "api"
) -> dict[str, Any]:
    return _validate_candidate(
        dataset_id=dataset_id, run_id=run_id, data_layer="gold", source_kind=source_kind
    )


def reconcile_dbt_gold_candidate(
    *, dataset_id: str, run_id: str, source_kind: str = "api"
) -> dict[str, Any]:
    return _validate_candidate(
        dataset_id=dataset_id,
        run_id=run_id,
        data_layer="reconciliation",
        source_kind=source_kind,
    )
