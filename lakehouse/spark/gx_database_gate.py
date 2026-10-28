"""GX validation boundary for Airbyte/database relational runs."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from gx_airflow_adapter import execute_quality_gate
from gx_route_resolver import GXRouteNotFoundError, resolve_route
from gx_universal_gate import SnapshotIdentity, resolve_snapshot_identity


def build_database_validation_request(
    *, dataset_id: str, run_id: str, contract_version: str,
    identity: SnapshotIdentity,
) -> dict[str, Any]:
    return {
        "dataset_id": dataset_id,
        "data_layer": "silver",
        "contract_version": contract_version,
        "source_kind": "database",
        "run_id": run_id,
        "nessie_branch": identity.branch,
        "snapshot_id": identity.snapshot_id,
        "nessie_commit_hash": identity.commit_hash,
        "execution_engine": "sql",
        "execution_mode": os.getenv("GX_DATABASE_GATE_MODE", "enforce"),
    }


def _not_configured(
    *, dataset_id: str | None, data_asset_ref: str | None, reason: str,
) -> dict[str, Any]:
    result = {
        "status": "NOT_CONFIGURED",
        "reason": reason,
        "dataset_id": dataset_id,
        "data_asset_ref": data_asset_ref,
        "source_kind": "database",
        "merge_allowed": None,
    }
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return result


def validate_database_context(**context) -> dict[str, Any]:
    """Validate a registered database asset, or report uncovered context."""
    ti = context["ti"]
    dataset_id = ti.xcom_pull(
        task_ids="ai_semantic_profiler", key="registered_dataset_id"
    )
    decision_file = ti.xcom_pull(
        task_ids="ai_semantic_profiler", key="decision_file"
    )
    decision: dict[str, Any] = {}
    if decision_file and Path(str(decision_file)).is_file():
        with Path(str(decision_file)).open(encoding="utf-8") as handle:
            decision = json.load(handle)
    data_asset_ref = decision.get("target_silver_table")

    try:
        route = resolve_route(
            dataset_id=str(dataset_id or ""),
            data_layer="silver",
            source_kind="database",
            execution_engine="sql",
        )
    except GXRouteNotFoundError:
        return _not_configured(
            dataset_id=str(dataset_id) if dataset_id else None,
            data_asset_ref=data_asset_ref,
            reason="Airbyte relational context has no effective GX validation route",
        )
    if data_asset_ref != route.data_asset_ref:
        return _not_configured(
            dataset_id=str(dataset_id),
            data_asset_ref=data_asset_ref,
            reason=(
                "Airbyte output table does not match the registered GX data asset; "
                "explicit canonical mapping is required"
            ),
        )
    branch = os.getenv("GX_DATABASE_NESSIE_BRANCH", "main")
    identity = resolve_snapshot_identity(data_asset_ref, branch)
    result = execute_quality_gate(
        build_database_validation_request(
            dataset_id=str(dataset_id),
            run_id=str(context.get("run_id") or ""),
            contract_version=route.contract_version,
            identity=identity,
        ),
        service_url=os.getenv("GX_SERVICE_URL", "http://gx-validator:8090"),
        timeout_seconds=float(os.getenv("GX_DATABASE_GATE_TIMEOUT_SECONDS", "300")),
        snapshot_refreshes=1,
    )
    result.update({
        "integration_status": "PASSED",
        "dataset_id": str(dataset_id),
        "data_asset_ref": data_asset_ref,
        "source_kind": "database",
    })
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return result
