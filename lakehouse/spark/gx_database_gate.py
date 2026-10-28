"""GX validation boundary for Airbyte/database relational runs."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from gx_airflow_adapter import execute_quality_gate
from gx_route_resolver import resolve_route_or_baseline
from gx_universal_gate import resolve_snapshot_identity
from gx_validation_request import GXTableValidationRequest


def validate_database_context(**context) -> dict[str, Any]:
    """Validate database output with contract or baseline GX coverage."""
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

    route = resolve_route_or_baseline(
        dataset_id=str(dataset_id) if dataset_id else None,
        data_asset_ref=str(data_asset_ref) if data_asset_ref else None,
        source_kind="database",
    )
    branch = os.getenv("GX_DATABASE_NESSIE_BRANCH", "main")
    identity = resolve_snapshot_identity(route.data_asset_ref, branch)
    result = execute_quality_gate(
        GXTableValidationRequest(
            dataset_id=route.dataset_id,
            data_layer="silver",
            source_kind="database",
            run_id=str(context.get("run_id") or ""),
            contract_version=route.contract_version,
            data_asset_ref=route.data_asset_ref,
            coverage=route.coverage,
            nessie_branch=identity.branch,
            snapshot_id=identity.snapshot_id,
            nessie_commit_hash=identity.commit_hash,
            execution_mode=os.getenv("GX_DATABASE_GATE_MODE", "enforce"),
        ).as_payload(),
        service_url=os.getenv("GX_SERVICE_URL", "http://gx-validator:8090"),
        timeout_seconds=float(os.getenv("GX_DATABASE_GATE_TIMEOUT_SECONDS", "300")),
        snapshot_refreshes=1,
    )
    result.update({
        "integration_status": "PASSED",
        "dataset_id": route.dataset_id,
        "data_asset_ref": route.data_asset_ref,
        "source_kind": "database",
        "coverage": route.coverage,
        "contract_status": route.contract_status,
    })
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return result
