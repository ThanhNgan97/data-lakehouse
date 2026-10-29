"""Snapshot-aware GX validation for dbt-published Gold assets."""

from __future__ import annotations

import json
import os
from typing import Any

from gx_airflow_adapter import execute_quality_gate
from gx_route_resolver import resolve_route
from gx_universal_gate import resolve_snapshot_identity
from gx_validation_request import GXTableValidationRequest


def validate_published_gold(
    *, dataset_id: str, run_id: str, source_kind: str = "api",
) -> dict[str, Any]:
    """Validate the immutable Gold identity produced by dbt publication."""
    route = resolve_route(
        dataset_id=dataset_id,
        data_layer="gold",
        source_kind=source_kind,
        execution_engine="sql",
    )
    branch = os.getenv("GX_GOLD_NESSIE_BRANCH", "main")
    identity = resolve_snapshot_identity(route.data_asset_ref, branch)
    result = execute_quality_gate(
        GXTableValidationRequest(
            dataset_id=dataset_id,
            data_layer="gold",
            run_id=run_id,
            contract_version=route.contract_version,
            source_kind=source_kind,
            data_asset_ref=route.data_asset_ref,
            nessie_branch=identity.branch,
            snapshot_id=identity.snapshot_id,
            nessie_commit_hash=identity.commit_hash,
            execution_mode=os.getenv("GX_GOLD_GATE_MODE", "enforce"),
        ).as_payload(),
        service_url=os.getenv("GX_SERVICE_URL", "http://gx-validator:8090"),
        timeout_seconds=float(os.getenv("GX_GOLD_GATE_TIMEOUT_SECONDS", "300")),
        snapshot_refreshes=1,
    )
    result.update({
        "integration_status": "PASSED",
        "dataset_id": dataset_id,
        "data_asset_ref": route.data_asset_ref,
        "data_layer": "gold",
        "source_kind": source_kind,
    })
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return result
