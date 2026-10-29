"""GX Silver-to-Gold reconciliation gate for registered datasets."""

from __future__ import annotations

import json
import os
from typing import Any

from gx_airflow_adapter import execute_quality_gate
from gx_route_resolver import resolve_route
from gx_universal_gate import resolve_snapshot_identity
from gx_validation_request import GXTableValidationRequest


def validate_silver_gold_reconciliation(
    *, dataset_id: str, run_id: str, source_kind: str = "api",
) -> dict[str, Any]:
    """Reconcile the registered Silver and Gold assets at one Nessie head."""
    route = resolve_route(
        dataset_id=dataset_id,
        data_layer="reconciliation",
        source_kind=source_kind,
        execution_engine="sql",
    )
    silver_route = resolve_route(
        dataset_id=dataset_id,
        data_layer="silver",
        source_kind=source_kind,
        execution_engine="sql",
    )
    branch = os.getenv("GX_RECONCILIATION_NESSIE_BRANCH", "main")
    # Reconciliation routes use Gold as their identity anchor while suite SQL
    # compares both Silver and Gold under the same Nessie branch/commit.
    identity = resolve_snapshot_identity(route.data_asset_ref, branch)
    result = execute_quality_gate(
        GXTableValidationRequest(
            dataset_id=dataset_id,
            data_layer="reconciliation",
            run_id=run_id,
            contract_version=route.contract_version,
            source_kind=source_kind,
            data_asset_ref=route.data_asset_ref,
            nessie_branch=identity.branch,
            snapshot_id=identity.snapshot_id,
            nessie_commit_hash=identity.commit_hash,
            execution_mode=os.getenv("GX_RECONCILIATION_GATE_MODE", "enforce"),
        ).as_payload(),
        service_url=os.getenv("GX_SERVICE_URL", "http://gx-validator:8090"),
        timeout_seconds=float(
            os.getenv("GX_RECONCILIATION_GATE_TIMEOUT_SECONDS", "300")
        ),
        snapshot_refreshes=1,
    )
    result.update({
        "integration_status": "PASSED",
        "dataset_id": dataset_id,
        "data_asset_ref": route.data_asset_ref,
        "data_layer": "reconciliation",
        "source_kind": source_kind,
        "silver_asset_ref": silver_route.data_asset_ref,
        "gold_asset_ref": route.data_asset_ref,
    })
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return result
