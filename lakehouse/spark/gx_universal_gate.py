"""GX publication gate used by the universal file-upload DAG."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any, Callable
from urllib.parse import quote
from urllib.request import urlopen

from gx_airflow_adapter import execute_quality_gate
from gx_route_resolver import resolve_route_or_baseline
from gx_validation_request import GXTableValidationRequest


@dataclass(frozen=True)
class SnapshotIdentity:
    branch: str
    snapshot_id: str
    commit_hash: str


def _read_json(url: str, opener: Callable[..., Any] = urlopen) -> dict[str, Any]:
    with opener(url, timeout=15) as response:
        return json.load(response)


def resolve_snapshot_identity(
    data_asset_ref: str,
    branch: str,
    *,
    nessie_url: str | None = None,
    opener: Callable[..., Any] = urlopen,
) -> SnapshotIdentity:
    base = (nessie_url or os.getenv("NESSIE_API_URL", "http://nessie:19120/api/v1")).rstrip("/")
    key = ".".join(data_asset_ref.split(".")[-2:])
    reference = _read_json(f"{base}/trees/tree/{quote(branch, safe='')}", opener)
    if reference.get("type") != "BRANCH" or not reference.get("hash"):
        raise RuntimeError(f"Nessie branch identity is invalid for {branch}")
    commit_hash = str(reference["hash"])
    content = _read_json(
        f"{base}/contents/{quote(key, safe='.')}?ref={quote(branch, safe='')}"
        f"&hashOnRef={quote(commit_hash, safe='')}",
        opener,
    )
    if content.get("type") != "ICEBERG_TABLE" or content.get("snapshotId") is None:
        raise RuntimeError(f"Nessie content is not an Iceberg table: {key}@{branch}")
    return SnapshotIdentity(branch, str(content["snapshotId"]), commit_hash)


def validate_file_upload(**context) -> dict[str, Any]:
    """Resolve the routed Silver asset and execute GX before downstream publication."""
    ti = context["ti"]
    dataset_id = ti.xcom_pull(
        task_ids="ai_semantic_profiler", key="registered_dataset_id"
    )
    data_asset_ref = ti.xcom_pull(
        task_ids="ai_semantic_profiler", key="target_silver_table"
    )
    run_id = str(context.get("run_id") or "")
    route = resolve_route_or_baseline(
        dataset_id=str(dataset_id) if dataset_id else None,
        data_asset_ref=str(data_asset_ref) if data_asset_ref else None,
        source_kind="file",
    )

    branch = os.getenv("GX_FILE_NESSIE_BRANCH", "main")
    identity = resolve_snapshot_identity(route.data_asset_ref, branch)
    result = execute_quality_gate(
        GXTableValidationRequest(
            dataset_id=route.dataset_id,
            data_layer="silver",
            source_kind="file",
            run_id=run_id,
            contract_version=route.contract_version,
            data_asset_ref=route.data_asset_ref,
            coverage=route.coverage,
            nessie_branch=identity.branch,
            snapshot_id=identity.snapshot_id,
            nessie_commit_hash=identity.commit_hash,
            execution_mode=os.getenv("GX_FILE_GATE_MODE", "enforce"),
        ).as_payload(),
        service_url=os.getenv("GX_SERVICE_URL", "http://gx-validator:8090"),
        timeout_seconds=float(os.getenv("GX_FILE_GATE_TIMEOUT_SECONDS", "300")),
        snapshot_refreshes=1,
    )
    result.update(
        {
            "integration_status": "PASSED",
            "dataset_id": route.dataset_id,
            "data_asset_ref": route.data_asset_ref,
            "source_kind": "file",
            "coverage": route.coverage,
            "contract_status": route.contract_status,
        }
    )
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return result
