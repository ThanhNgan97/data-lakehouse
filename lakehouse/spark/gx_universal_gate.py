"""GX publication gate used by the universal file-upload DAG."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any, Callable
from urllib.parse import quote
from urllib.request import urlopen

from gx_airflow_adapter import execute_quality_gate
from gx_route_resolver import GXRouteNotFoundError, resolve_route


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
    content = _read_json(
        f"{base}/contents/{quote(key, safe='.')}?ref={quote(branch, safe='')}",
        opener,
    )
    if reference.get("type") != "BRANCH" or not reference.get("hash"):
        raise RuntimeError(f"Nessie branch identity is invalid for {branch}")
    if content.get("type") != "ICEBERG_TABLE" or content.get("snapshotId") is None:
        raise RuntimeError(f"Nessie content is not an Iceberg table: {key}@{branch}")
    return SnapshotIdentity(branch, str(content["snapshotId"]), str(reference["hash"]))


def build_file_validation_request(
    *, dataset_id: str, run_id: str, contract_version: str,
    identity: SnapshotIdentity
) -> dict[str, Any]:
    return {
        "dataset_id": dataset_id,
        "data_layer": "silver",
        "contract_version": contract_version,
        "source_kind": "file",
        "run_id": run_id,
        "nessie_branch": identity.branch,
        "snapshot_id": identity.snapshot_id,
        "nessie_commit_hash": identity.commit_hash,
        "execution_engine": "sql",
        "execution_mode": os.getenv("GX_FILE_GATE_MODE", "enforce"),
    }


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
    try:
        route = resolve_route(
            dataset_id=str(dataset_id or ""),
            data_layer="silver",
            source_kind="file",
            execution_engine="sql",
        )
    except GXRouteNotFoundError:
        result = {
            "status": "NOT_CONFIGURED",
            "reason": "No GX contract route for this dynamically uploaded dataset",
            "dataset_id": dataset_id,
            "data_asset_ref": data_asset_ref,
            "merge_allowed": None,
        }
        print(json.dumps(result, ensure_ascii=False, sort_keys=True))
        return result
    if data_asset_ref != route.data_asset_ref:
        raise RuntimeError(
            "Registered upload asset does not match the effective GX route: "
            f"{data_asset_ref!r} != {route.data_asset_ref!r}"
        )

    branch = os.getenv("GX_FILE_NESSIE_BRANCH", "main")
    identity = resolve_snapshot_identity(data_asset_ref, branch)
    result = execute_quality_gate(
        build_file_validation_request(
            dataset_id=route.dataset_id,
            run_id=run_id,
            contract_version=route.contract_version,
            identity=identity,
        ),
        service_url=os.getenv("GX_SERVICE_URL", "http://gx-validator:8090"),
        timeout_seconds=float(os.getenv("GX_FILE_GATE_TIMEOUT_SECONDS", "300")),
        snapshot_refreshes=1,
    )
    result.update(
        {
            "integration_status": "PASSED",
            "dataset_id": dataset_id,
            "data_asset_ref": data_asset_ref,
            "source_kind": "file",
        }
    )
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return result
