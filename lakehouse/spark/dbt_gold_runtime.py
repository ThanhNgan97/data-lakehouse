"""Retry-stable Gold lifecycle. Atomically publish the validated Iceberg output bundle."""
import copy
import json
import os
import time
from urllib.parse import quote, urlparse

import boto3
from botocore.config import Config
import requests

from dbt_gold_contract import identity, outputs, SQL_VALIDATION_CONTRACT, require_config
from env_config import S3_ENDPOINT, S3_ACCESS_KEY, S3_SECRET_KEY, S3_BUCKET_NAME, NESSIE_API_URL

WORKER = os.environ.get("DBT_GOLD_WORKER_URL", "http://dbt-worker:8080")


def api(method, path, **kwargs):
    response = requests.request(method, NESSIE_API_URL.rstrip("/") + path, timeout=30, **kwargs)
    response.raise_for_status()
    return response.json()


def head(ref):
    return api("GET", "/trees/tree/" + quote(ref, safe=""))["hash"]


def content(ref, key, hash_value=None):
    response = requests.get(NESSIE_API_URL.rstrip("/") + "/contents/" + key,
                            params={"ref": ref, **({"hashOnRef": hash_value} if hash_value else {})}, timeout=30)
    if response.status_code == 404:
        return None
    response.raise_for_status()
    return response.json()


def store():
    return boto3.client("s3", endpoint_url=S3_ENDPOINT,
                        aws_access_key_id=S3_ACCESS_KEY, aws_secret_access_key=S3_SECRET_KEY,
                        region_name=os.environ.get("S3_REGION", "us-east-1"),
                        config=Config(s3={"addressing_style": "path"}, connect_timeout=10, read_timeout=30))


def state_key(config, run_id):
    return "orchestration/dbt_gold/" + identity(config.dataset, run_id)[0] + ".json"


def load(config, run_id, optional=False):
    require_config(config)
    try:
        result = store().get_object(Bucket=S3_BUCKET_NAME, Key=state_key(config, run_id))
        state = json.loads(result["Body"].read())
    except Exception as exc:
        if optional and getattr(exc, "response", {}).get("Error", {}).get("Code") in ("NoSuchKey", "404"):
            return None
        raise
    if state["dataset"] != config.dataset or state["run_id"] != run_id:
        raise RuntimeError("Gold state identity mismatch")
    job, ref, alias = identity(config.dataset, run_id)
    if (state.get("job") != job or state.get("ref") != ref or state.get("alias") != alias
            or state.get("validation_contract") != SQL_VALIDATION_CONTRACT
            or {name: item.get("alias") for name, item in state.get("outputs", {}).items()}
            != outputs(config.dataset, run_id)):
        raise RuntimeError("Gold state contract/output identity mismatch")
    if state["target_ref"] != os.environ.get("GOLD_TARGET_REF", "main"):
        raise RuntimeError("Target ref changed within the same run")
    return state


def save(config, run_id, state):
    store().put_object(Bucket=S3_BUCKET_NAME, Key=state_key(config, run_id),
                       Body=json.dumps(state).encode(), ContentType="application/json")


def prepare(config, run_id):
    require_config(config)
    state = load(config, run_id, optional=True)
    if state:
        return state
    job, ref, alias = identity(config.dataset, run_id)
    target = os.environ.get("GOLD_TARGET_REF", "main")
    if target != "main" and not target.startswith("dbt_gold_test_"):
        raise ValueError("Only main or dbt_gold_test_* targets are allowed")
    base_hash = head(target)
    source_key = ".".join(config.silver_table.split(".")[-2:])
    source = content(target, source_key, base_hash)
    if not source or source.get("type") != "ICEBERG_TABLE":
        raise RuntimeError("Silver source must exist before dbt prepare")
    if content(target, "gold", base_hash) is None:
        raise RuntimeError("Target Gold namespace must exist before dbt publication")
    try:
        api("POST", "/trees/tree", params={"sourceRefName": target},
            json={"type": "BRANCH", "name": ref, "hash": base_hash})
    except requests.HTTPError as exc:
        if exc.response.status_code != 409 or head(ref) != base_hash:
            raise

    state = {"dataset": config.dataset, "run_id": run_id, "job": job, "ref": ref,
             "alias": alias, "target_ref": target, "base_hash": base_hash,
             "phase": "prepared", "validation_contract": SQL_VALIDATION_CONTRACT, "source_content": source}
    state["outputs"] = {name: {"alias": candidate_alias,
        "original_content": content(target, "gold." + name, base_hash)}
        for name, candidate_alias in outputs(config.dataset, run_id).items()}
    save(config, run_id, state)
    return state


def worker_result(state):
    response = requests.get(WORKER + "/jobs/" + state["job"], timeout=30)
    response.raise_for_status()
    result = response.json()
    if (result["status"] != "passed" or result["ref"] != state["ref"]
            or result.get("dataset") != state["dataset"]
            or result.get("job_id") != state["job"] or result.get("run_id") != state["run_id"]
            or result.get("expected_head") != state["base_hash"]
            or result.get("validation_contract") != SQL_VALIDATION_CONTRACT
            or result.get("catalog_cleanup_failed")):
        raise RuntimeError("dbt worker has not produced a successful build")
    return result


def build(config, run_id):
    state = load(config, run_id)
    if state["phase"] in ("built", "validated", "published"):
        worker_result(state)
        return
    response = requests.post(WORKER + "/jobs", json={"dataset": config.dataset, "run_id": run_id, "expected_head": state["base_hash"]}, timeout=30)
    if not response.ok:
        raise RuntimeError(f"dbt worker rejected build: {response.status_code} {response.text}")
    deadline = time.monotonic() + 1200
    while True:
        try:
            response = requests.get(WORKER + "/jobs/" + state["job"], timeout=30)
        except requests.ConnectionError:
            if time.monotonic() >= deadline:
                raise
            time.sleep(2)
            continue
        if response.status_code == 503 and time.monotonic() < deadline:
            time.sleep(2)
            continue
        response.raise_for_status()
        result = response.json()
        if result["status"] == "passed":
            state.update(phase="built", artifact_sha256=result["artifact_sha256"])
            save(config, run_id, state)
            return
        if result["status"] == "failed":
            raise RuntimeError(result.get("error", "dbt worker failed"))
        if time.monotonic() > deadline:
            raise RuntimeError("dbt worker timeout; retry task to poll the same job")
        time.sleep(2)


def validate(config, run_id):
    return _validate_sql(config, run_id, load(config, run_id))


def metadata_document(candidate):
    location = urlparse(candidate["metadataLocation"])
    if location.scheme not in ("s3", "s3a") or location.netloc != S3_BUCKET_NAME:
        raise RuntimeError("Candidate metadata is outside the configured warehouse bucket")
    raw = store().get_object(Bucket=location.netloc, Key=location.path.lstrip("/"))["Body"].read()
    if raw.startswith(b"\x1f\x8b"):
        import gzip
        raw = gzip.decompress(raw)
    metadata = json.loads(raw)
    for pointer, field in (("snapshotId", "current-snapshot-id"), ("schemaId", "current-schema-id"),
                           ("specId", "default-spec-id"), ("sortOrderId", "default-sort-order-id")):
        if candidate.get(pointer) != metadata.get(field):
            raise RuntimeError("Iceberg metadata and Nessie pointer disagree: " + pointer)
    return metadata


def _validate_sql(config, run_id, state):
    if state["phase"] == "published":
        return
    if state["phase"] not in ("built", "validated"):
        raise RuntimeError("Gold build must complete before validation")
    result = worker_result(state)
    expected_contract = state.get("validation_contract")
    if expected_contract != SQL_VALIDATION_CONTRACT or result.get("validation_contract") != expected_contract or result.get("dataset") != config.dataset:
        raise RuntimeError("Build lacks the current mandatory SQL quality contract; start a new run")
    if result["artifact_sha256"] != state["artifact_sha256"]:
        raise RuntimeError("Build artifact changed after completion")
    expected_hash = state.get("validated_hash") if state["phase"] == "validated" else result.get("build_ref_hash")
    if not expected_hash or head(state["ref"]) != expected_hash:
        raise RuntimeError("Candidate changed after dbt build/validation")
    if state["phase"] == "validated":
        return
    entries = state["outputs"]
    sealed = result.get("candidate_contents", {})
    if set(entries) != set(sealed) or not result.get("source_content"):
        raise RuntimeError("Build output bundle/source receipt is incomplete")
    source_key = ".".join(config.silver_table.split(".")[-2:])
    if (result["source_content"] != state["source_content"]
            or content(state["ref"], source_key, expected_hash) != result["source_content"]):
        raise RuntimeError("Silver source changed after dbt tests")
    from dbt_gold_metadata import guard
    guard_receipt = guard(config, run_id, state)
    guarded_hash = head(state["ref"])
    if content(state["ref"], source_key, guarded_hash) != result["source_content"]:
        raise RuntimeError("Silver source changed after dbt tests")
    candidates = {}
    for name, entry in entries.items():
        candidate = content(state["ref"], "gold." + entry["alias"], guarded_hash)
        if not candidate or candidate.get("type") != "ICEBERG_TABLE" or any(
                candidate.get(key) != sealed[name].get(key)
                for key in ("snapshotId", "schemaId", "specId", "sortOrderId")):
            raise RuntimeError("Candidate data/schema changed after dbt tests")
        properties = metadata_document(candidate).get("properties", {})
        if any(properties.get(key) != "false" for key in ("gc.enabled", "write.metadata.delete-after-commit.enabled")):
            raise RuntimeError("Nessie GC guard missing")
        candidates[name] = candidate
    if head(state["ref"]) != guarded_hash:
        raise RuntimeError("Candidate changed during quality validation")
    state.update(phase="validated", validation_contract=SQL_VALIDATION_CONTRACT,
                 quality={"engine":"dbt_sql", "contract":SQL_VALIDATION_CONTRACT, "metadata_guard":guard_receipt},
                 validated_hash=guarded_hash, candidate_contents=candidates)
    save(config, run_id, state)


def _require_gx_approval(result, state, config, data_layer):
    from gx_candidate_binding import dbt_gold_candidate_binding

    canonical = config.gold_table
    binding = dbt_gold_candidate_binding(state["dataset"], state["run_id"], canonical)
    output_name = canonical.rsplit(".", 1)[-1]
    candidate = state["candidate_contents"].get(output_name)
    approval = result.get("approval") if isinstance(result, dict) else None
    if (
        not isinstance(result, dict)
        or result.get("data_layer") != data_layer
        or result.get("merge_allowed") is not True
        or result.get("data_asset_ref") != canonical
        or result.get("physical_data_asset_ref") != binding.physical_asset_ref
        or result.get("candidate_branch") != state["ref"]
        or result.get("candidate_hash") != state["validated_hash"]
        or not isinstance(candidate, dict)
        or not isinstance(approval, dict)
        or approval.get("merge_allowed") is not True
        or str(result.get("validation_id") or "")
        != str(approval.get("validation_id") or "")
    ):
        raise RuntimeError(f"Refuse publish: valid GX {data_layer} approval is required")
    expected = {
        "data_asset_ref": binding.physical_asset_ref,
        "validated_branch": state["ref"],
        "validated_snapshot_id": str(candidate.get("snapshotId") or ""),
        "validated_commit_hash": state["validated_hash"],
    }
    if not str(approval.get("validation_id") or "").strip() or any(
        str(approval.get(key) or "") != str(value) for key, value in expected.items()
    ):
        raise RuntimeError(f"Refuse publish: stale GX {data_layer} approval")
    return approval


def publish(config, run_id, gx_gold_result=None, gx_reconciliation_result=None):
    state = load(config, run_id)
    if state["phase"] == "published":
        return
    if state["phase"] != "validated":
        raise RuntimeError("Refuse publish: Gold has not passed validation")
    gold_approval = _require_gx_approval(gx_gold_result, state, config, "gold")
    reconciliation_approval = _require_gx_approval(
        gx_reconciliation_result, state, config, "reconciliation"
    )
    if worker_result(state)["artifact_sha256"] != state["artifact_sha256"] or head(state["ref"]) != state["validated_hash"]:
        raise RuntimeError("Validated candidate changed; publish refused")
    target = state["target_ref"]
    entries = state["outputs"]
    candidates = state["candidate_contents"]
    if set(entries) != set(candidates):
        raise RuntimeError("Validated output bundle mismatch")
    target_hash = head(target)
    current = {name: content(target, "gold." + name, target_hash) for name in entries}
    # Recover only when the whole bundle is already promoted, never a partial bundle.
    if all(current[name] and current[name].get("metadataLocation") == candidates[name]["metadataLocation"] for name in entries):
        state.update(
            phase="published",
            published_hash=target_hash,
            gx_approvals={
                "gold": gold_approval["validation_id"],
                "reconciliation": reconciliation_approval["validation_id"],
            },
        )
        save(config, run_id, state)
        return
    if target_hash != state["base_hash"] or any(current[name] != entries[name]["original_content"] for name in entries):
        raise RuntimeError("Target changed since prepare; start a new run, do not overwrite")
    operations = []
    for name in entries:
        candidate = copy.deepcopy(candidates[name])
        original = current[name]
        if original:
            candidate["id"] = original["id"]
        else:
            candidate.pop("id", None)
        operation = {"type": "PUT", "key": {"elements": ["gold", name]}, "content": candidate}
        if original:
            operation["expectedContent"] = original
        operations.append(operation)
    committed = api("POST", "/trees/branch/" + quote(target, safe="") + "/commit",
                    params={"expectedHash": state["base_hash"]},
                    json={"commitMeta": {"message": "dbt Gold " + state["job"]}, "operations": operations})
    for name, candidate in candidates.items():
        if content(target, "gold." + name, committed["hash"])["metadataLocation"] != candidate["metadataLocation"]:
            raise RuntimeError("Gold publish readback mismatch")
    state.update(
        phase="published",
        published_hash=committed["hash"],
        gx_approvals={
            "gold": gold_approval["validation_id"],
            "reconciliation": reconciliation_approval["validation_id"],
        },
    )
    save(config, run_id, state)
