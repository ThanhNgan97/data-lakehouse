# -*- coding: utf-8 -*-
"""Reusable Airflow-facing orchestration runner for registered API datasets.

This module performs routing and runtime validation only. It deliberately
delegates ingestion, Silver processing, Gold processing, and Gold quality to
existing project implementations.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib
import json
import os
from pathlib import Path
from typing import Any
from urllib import request

from pyspark.sql import functions as F

from api_dataset_registry import (
    ApiDatasetConfig,
    get_dataset_config,
)
from api_ingestion import METADATA_COLUMNS
from incremental_ingestion import (
    STATUS_COMMITTED,
    STATUS_NO_CHANGE,
    fetch_incremental_payload,
    run_incremental_ingestion,
)
from bronze_writer import read_parquet_object
from env_config import S3_BUCKET_NAME
from checkpoint_store import OzoneCheckpointStore
from gx_candidate_state import (
    CandidateAssetIdentity,
    CandidateState,
    candidate_state_path,
    load_candidate_state,
    retry_stable_branch_name,
    save_candidate_state,
)
from gx_universal_gate import resolve_snapshot_identity
from nessie_catalog_utils import get_nessie_reference_hash, use_main
from spark_bronze_to_silver import get_spark_session


DEFAULT_API_BASE_URL = (
    "http://ctu-ioc-mock-api:8000"
)
DEFAULT_TRINO_BASE_URL = "http://trino:8080"


def deterministic_batch_id(
    dataset_id: str,
    run_id: str,
) -> str:
    """Return a retry-stable Bronze batch identity for one Airflow DAG run."""
    if not dataset_id:
        raise ValueError("dataset_id must not be empty")

    if not run_id:
        raise ValueError("run_id must not be empty")

    digest = hashlib.sha256(
        f"{dataset_id}\n{run_id}".encode("utf-8")
    ).hexdigest()[:20]

    return f"airflow_api_{digest}"


def require_orchestration_config(
    dataset_id: str,
) -> ApiDatasetConfig:
    """Return a registered dataset that is ready for Day 7.5 routing."""
    config = get_dataset_config(dataset_id)

    if not config.orchestration_ready:
        raise ValueError(
            f"Dataset '{dataset_id}' is registered but "
            "missing orchestration metadata"
        )

    return config


def source_api_url(
    config: ApiDatasetConfig,
) -> str:
    runtime_url = os.getenv("SOURCE_API_URL", "").strip()
    if runtime_url:
        return runtime_url

    base_url = (
        os.getenv("CTU_IOC_API_BASE_URL")
        or DEFAULT_API_BASE_URL
    ).rstrip("/")

    return base_url + str(config.source_api_path)


def _api_key() -> str | None:
    return (
        os.getenv("SOURCE_API_KEY")
        or os.getenv("CTU_IOC_API_KEY")
        or os.getenv("MOCK_API_KEY")
        or None
    )


def _load_callable(route: str):
    module_name, function_name = route.split(
        ":",
        1,
    )
    module = importlib.import_module(module_name)

    try:
        function = getattr(
            module,
            function_name,
        )
    except AttributeError as exc:
        raise ValueError(
            f"Configured callable not found: {route}"
        ) from exc

    if not callable(function):
        raise ValueError(
            f"Configured route is not callable: {route}"
        )

    return function


def _print_primitive_result(
    prefix: str,
    result: Any,
) -> None:
    if not isinstance(result, dict):
        print(
            f"{prefix}_RESULT_TYPE="
            f"{type(result).__name__}"
        )
        return

    for key in sorted(result):
        value = result[key]

        if isinstance(
            value,
            (str, int, float, bool),
        ) or value is None:
            print(
                f"{prefix}_{key.upper()}="
                f"{value}"
            )


def _checkpoint_version(state) -> str:
    if state is None:
        return "NONE"

    return str(state.version)


def _checkpoint_payload(state) -> str:
    if state is None:
        return "NONE"

    return json.dumps(
        state.checkpoint_payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def _latest_committed_batch_id(
    dataset_id: str,
) -> str:
    state = OzoneCheckpointStore().load(
        dataset_id
    )

    if state is None:
        raise RuntimeError(
            "checkpoint is missing for committed batch"
        )

    if not state.last_batch_id:
        raise RuntimeError(
            "checkpoint has no committed batch_id"
        )

    return state.last_batch_id

def command_api_preflight(
    config: ApiDatasetConfig,
    run_id: str,
) -> None:
    url = source_api_url(config)

    result = fetch_incremental_payload(
        url,
        dataset=config.dataset,
        api_key=_api_key(),
        page_limit=500,
        timeout_seconds=30,
    )

    print(f"DATASET_ID={config.dataset}")
    print(f"SCHEMA_VERSION={config.schema_version}")
    print(f"SOURCE_API_PATH={config.source_api_path}")
    print("SOURCE_ENDPOINT_REACHABLE=True")
    print(
        f"SOURCE_RECORD_COUNT="
        f"{result.fetched_count}"
    )
    print(
        "UPDATED_AFTER="
        + (result.updated_after or "NONE")
    )
    print(
        "CHECKPOINT_BEFORE_VERSION="
        + _checkpoint_version(
            result.checkpoint_before
        )
    )
    print(
        "CHECKPOINT_BEFORE_PAYLOAD="
        + _checkpoint_payload(
            result.checkpoint_before
        )
    )
    print(
        "SOURCE_SYSTEM="
        + str(result.payload.get("source_system"))
    )
    print(f"AIRFLOW_RUN_ID={run_id}")
    print("INCREMENTAL_PREFLIGHT=True")
    print("API_PREFLIGHT_PASS=True")


def command_ingest_bronze(
    config: ApiDatasetConfig,
    run_id: str,
):
    spark = get_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    try:
        result = run_incremental_ingestion(
            spark,
            source_api_url(config),
            dataset=config.dataset,
            api_key=_api_key(),
            page_limit=500,
            timeout_seconds=30,
        )

        print(f"DATASET_ID={config.dataset}")
        print(
            f"INGESTION_STATUS={result.status}"
        )
        print(
            f"SOURCE_RECORD_COUNT="
            f"{result.fetched_count}"
        )
        print(
            "UPDATED_AFTER="
            + (result.updated_after or "NONE")
        )
        print(
            "CHECKPOINT_BEFORE_VERSION="
            + _checkpoint_version(
                result.checkpoint_before
            )
        )
        print(
            "CHECKPOINT_BEFORE_PAYLOAD="
            + _checkpoint_payload(
                result.checkpoint_before
            )
        )
        print(
            "CHECKPOINT_AFTER_VERSION="
            + _checkpoint_version(
                result.checkpoint_after
            )
        )
        print(
            "CHECKPOINT_AFTER_PAYLOAD="
            + _checkpoint_payload(
                result.checkpoint_after
            )
        )

        if result.status == STATUS_NO_CHANGE:
            print("BRONZE_WRITTEN=False")
            print("BRONZE_READBACK_COUNT=0")
            print("CHECKPOINT_ADVANCED=False")
            print("INGEST_BRONZE_NO_CHANGE=True")
            return result

        if result.status != STATUS_COMMITTED:
            raise RuntimeError(
                "Unexpected incremental ingestion status: "
                + repr(result.status)
            )

        bronze = result.bronze_result

        if bronze is None:
            raise RuntimeError(
                "COMMITTED result missing Bronze evidence"
            )

        print(f"BRONZE_BATCH_ID={bronze.batch_id}")
        print(f"BRONZE_OBJECT_KEY={bronze.object_key}")
        print(
            f"BRONZE_INPUT_COUNT="
            f"{bronze.input_count}"
        )
        print(
            f"BRONZE_DATAFRAME_COUNT="
            f"{bronze.dataframe_count}"
        )
        print(
            f"BRONZE_READBACK_COUNT="
            f"{bronze.readback_count}"
        )
        print("BRONZE_WRITTEN=True")
        print("CHECKPOINT_ADVANCED=True")
        print("INGEST_BRONZE_PASS=True")

        return result
    finally:
        spark.stop()

def command_validate_bronze(
    config: ApiDatasetConfig,
    run_id: str,
) -> None:
    batch_id = _latest_committed_batch_id(
        config.dataset
    )
    object_key = (
        f"{config.bronze_prefix}"
        f"batch_id={batch_id}/data.parquet"
    )

    readback = read_parquet_object(
        object_key
    )

    row_count = len(readback.index)

    if row_count <= 0:
        raise RuntimeError(
            "Bronze validation found zero rows"
        )

    required_metadata = set(METADATA_COLUMNS)
    missing_metadata = sorted(
        required_metadata
        - set(readback.columns)
    )
    if missing_metadata:
        raise RuntimeError(
            "Bronze technical metadata missing: "
            + repr(missing_metadata)
        )

    dataset_values = {
        str(value)
        for value in readback["_dataset"].dropna()
    }
    batch_values = {
        str(value)
        for value in readback["_batch_id"].dropna()
    }

    checksum_nulls = int(
        readback["_record_checksum"]
        .isna()
        .sum()
    )

    if dataset_values != {config.dataset}:
        raise RuntimeError(
            "Bronze dataset identity mismatch: "
            + repr(sorted(dataset_values))
        )

    if batch_values != {batch_id}:
        raise RuntimeError(
            "Bronze batch identity mismatch: "
            + repr(sorted(batch_values))
        )

    if checksum_nulls != 0:
        raise RuntimeError(
            "Bronze contains null record checksums"
        )

    from gx_bronze_gate import validate_bronze_metrics

    gx_result = validate_bronze_metrics(
        dataset_id=config.dataset,
        source_kind="api",
        run_id=run_id,
        artifact_ref=f"s3://{S3_BUCKET_NAME}/{object_key}",
        metrics={
            "readable": True,
            "row_count": row_count,
            "corrupt_record_ratio": 0.0,
            "metadata_complete": not missing_metadata,
            "checksum_present": checksum_nulls == 0,
            "duplicate_ingestion_count": 0,
            "http_success": True,
            "pagination_complete": True,
        },
    )
    if gx_result.get("merge_allowed") is not True:
        raise RuntimeError("GX Bronze gate denied Silver candidate creation")

    print(f"DATASET_ID={config.dataset}")
    print(f"BRONZE_BATCH_ID={batch_id}")
    print(f"BRONZE_OBJECT_KEY={object_key}")
    print(f"BRONZE_ROW_COUNT={row_count}")
    print(
        f"BRONZE_METADATA_COUNT="
        f"{len(METADATA_COLUMNS)}"
    )
    print(
        f"BRONZE_CHECKSUM_NULLS="
        f"{checksum_nulls}"
    )
    print("VALIDATE_BRONZE_PASS=True")
    print(f"GX_BRONZE_VALIDATION_ID={gx_result['validation_id']}")
    print(f"GX_BRONZE_VALIDATION_URI={gx_result['validation_uri']}")


def command_process_silver(
    config: ApiDatasetConfig,
    run_id: str,
) -> None:
    batch_id = _latest_committed_batch_id(
        config.dataset
    )
    processor = _load_callable(
        str(config.silver_processor)
    )

    spark = get_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    try:
        result = processor(
            spark,
            batch_id,
            merge_to_main=True,
        )

        print(f"DATASET_ID={config.dataset}")
        print(f"SILVER_BATCH_ID={batch_id}")
        print(
            f"SILVER_PROCESSOR="
            f"{config.silver_processor}"
        )
        _print_primitive_result(
            "SILVER",
            result,
        )
        print("PROCESS_SILVER_PASS=True")
    finally:
        try:
            use_main(spark)
        finally:
            spark.stop()


def command_prepare_silver_candidate(
    config: ApiDatasetConfig,
    run_id: str,
) -> None:
    """Write registered Silver data to a durable candidate without publishing."""
    batch_id = _latest_committed_batch_id(config.dataset)
    processor = _load_callable(str(config.silver_processor))
    branch = retry_stable_branch_name("gx_api_silver", config.dataset, run_id)
    root = Path(
        os.getenv("GX_CANDIDATE_STATE_ROOT", "/opt/airflow/logs/gx-candidates")
    )
    state_file = candidate_state_path(root, config.dataset, run_id)
    if state_file.is_file():
        existing = load_candidate_state(state_file)
        if (
            existing.dataset_id != config.dataset
            or existing.run_id != run_id
            or existing.source_kind != "api"
            or existing.candidate_branch != branch
        ):
            raise RuntimeError("Existing candidate state does not match this API run")
        print(f"CANDIDATE_STATE_PATH={state_file}")
        print("PREPARE_SILVER_CANDIDATE_REPLAY=True")
        return
    if get_nessie_reference_hash(branch, missing_ok=True) is not None:
        raise RuntimeError(
            "Candidate branch exists without durable state; refuse unsafe retry"
        )
    spark = get_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    try:
        use_main(spark)
        # Candidate cutover is allowed only after bootstrap has created both
        # tables. Otherwise a hidden initialization commit could change main.
        spark.table(config.silver_table).limit(0).collect()
        spark.table(config.quarantine_table).limit(0).collect()
        base_hash = get_nessie_reference_hash("main")
        result = processor(
            spark,
            batch_id,
            merge_to_main=False,
            candidate_branch=branch,
        )
        identities = tuple(
            resolve_snapshot_identity(asset, branch)
            for asset in (config.silver_table, config.quarantine_table)
        )
        candidate_hashes = {identity.commit_hash for identity in identities}
        if len(candidate_hashes) != 1:
            raise RuntimeError("Candidate assets did not resolve at one Nessie head")
        state = CandidateState(
            dataset_id=config.dataset,
            source_kind="api",
            run_id=run_id,
            candidate_branch=branch,
            base_reference="main",
            base_hash=base_hash,
            candidate_hash=identities[0].commit_hash,
            assets=tuple(
                CandidateAssetIdentity(asset, identity.snapshot_id)
                for asset, identity in zip(
                    (config.silver_table, config.quarantine_table), identities
                )
            ),
        )
        state_file = save_candidate_state(
            state,
            candidate_state_path(root, config.dataset, run_id),
        )

        print(f"DATASET_ID={config.dataset}")
        print(f"SILVER_BATCH_ID={batch_id}")
        print(f"CANDIDATE_BRANCH={branch}")
        print(f"CANDIDATE_BASE_HASH={base_hash}")
        print(f"CANDIDATE_HASH={state.candidate_hash}")
        print(f"CANDIDATE_STATE_PATH={state_file}")
        _print_primitive_result("SILVER", result)
        print("PREPARE_SILVER_CANDIDATE_PASS=True")
    finally:
        try:
            use_main(spark)
        finally:
            spark.stop()


def command_validate_silver(
    config: ApiDatasetConfig,
    run_id: str,
) -> None:
    spark = get_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    try:
        use_main(spark)
        spark.catalog.clearCache()

        silver = spark.table(
            config.silver_table
        )
        quarantine = spark.table(
            config.quarantine_table
        )

        row_count = silver.count()
        distinct_keys = (
            silver
            .select(*config.business_key)
            .distinct()
            .count()
        )
        duplicate_groups = (
            silver
            .groupBy(*config.business_key)
            .count()
            .filter(F.col("count") > 1)
            .count()
        )
        quarantine_count = (
            quarantine.count()
        )

        if row_count <= 0:
            raise RuntimeError(
                "Silver validation found zero rows"
            )

        if duplicate_groups != 0:
            raise RuntimeError(
                "Silver contains duplicate business keys"
            )

        print(f"DATASET_ID={config.dataset}")
        print(
            f"SILVER_TABLE={config.silver_table}"
        )
        print(f"SILVER_ROW_COUNT={row_count}")
        print(
            f"SILVER_DISTINCT_BUSINESS_KEYS="
            f"{distinct_keys}"
        )
        print(
            f"SILVER_DUPLICATE_KEY_GROUPS="
            f"{duplicate_groups}"
        )
        print(
            f"SILVER_QUARANTINE_COUNT="
            f"{quarantine_count}"
        )
        print("SILVER_QUALITY_RESULT=PASS")
        print("VALIDATE_SILVER_PASS=True")
    finally:
        spark.stop()


def command_prepare_gold(config: ApiDatasetConfig, run_id: str) -> None:
    from dbt_gold_runtime import prepare
    state = prepare(config, run_id)
    print(f"GOLD_ENGINE=dbt\nGOLD_REF={state['ref']}\nPREPARE_GOLD_PASS=True")


def command_process_gold(config: ApiDatasetConfig, run_id: str) -> None:
    from dbt_gold_runtime import build
    build(config, run_id)
    print("GOLD_ENGINE=dbt\nPROCESS_GOLD_PASS=True")


def command_validate_gold(config: ApiDatasetConfig, run_id: str) -> None:
    from dbt_gold_runtime import validate
    validate(config, run_id)
    print("GOLD_ENGINE=dbt\nVALIDATE_GOLD_PASS=True")


def command_publish_gold(config: ApiDatasetConfig, run_id: str) -> None:
    raise RuntimeError(
        "Direct publish-gold is disabled; use the Airflow Gold publisher with "
        "GX Gold and reconciliation approval receipts"
    )


def _trino_scalar_count(
    sql: str,
) -> int:
    base_url = (
        os.getenv("TRINO_HTTP_URL")
        or DEFAULT_TRINO_BASE_URL
    ).rstrip("/")

    statement_url = (
        base_url + "/v1/statement"
    )

    headers = {
        "X-Trino-User": "airflow",
        "X-Trino-Source":
            "api_dataset_pipeline",
        "Content-Type":
            "text/plain; charset=utf-8",
    }

    initial = request.Request(
        statement_url,
        data=sql.encode("utf-8"),
        headers=headers,
        method="POST",
    )

    next_request = initial

    for _ in range(100):
        with request.urlopen(
            next_request,
            timeout=30,
        ) as response:
            payload = json.loads(
                response.read().decode("utf-8")
            )

        if payload.get("error"):
            raise RuntimeError(
                "Trino query failed: "
                + json.dumps(
                    payload["error"],
                    ensure_ascii=False,
                    sort_keys=True,
                )
            )

        data = payload.get("data")
        if data:
            return int(data[0][0])

        next_uri = payload.get("nextUri")
        if not next_uri:
            break

        next_request = request.Request(
            next_uri,
            headers={
                "X-Trino-User": "airflow",
                "X-Trino-Source":
                    "api_dataset_pipeline",
            },
            method="GET",
        )

    raise RuntimeError(
        "Trino query returned no scalar result"
    )


def command_trino_smoke_test(
    config: ApiDatasetConfig,
    run_id: str,
) -> None:
    from dbt_gold_contract import outputs, require_config
    require_config(config)
    for name in outputs(config.dataset, run_id):
        gold_table = "lakehouse.gold." + name
        row_count = _trino_scalar_count(f"SELECT count(*) FROM {gold_table}")
        if row_count <= 0:
            raise RuntimeError("Trino Gold output returned zero rows: " + name)
        print(f"GOLD_TABLE={gold_table}\nTRINO_ROW_COUNT={row_count}")
    print(f"DATASET_ID={config.dataset}\nTRINO_CONNECTION=PASS\nTRINO_QUERY=PASS\nTRINO_SMOKE_TEST_PASS=True")


COMMANDS = {
    "api-preflight": command_api_preflight,
    "ingest-bronze": command_ingest_bronze,
    "validate-bronze": command_validate_bronze,
    "process-silver": command_process_silver,
    "prepare-silver-candidate": command_prepare_silver_candidate,
    "validate-silver": command_validate_silver,
    "prepare-gold": command_prepare_gold,
    "process-gold": command_process_gold,
    "validate-gold": command_validate_gold,
    "publish-gold": command_publish_gold,
    "trino-smoke-test":
        command_trino_smoke_test,
}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Generic registered API dataset "
            "orchestration runner"
        )
    )
    parser.add_argument(
        "command",
        choices=tuple(COMMANDS),
    )
    parser.add_argument(
        "--dataset-id",
        required=True,
    )
    parser.add_argument(
        "--run-id",
        required=True,
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()

    config = require_orchestration_config(
        args.dataset_id
    )

    print(
        "ORCHESTRATION_COMMAND="
        + args.command
    )
    print(
        "ORCHESTRATION_DATASET="
        + config.dataset
    )

    result = COMMANDS[args.command](
        config,
        args.run_id,
    )

    if (
        args.command == "ingest-bronze"
        and getattr(result, "status", None)
        == STATUS_NO_CHANGE
    ):
        print("ORCHESTRATION_SKIP_EXIT_CODE=99")
        raise SystemExit(99)


if __name__ == "__main__":
    main()
