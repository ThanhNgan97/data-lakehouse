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
from checkpoint_store import PostgresCheckpointStore
from nessie_catalog_utils import use_main
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
        os.getenv("CTU_IOC_API_KEY")
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
    state = PostgresCheckpointStore().load(
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


def command_process_gold(
    config: ApiDatasetConfig,
    run_id: str,
) -> None:
    processor = _load_callable(
        str(config.gold_processor)
    )

    spark = get_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    try:
        result = processor(
            spark,
            merge_to_main=True,
        )

        print(f"DATASET_ID={config.dataset}")
        print(
            f"GOLD_PROCESSOR="
            f"{config.gold_processor}"
        )
        _print_primitive_result(
            "GOLD",
            result,
        )
        print("PROCESS_GOLD_PASS=True")
    finally:
        try:
            use_main(spark)
        finally:
            spark.stop()


def _expected_gold_source_rows(
    spark,
    config: ApiDatasetConfig,
) -> int:
    silver = spark.table(
        config.silver_table
    )

    if not config.delete_supported:
        return silver.count()

    delete_field = config.canonical_field(
        str(config.source_delete_field)
    )

    return (
        silver
        .filter(
            F.col(delete_field)
            == F.lit(False)
        )
        .count()
    )


def command_validate_gold(
    config: ApiDatasetConfig,
    run_id: str,
) -> None:
    gold_module_name = str(
        config.gold_processor
    ).split(":", 1)[0]
    gold_module = importlib.import_module(
        gold_module_name
    )

    try:
        quality_function = getattr(
            gold_module,
            "check_gold_quality",
        )
    except AttributeError as exc:
        raise ValueError(
            "Configured Gold module does not expose "
            "check_gold_quality"
        ) from exc

    spark = get_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    try:
        use_main(spark)
        spark.catalog.clearCache()

        expected_rows = (
            _expected_gold_source_rows(
                spark,
                config,
            )
        )

        quality = quality_function(
            spark,
            expected_rows,
        )

        gold = spark.table(
            str(config.gold_table)
        )

        gold_rows = gold.count()
        distinct_keys = (
            gold
            .select(*config.business_key)
            .distinct()
            .count()
        )
        duplicate_groups = (
            gold
            .groupBy(*config.business_key)
            .count()
            .filter(F.col("count") > 1)
            .count()
        )

        if gold_rows <= 0:
            raise RuntimeError(
                "Gold validation found zero rows"
            )

        if gold_rows != expected_rows:
            raise RuntimeError(
                "Gold row reconciliation failed: "
                f"expected={expected_rows}, "
                f"actual={gold_rows}"
            )

        if distinct_keys != gold_rows:
            raise RuntimeError(
                "Gold grain reconciliation failed"
            )

        if duplicate_groups != 0:
            raise RuntimeError(
                "Gold contains duplicate business keys"
            )

        print(f"DATASET_ID={config.dataset}")
        print(
            f"GOLD_TABLE={config.gold_table}"
        )
        print(
            f"EXPECTED_GOLD_ROWS="
            f"{expected_rows}"
        )
        print(f"GOLD_ROW_COUNT={gold_rows}")
        print(
            f"GOLD_DISTINCT_BUSINESS_KEYS="
            f"{distinct_keys}"
        )
        print(
            f"GOLD_DUPLICATE_KEY_GROUPS="
            f"{duplicate_groups}"
        )
        _print_primitive_result(
            "GOLD_QUALITY",
            quality,
        )
        print("GOLD_QUALITY_RESULT=PASS")
        print("VALIDATE_GOLD_PASS=True")
    finally:
        spark.stop()


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
    gold_table = str(config.gold_table)

    if not gold_table.startswith(
        "lakehouse.gold."
    ):
        raise ValueError(
            "Refusing non-Gold Trino target"
        )

    sql = (
        "SELECT count(*) "
        f"FROM {gold_table}"
    )

    row_count = _trino_scalar_count(sql)

    if row_count <= 0:
        raise RuntimeError(
            "Trino Gold query returned zero rows"
        )

    print(f"DATASET_ID={config.dataset}")
    print(f"GOLD_TABLE={gold_table}")
    print("TRINO_CONNECTION=PASS")
    print("TRINO_QUERY=PASS")
    print(f"TRINO_ROW_COUNT={row_count}")
    print("TRINO_SMOKE_TEST_PASS=True")


COMMANDS = {
    "api-preflight": command_api_preflight,
    "ingest-bronze": command_ingest_bronze,
    "validate-bronze": command_validate_bronze,
    "process-silver": command_process_silver,
    "validate-silver": command_validate_silver,
    "process-gold": command_process_gold,
    "validate-gold": command_validate_gold,
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
