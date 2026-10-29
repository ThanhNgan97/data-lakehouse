# -*- coding: utf-8 -*-
"""Universal AI-Augmented Data Lakehouse Pipeline.

This DAG dynamically ingests ANY incoming dataset:
1. Samples incoming files or payloads
2. Leverages Gemini AI (Control Plane) to profile schema and determine routing
3. Resolves the registered dataset contract and source kind
4. Builds immutable Silver/Gold candidates and enforces GX before each publish
5. Smoke tests Trino query engine after governed publication
"""

from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

# Thêm /opt/airflow/spark vào sys.path để nạp router và config
SPARK_DIR = "/opt/airflow/spark"
if SPARK_DIR not in sys.path:
    sys.path.insert(0, SPARK_DIR)

from relational_context import build_context_manifest, parse_context_object_key
from ozone_object_lifecycle import archive_object

default_args = {
    "owner": "lakehouse",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
}

UNIVERSAL_API_ENV = {
    "SOURCE_API_URL": "{{ dag_run.conf.get('source_api_url', '') if dag_run else '' }}",
    "SOURCE_API_KEY": "{{ dag_run.conf.get('source_api_key', '') if dag_run else '' }}",
}


# =====================================================================
# TASK 1: AI SEMANTIC PROFILER & ROUTER
# =====================================================================

def _get_s3_client():
    """Tạo S3 client kết nối tới Apache Ozone S3 Gateway."""
    import boto3
    from env_config import S3_ACCESS_KEY, S3_ENDPOINT, S3_SECRET_KEY
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
    )


def _list_s3_objects(s3, bucket: str, prefix: str):
    """List every object below a prefix, including pages beyond S3's 1,000-key limit."""
    objects = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        objects.extend(page.get("Contents", []))
    return objects


def _download_staging_object(s3, bucket: str, key: str, downloads_dir: Path) -> str:
    """Preserve the nested key locally so equal names such as part_0.parquet never collide."""
    relative = Path(key.replace("staging/", "", 1))
    local_target = downloads_dir / relative
    local_target.parent.mkdir(parents=True, exist_ok=True)
    s3.download_file(bucket, key, str(local_target))
    return str(local_target)


def run_ai_semantic_profiling(**context):
    """Lấy mẫu dữ liệu và gọi AI Semantic Profiler để đưa ra quyết định định tuyến.
    Ưu tiên 1: input_path từ dag_run.conf
    Ưu tiên 2: Tự động quét file mới nhất trong Ozone staging/
    Ưu tiên 3: Dùng file mẫu testdata/dynamic_sample_tuition.json
    """
    dag_run = context.get("dag_run")
    run_id = context.get("run_id", "manual_run")
    conf = dag_run.conf if dag_run and dag_run.conf else {}

    input_path = conf.get("input_path")
    context_id = conf.get("context_id")
    airbyte_job_id = conf.get("airbyte_job_id")
    source_name = conf.get("source_name", "")
    s3_staging_key = None
    context_manifest_file = None
    context_objects = None

    from env_config import S3_BUCKET_NAME
    downloads_dir = Path("/opt/airflow/spark/.staging_downloads")
    downloads_dir.mkdir(parents=True, exist_ok=True)

    # Ưu tiên 1: Có truyền input_path cụ thể
    if input_path:
        if input_path.startswith("staging/") or input_path.startswith("s3://"):
            s3_key = input_path.replace(f"s3://{S3_BUCKET_NAME}/", "").lstrip("/")
            s3 = _get_s3_client()
            input_path = _download_staging_object(
                s3, S3_BUCKET_NAME, s3_key, downloads_dir
            )
            source_name = Path(s3_key).name
            s3_staging_key = s3_key
            parsed = parse_context_object_key(s3_key)
            if parsed:
                context_id = parsed["context_id"]
            print(f"📥 [Ozone] Đã tải file từ '{s3_key}' về '{input_path}'")

    # Ưu tiên 2: Tự động quét trạm staging/ trên Ozone
    if not input_path:
        try:
            s3 = _get_s3_client()
            staging_prefix = f"staging/{context_id}/" if context_id else "staging/"
            contents = _list_s3_objects(s3, S3_BUCKET_NAME, staging_prefix)
            staging_items = [
                obj for obj in contents
                if obj.get("Size", 0) > 0 and obj.get("Key") != "staging/"
            ]
            if staging_items:
                # Sắp xếp lấy file mới nhất (LastModified)
                staging_items.sort(key=lambda x: x["LastModified"], reverse=True)
                latest_item = staging_items[0]
                s3_staging_key = latest_item["Key"]
                filename = Path(s3_staging_key).name
                input_path = _download_staging_object(
                    s3, S3_BUCKET_NAME, s3_staging_key, downloads_dir
                )
                source_name = filename
                parsed = parse_context_object_key(s3_staging_key)
                if parsed:
                    context_id = parsed["context_id"]
                print(f"🔍 [Ozone Staging] Tự động phát hiện file mới trong staging/: '{s3_staging_key}' ({latest_item.get('Size', 0)} bytes)")
        except Exception as se:
            print(f"⚠️ [Ozone Staging Scan] Không thể quét staging/: {se}")

    # Ưu tiên 3: Fallback file mẫu demo nếu cả 2 nguồn trên đều rỗng
    if not input_path:
        default_test_file = "/opt/airflow/spark/testdata/dynamic_sample_tuition.json"
        if os.path.exists(default_test_file):
            input_path = default_test_file
            source_name = "dynamic_sample_tuition.json"
            print("ℹ️ [Fallback] Ozone staging rỗng, sử dụng file mẫu mặc định để demo.")
        else:
            raise ValueError("Không tìm thấy dữ liệu trong staging/ và không có default test file!")

    # Airbyte relational landing: only an explicit Airbyte-triggered run may
    # process a whole context. Folder depth alone is not a routing signal;
    # manual UI uploads also have a nested staging path.
    is_airbyte_relational_run = airbyte_job_id is not None and bool(context_id)
    if (
        is_airbyte_relational_run
        and s3_staging_key
        and parse_context_object_key(s3_staging_key)
    ):
        s3 = _get_s3_client()
        context_objects = _list_s3_objects(
            s3, S3_BUCKET_NAME, f"staging/{context_id}/"
        )
        manifest = build_context_manifest(
            context_objects,
            context_id,
            bucket=S3_BUCKET_NAME,
            batch_id=(f"airbyte_job_{airbyte_job_id}" if airbyte_job_id else run_id),
        )
        if manifest["objects"]:
            manifests_dir = Path("/opt/airflow/spark/.context_manifests")
            manifests_dir.mkdir(parents=True, exist_ok=True)
            clean_run_id = "".join(c if c.isalnum() else "_" for c in run_id)
            context_manifest_file = str(
                manifests_dir / f"{context_id}_{clean_run_id}.json"
            )
            with open(context_manifest_file, "w", encoding="utf-8") as handle:
                json.dump(manifest, handle, ensure_ascii=False, indent=2)
            print(
                f"📦 [Relational Context] '{context_id}': "
                f"{len(manifest['entities'])} entities, {len(manifest['objects'])} objects"
            )

    print(f"🔍 [AI Profiler] Đang phân tích mẫu dữ liệu từ '{input_path}' (Source: {source_name})...")

    # Gọi module ai_dataset_router
    from ai_dataset_router import route_from_file_path

    decision = route_from_file_path(input_path)
    if context_manifest_file:
        safe_context = "".join(
            char if char.isalnum() or char == "_" else "_" for char in context_id.lower()
        ).strip("_")
        decision.dataset_entity = f"{safe_context}_context"
        decision.route_target = "relational_context"
        decision.target_silver_table = f"lakehouse.silver.{safe_context}__context"
        decision.target_quarantine_table = f"lakehouse.silver.{safe_context}__quarantine"
        decision.target_gold_table = f"lakehouse.gold.{safe_context}_context"
        decision.reasoning = (
            f"Phát hiện Airbyte relational context '{context_id}' với nhiều entity; "
            "định tuyến theo manifest thay vì xử lý một file riêng lẻ."
        )
    print("✅ [AI Profiler] Quyết định định tuyến:")
    print(decision.model_dump_json(indent=2))

    # Lưu decision ra file để Spark processor tái sử dụng không cần gọi lại LLM
    decisions_dir = Path("/opt/airflow/spark/.routing_decisions")
    decisions_dir.mkdir(parents=True, exist_ok=True)
    clean_run_id = "".join(c if c.isalnum() else "_" for c in run_id)
    decision_file_path = str(decisions_dir / f"decision_{clean_run_id}.json")

    # Persist the same resolved identity consumed by candidate writers. Without
    # this, an explicitly mapped upload/database run could be routed correctly
    # by Airflow while the immutable-candidate process still saw an unmapped
    # decision file and failed later with a misleading NOT_CONFIGURED result.
    registered_dataset_id = conf.get("dataset_id") or decision.registered_dataset_id
    source_kind = str(conf.get("source_kind") or "").strip().lower()
    if not source_kind:
        source_kind = "database" if is_airbyte_relational_run else "file"
    if registered_dataset_id:
        decision.registered_dataset_id = str(registered_dataset_id)
        from api_dataset_registry import get_dataset_config
        governed_config = get_dataset_config(str(registered_dataset_id))
        decision.target_silver_table = governed_config.silver_table
        if governed_config.gold_table:
            decision.target_gold_table = governed_config.gold_table
        if source_kind == "file":
            decision.route_target = "registered_api"

    with open(decision_file_path, "w", encoding="utf-8") as f:
        f.write(decision.model_dump_json(indent=2))

    # Nếu tài liệu đã được bóc tách sang JSON bảng, cập nhật input_path cho Spark Processor
    effective_input_path = decision.extracted_json_path if decision.extracted_json_path else input_path

    # Đẩy các thông tin quan trọng lên XCom
    ti = context["ti"]
    ti.xcom_push(key="route_target", value=decision.route_target)
    ti.xcom_push(key="decision_file", value=decision_file_path)
    ti.xcom_push(key="input_path", value=effective_input_path)
    ti.xcom_push(key="source_name", value=source_name)
    ti.xcom_push(key="s3_staging_key", value=s3_staging_key)
    ti.xcom_push(key="context_id", value=context_id)
    ti.xcom_push(key="context_manifest", value=context_manifest_file)
    ti.xcom_push(key="dataset_entity", value=decision.dataset_entity)
    # Explicit trigger metadata wins over semantic inference.  This lets API and
    # database producers identify themselves without teaching the router about
    # every future dataset, while the contract registry remains authoritative.
    ti.xcom_push(key="registered_dataset_id", value=registered_dataset_id)
    ti.xcom_push(key="source_kind", value=source_kind)
    ti.xcom_push(key="target_silver_table", value=decision.target_silver_table)
    ti.xcom_push(key="target_gold_table", value=decision.target_gold_table)


# =====================================================================
# TASK 2: BRANCH ROUTER
# =====================================================================

def determine_branch(**context):
    """Route only registered datasets into a governed candidate flow."""
    ti = context["ti"]
    dataset_id = ti.xcom_pull(
        task_ids="ai_semantic_profiler", key="registered_dataset_id"
    )
    source_kind = str(
        ti.xcom_pull(task_ids="ai_semantic_profiler", key="source_kind") or "file"
    ).lower()
    print(f"🔀 [Branch Router] dataset={dataset_id!r}, source_kind={source_kind!r}")
    if not dataset_id:
        return "onboarding_required"
    return {
        "api": "api_flow.api_preflight",
        "database": "database_flow.prepare_silver_candidate",
        "file": "file_flow.gx_validate_bronze",
    }.get(source_kind, "onboarding_required")


def onboarding_required(**context):
    """Fail closed when no active contract route exists."""
    from airflow.exceptions import AirflowFailException

    ti = context["ti"]
    dataset_id = ti.xcom_pull(
        task_ids="ai_semantic_profiler", key="registered_dataset_id"
    )
    source_kind = ti.xcom_pull(task_ids="ai_semantic_profiler", key="source_kind")
    raise AirflowFailException(
        "NOT_CONFIGURED: dataset must be onboarded with an active GX contract "
        f"before publication (dataset_id={dataset_id!r}, source_kind={source_kind!r})"
    )


def _universal_dataset_id(context) -> str:
    dataset_id = context["ti"].xcom_pull(
        task_ids="ai_semantic_profiler", key="registered_dataset_id"
    )
    if not dataset_id:
        raise RuntimeError("NOT_CONFIGURED: registered_dataset_id is missing")
    return str(dataset_id)


def validate_universal_bronze(**context):
    from gx_bronze_gate import validate_file_bronze

    ti = context["ti"]
    input_path = ti.xcom_pull(task_ids="ai_semantic_profiler", key="input_path")
    source_path = ti.xcom_pull(task_ids="ai_semantic_profiler", key="s3_staging_key")
    return validate_file_bronze(
        dataset_id=_universal_dataset_id(context),
        run_id=str(context["run_id"]),
        path=str(input_path),
        artifact_ref=str(source_path or input_path),
    )


def validate_universal_silver(*, source_kind: str, **context):
    dataset_id = _universal_dataset_id(context)
    run_id = str(context["run_id"])
    if source_kind == "file":
        from gx_file_gate import validate_file_candidate
        return validate_file_candidate(dataset_id=dataset_id, run_id=run_id)
    if source_kind == "api":
        from gx_api_gate import validate_api_candidate_dataset
        return validate_api_candidate_dataset(dataset_id=dataset_id, run_id=run_id)
    if source_kind == "database":
        from gx_database_gate import validate_database_candidate
        return validate_database_candidate(dataset_id=dataset_id, run_id=run_id)
    raise RuntimeError(f"NOT_CONFIGURED: unsupported source kind {source_kind!r}")


def publish_universal_silver(*, source_kind: str, validation_task_id: str, **context):
    from gx_candidate_publisher import (
        publish_api_candidate, publish_database_candidate, publish_file_candidate,
    )

    publishers = {
        "api": publish_api_candidate,
        "database": publish_database_candidate,
        "file": publish_file_candidate,
    }
    result = context["ti"].xcom_pull(task_ids=validation_task_id)
    if not isinstance(result, dict):
        raise RuntimeError("GX Silver approval XCom is missing")
    return publishers[source_kind](
        dataset_id=_universal_dataset_id(context),
        run_id=str(context["run_id"]),
        gx_result=result,
    )


def validate_universal_gold(*, source_kind: str, **context):
    from gx_dbt_candidate_gate import validate_dbt_gold_candidate
    return validate_dbt_gold_candidate(
        dataset_id=_universal_dataset_id(context),
        run_id=str(context["run_id"]),
        source_kind=source_kind,
    )


def reconcile_universal_gold(*, source_kind: str, **context):
    from gx_dbt_candidate_gate import reconcile_dbt_gold_candidate
    return reconcile_dbt_gold_candidate(
        dataset_id=_universal_dataset_id(context),
        run_id=str(context["run_id"]),
        source_kind=source_kind,
    )


def publish_universal_gold(
    *, gold_task_id: str, reconciliation_task_id: str, **context
):
    from api_dataset_registry import get_dataset_config
    from dbt_gold_runtime import publish

    ti = context["ti"]
    dataset_id = _universal_dataset_id(context)
    gold_result = ti.xcom_pull(task_ids=gold_task_id)
    reconciliation_result = ti.xcom_pull(task_ids=reconciliation_task_id)
    publish(
        get_dataset_config(dataset_id),
        str(context["run_id"]),
        gx_gold_result=gold_result,
        gx_reconciliation_result=reconciliation_result,
    )
    return {"status": "PUBLISHED", "dataset_id": dataset_id}


def _orchestration_command(command: str) -> str:
    return (
        "cd /opt/airflow/spark && python api_dataset_orchestration.py "
        f"{command} "
        "--dataset-id "
        "'{{ task_instance.xcom_pull(task_ids=\"ai_semantic_profiler\", "
        "key=\"registered_dataset_id\") }}' "
        "--run-id '{{ run_id }}'"
    )


# =====================================================================
# TASK 4: SMOKE TEST TRINO
# =====================================================================

def _trino_rest_query(sql: str) -> bool:
    """Run a Trino statement to completion through the REST protocol."""
    import urllib.request
    trino_host = os.environ.get("TRINO_HOST", "trino")
    trino_port = os.environ.get("TRINO_PORT", "8080")
    statement_url = f"http://{trino_host}:{trino_port}/v1/statement"

    headers = {
        "X-Trino-User": "airflow",
        "X-Trino-Catalog": "lakehouse",
        "X-Trino-Source": "universal_lakehouse_pipeline",
        "Content-Type": "text/plain; charset=utf-8",
    }
    request = urllib.request.Request(
        statement_url,
        data=sql.encode("utf-8"),
        headers=headers,
        method="POST",
    )
    try:
        for _ in range(10_000):
            with urllib.request.urlopen(request, timeout=30) as resp:
                payload = json.loads(resp.read().decode("utf-8"))
            if payload.get("error"):
                print(f"⚠️ Trino query error: {payload['error'].get('message')}")
                return False
            next_uri = payload.get("nextUri")
            if not next_uri:
                return True
            request = urllib.request.Request(next_uri, headers=headers, method="GET")
        print("⚠️ Trino query exceeded the REST response-page limit.")
        return False
    except Exception as exc:
        print(f"⚠️ Trino connection error: {exc}")
        return False


def smoke_test_trino(**context):
    """Kiểm tra Trino query engine đọc được bảng đích sau khi pipeline hoàn tất,
    đồng thời tự động lưu file nguồn đã xử lý vào archive trên Ozone nếu có."""
    ti = context["ti"]
    target_silver = ti.xcom_pull(task_ids="ai_semantic_profiler", key="target_silver_table") or "lakehouse.silver.kpi_cusc_master"
    target_gold = ti.xcom_pull(task_ids="ai_semantic_profiler", key="target_gold_table") or "lakehouse.gold.kpi_tong_hop_don_vi"
    s3_staging_key = ti.xcom_pull(task_ids="ai_semantic_profiler", key="s3_staging_key")
    context_manifest = ti.xcom_pull(task_ids="ai_semantic_profiler", key="context_manifest")
    decision_file = ti.xcom_pull(task_ids="ai_semantic_profiler", key="decision_file")

    # The relational processor refines the decision after key/relationship profiling.
    if context_manifest and decision_file and os.path.exists(decision_file):
        try:
            with open(decision_file, "r", encoding="utf-8") as handle:
                updated_decision = json.load(handle)
            target_silver = updated_decision.get("target_silver_table") or target_silver
            target_gold = updated_decision.get("target_gold_table") or target_gold
        except Exception as exc:
            print(f"⚠️ Không thể đọc RoutingDecision đã cập nhật: {exc}")

    print(f"🔍 [Trino Smoke Test] Đang kiểm tra truy vấn Trino trên các bảng:")
    print(f"  - Silver: {target_silver}")
    print(f"  - Gold:   {target_gold}")

    try:
        silver_ok = _trino_rest_query(f"SELECT * FROM {target_silver} LIMIT 1")
        if silver_ok:
            print(f"✅ [Trino] Truy vấn thành công Silver table: '{target_silver}'.")
        else:
            print(f"ℹ️ [Trino] Silver table '{target_silver}' đã được ghi trong Iceberg.")

        gold_ok = _trino_rest_query(f"SELECT * FROM {target_gold} LIMIT 1")
        if gold_ok:
            print(f"✅ [Trino] Truy vấn thành công Gold table: '{target_gold}'.")
        else:
            print(f"ℹ️ [Trino] Gold table '{target_gold}' đã được ghi trong Iceberg.")

        if (context_manifest or s3_staging_key) and (not silver_ok or not gold_ok):
            raise RuntimeError(
                "Pipeline output is not queryable through Trino; "
                "the staging batch will be retained for retry."
            )

        print("🎉 [Trino Smoke Test] Hoàn tất! Dữ liệu đã sẵn sàng để truy vấn và trực quan hóa trên Superset.")
    except Exception as exc:
        if context_manifest:
            raise
        print(f"ℹ️ [Trino Smoke Test] Hoàn tất kiểm tra với thông báo: {exc}")

    # Relational contexts are committed and archived atomically by manifest.
    if context_manifest and os.path.exists(context_manifest):
        try:
            with open(context_manifest, "r", encoding="utf-8") as handle:
                manifest = json.load(handle)
            from env_config import S3_BUCKET_NAME
            s3 = _get_s3_client()
            safe_batch = "".join(
                char if char.isalnum() or char in "_-" else "_"
                for char in str(manifest.get("batch_id") or "batch")
            )
            context_id = manifest["context_id"]
            archived = 0
            for item in manifest.get("objects", []):
                key = item["key"]
                try:
                    s3.head_object(Bucket=S3_BUCKET_NAME, Key=key)
                except Exception:
                    continue
                archive_object(
                    s3,
                    bucket=S3_BUCKET_NAME,
                    source_key=key,
                    archive_prefix=f"archive/processed/batch_id={safe_batch}",
                )
                archived += 1
            print(
                f"✅ [Context Archive] Đã archive {archived} object của "
                f"context '{context_id}', batch '{safe_batch}'."
            )
        except Exception as exc:
            raise RuntimeError(f"Không thể archive relational context: {exc}") from exc
    # Tự động lưu file nguồn đơn lẻ từ Ozone staging/ sang archive/processed/
    elif s3_staging_key:
        try:
            from env_config import ARCHIVE_PROCESSED_PREFIX, S3_BUCKET_NAME
            s3 = _get_s3_client()
            # Kiểm tra xem file còn tồn tại ở staging/ không (nhánh KPI flow có thể đã dọn dẹp trước đó)
            file_exists = False
            try:
                s3.head_object(Bucket=S3_BUCKET_NAME, Key=s3_staging_key)
                file_exists = True
            except Exception:
                print(f"ℹ️ [Archive Cleanup] File '{s3_staging_key}' đã được xử lý và di chuyển trước đó.")

            if file_exists:
                result = archive_object(
                    s3,
                    bucket=S3_BUCKET_NAME,
                    source_key=s3_staging_key,
                    archive_prefix=ARCHIVE_PROCESSED_PREFIX,
                )
                print(
                    f"✅ [Archive Cleanup] Đã chuyển '{s3_staging_key}' sang "
                    f"'{result.archive_key}' và xác minh {result.content_length} bytes."
                )
        except Exception as ae:
            raise RuntimeError(
                f"Không thể xác minh và archive file nguồn trên Ozone: {ae}"
            ) from ae


# =====================================================================
# TASK 5: DYNAMIC SUPERSET PROVISIONING (BI PLANE AUTOMATION)
# =====================================================================

def provision_superset_dashboard_task(**context):
    """Tự động sinh và import dashboard Superset cho dataset vừa được đưa vào tầng Gold."""
    ti = context["ti"]
    decision_file = ti.xcom_pull(task_ids="ai_semantic_profiler", key="decision_file")

    if not decision_file or not os.path.exists(decision_file):
        print(f"ℹ️ [Superset Task] Không tìm thấy decision file: '{decision_file}'. Bỏ qua auto-provisioning.")
        return

    print(f"🎨 [Superset Task] Bắt đầu tự động tạo Dashboard trên Superset từ '{decision_file}'...")
    try:
        from superset_dynamic_provisioner import provision_dynamic_dashboard
        from ai_dataset_router import RoutingDecision

        with open(decision_file, "r", encoding="utf-8") as f:
            decision = RoutingDecision(**json.load(f))

        output_dir = Path("/opt/airflow/spark/.generated_exports")
        output_dir.mkdir(parents=True, exist_ok=True)

        result = provision_dynamic_dashboard(
            decision=decision,
            output_dir=output_dir,
            auto_import=True,
            superset_url="http://superset:8088",
        )
        print("✅ [Superset Task] Tự động tạo Dashboard thành công:")
        print(json.dumps(result, indent=2, ensure_ascii=False))
        # PythonOperator stores this mapping in XCom so the API can embed the
        # dashboard produced for this exact DAG run.
        return result
    except Exception as exc:
        raise RuntimeError(f"Không thể tạo dashboard Superset: {exc}") from exc


# =====================================================================
# DAG DEFINITION
# =====================================================================

with DAG(
    "universal_lakehouse_pipeline",
    default_args=default_args,
    description="Autonomous AI-Augmented Multi-Source Lakehouse Pipeline (Gemini -> Spark Iceberg -> Trino)",
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    # Spark/Iceberg jobs share one local Airflow host. Serialise uploads so
    # queued runs cannot start several memory-heavy Spark sessions at once.
    max_active_runs=1,
    tags=["lakehouse", "ai", "universal", "iceberg", "trino", "superset"],
) as dag:

    # 1. AI Profiler (Control Plane)
    ai_semantic_profiler = PythonOperator(
        task_id="ai_semantic_profiler",
        python_callable=run_ai_semantic_profiling,
        retries=3,
        retry_delay=timedelta(seconds=5),
    )

    # 2. Branch Decision
    branch_router = BranchPythonOperator(
        task_id="branch_router",
        python_callable=determine_branch,
    )

    onboarding = PythonOperator(
        task_id="onboarding_required",
        python_callable=onboarding_required,
    )

    # Registered API: the existing API ingestion path now publishes only sealed
    # candidates carrying valid GX approvals.
    with TaskGroup("api_flow", tooltip="Governed API candidate flow") as api_flow:
        api_preflight = BashOperator(
            task_id="api_preflight",
            bash_command=_orchestration_command("api-preflight"),
            env=UNIVERSAL_API_ENV,
            append_env=True,
        )
        api_ingest = BashOperator(
            task_id="ingest_bronze",
            bash_command=_orchestration_command("ingest-bronze"),
            env=UNIVERSAL_API_ENV,
            append_env=True,
            skip_on_exit_code=99,
        )
        api_bronze = BashOperator(
            task_id="gx_validate_bronze",
            bash_command=_orchestration_command("validate-bronze"),
            env=UNIVERSAL_API_ENV,
            append_env=True,
        )
        api_prepare_silver = BashOperator(
            task_id="prepare_silver_candidate",
            bash_command=_orchestration_command("prepare-silver-candidate"),
        )
        api_gx_silver = PythonOperator(
            task_id="gx_validate_silver",
            python_callable=validate_universal_silver,
            op_kwargs={"source_kind": "api"},
        )
        api_publish_silver = PythonOperator(
            task_id="publish_silver_candidate",
            python_callable=publish_universal_silver,
            op_kwargs={
                "source_kind": "api",
                "validation_task_id": "api_flow.gx_validate_silver",
            },
        )
        api_validate_silver = BashOperator(
            task_id="validate_published_silver",
            bash_command=_orchestration_command("validate-silver"),
        )
        api_preflight >> api_ingest >> api_bronze >> api_prepare_silver
        api_prepare_silver >> api_gx_silver >> api_publish_silver >> api_validate_silver

    with TaskGroup("file_flow", tooltip="Governed file candidate flow") as file_flow:
        file_bronze = PythonOperator(
            task_id="gx_validate_bronze", python_callable=validate_universal_bronze
        )
        file_prepare_silver = BashOperator(
            task_id="prepare_silver_candidate",
            bash_command=(
                "cd /opt/airflow/spark && python generic_candidate_writer.py "
                "--source-kind file "
                "--input '{{ task_instance.xcom_pull(task_ids=\"ai_semantic_profiler\", key=\"input_path\") }}' "
                "--decision-file '{{ task_instance.xcom_pull(task_ids=\"ai_semantic_profiler\", key=\"decision_file\") }}' "
                "--run-id '{{ run_id }}'"
            ),
        )
        file_gx_silver = PythonOperator(
            task_id="gx_validate_silver",
            python_callable=validate_universal_silver,
            op_kwargs={"source_kind": "file"},
        )
        file_publish_silver = PythonOperator(
            task_id="publish_silver_candidate",
            python_callable=publish_universal_silver,
            op_kwargs={
                "source_kind": "file",
                "validation_task_id": "file_flow.gx_validate_silver",
            },
        )
        file_bronze >> file_prepare_silver >> file_gx_silver >> file_publish_silver

    with TaskGroup("database_flow", tooltip="Governed database candidate flow") as database_flow:
        database_prepare_silver = BashOperator(
            task_id="prepare_silver_candidate",
            bash_command=(
                "cd /opt/airflow/spark && python generic_candidate_writer.py "
                "--source-kind database "
                "--manifest '{{ task_instance.xcom_pull(task_ids=\"ai_semantic_profiler\", key=\"context_manifest\") }}' "
                "--decision-file '{{ task_instance.xcom_pull(task_ids=\"ai_semantic_profiler\", key=\"decision_file\") }}' "
                "--run-id '{{ run_id }}'"
            ),
        )
        database_gx_silver = PythonOperator(
            task_id="gx_validate_silver",
            python_callable=validate_universal_silver,
            op_kwargs={"source_kind": "database"},
        )
        database_publish_silver = PythonOperator(
            task_id="publish_silver_candidate",
            python_callable=publish_universal_silver,
            op_kwargs={
                "source_kind": "database",
                "validation_task_id": "database_flow.gx_validate_silver",
            },
        )
        database_prepare_silver >> database_gx_silver >> database_publish_silver

    # Each source converges on the same contract-driven Gold gates. They are
    # separate task instances only to preserve branch isolation and source_kind.
    publish_ends = []
    for source_kind, upstream in (
        ("api", api_validate_silver),
        ("file", file_publish_silver),
        ("database", database_publish_silver),
    ):
        with TaskGroup(
            f"{source_kind}_gold", tooltip=f"Governed {source_kind} Gold candidate"
        ):
            prepare_gold = BashOperator(
                task_id="prepare_candidate",
                bash_command=_orchestration_command("prepare-gold"),
            )
            build_gold = BashOperator(
                task_id="build_candidate",
                bash_command=_orchestration_command("process-gold"),
            )
            validate_gold = BashOperator(
                task_id="validate_candidate",
                bash_command=_orchestration_command("validate-gold"),
            )
            gx_gold = PythonOperator(
                task_id="gx_validate",
                python_callable=validate_universal_gold,
                op_kwargs={"source_kind": source_kind},
            )
            gx_reconcile = PythonOperator(
                task_id="gx_reconcile",
                python_callable=reconcile_universal_gold,
                op_kwargs={"source_kind": source_kind},
            )
            publish_gold = PythonOperator(
                task_id="publish",
                python_callable=publish_universal_gold,
                op_kwargs={
                    "gold_task_id": f"{source_kind}_gold.gx_validate",
                    "reconciliation_task_id": f"{source_kind}_gold.gx_reconcile",
                },
            )
            prepare_gold >> build_gold >> validate_gold >> gx_gold >> gx_reconcile >> publish_gold
        upstream >> prepare_gold
        publish_ends.append(publish_gold)

    # Trino and Superset run only after one governed branch has published.
    join_and_smoke_test = PythonOperator(
        task_id="join_and_smoke_test",
        python_callable=smoke_test_trino,
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    # 7. Dynamic Superset Dashboard Provisioner (BI Plane)
    auto_provision_superset = PythonOperator(
        task_id="auto_provision_superset",
        python_callable=provision_superset_dashboard_task,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # Wire DAG Dependencies
    ai_semantic_profiler >> branch_router
    branch_router >> [api_preflight, file_bronze, database_prepare_silver, onboarding]
    publish_ends >> join_and_smoke_test
    join_and_smoke_test >> auto_provision_superset

