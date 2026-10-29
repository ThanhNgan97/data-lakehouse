"""Fail-closed candidate-first pipeline for governed file uploads."""

from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.exceptions import AirflowException, AirflowFailException
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator


SPARK_DIR = "/opt/airflow/spark"
if SPARK_DIR not in sys.path:
    sys.path.insert(0, SPARK_DIR)


def profile_and_resolve_contract(**context):
    from ai_dataset_router import route_from_file_path
    from env_config import S3_ACCESS_KEY, S3_BUCKET_NAME, S3_ENDPOINT, S3_SECRET_KEY
    import boto3

    conf = context["dag_run"].conf or {}
    source = str(conf.get("input_path") or "").strip()
    if not source:
        raise AirflowFailException("input_path is required")
    local = source
    if source.startswith("staging/") or source.startswith("s3://"):
        key = source.replace(f"s3://{S3_BUCKET_NAME}/", "").lstrip("/")
        local = f"/opt/airflow/spark/.staging_downloads/{context['run_id'].replace(':', '_')}/{Path(key).name}"
        Path(local).parent.mkdir(parents=True, exist_ok=True)
        boto3.client(
            "s3",
            endpoint_url=S3_ENDPOINT,
            aws_access_key_id=S3_ACCESS_KEY,
            aws_secret_access_key=S3_SECRET_KEY,
        ).download_file(S3_BUCKET_NAME, key, local)
    decision = route_from_file_path(local)
    if decision.registered_dataset_id:
        from gx_route_resolver import resolve_route

        resolve_route(
            dataset_id=decision.registered_dataset_id,
            data_layer="silver",
            source_kind="file",
            execution_engine="sql",
        )
    decision_dir = Path("/opt/airflow/spark/.routing_decisions")
    decision_dir.mkdir(parents=True, exist_ok=True)
    decision_file = decision_dir / f"file_{context['run_id'].replace(':', '_')}.json"
    decision_file.write_text(decision.model_dump_json(indent=2), encoding="utf-8")
    registered = bool(
        decision.route_target == "registered_api" and decision.registered_dataset_id
    )
    result = {
        "status": "REGISTERED" if registered else "NOT_CONFIGURED",
        "dataset_id": decision.registered_dataset_id,
        "input_path": local,
        "decision_file": str(decision_file),
        "source_path": source,
    }
    return result


def branch_contract_status(**context):
    result = context["ti"].xcom_pull(task_ids="profile_and_resolve_contract") or {}
    return "prepare_file_silver_candidate" if result.get("status") == "REGISTERED" else "onboarding_required"


def onboarding_required(**context):
    profile = context["ti"].xcom_pull(task_ids="profile_and_resolve_contract") or {}
    return {
        "status": "NOT_CONFIGURED",
        "contract_status": "MISSING",
        "merge_allowed": None,
        "reason": "Uploaded file does not map unambiguously to an active dataset contract",
        "source_path": profile.get("source_path"),
    }


def validate_file_candidate(**context):
    from gx_airflow_adapter import GXAirflowError, NON_RETRYABLE
    from gx_file_gate import validate_file_candidate

    profile = context["ti"].xcom_pull(task_ids="profile_and_resolve_contract") or {}
    try:
        return validate_file_candidate(
            dataset_id=str(profile["dataset_id"]), run_id=str(context["run_id"])
        )
    except GXAirflowError as exc:
        if exc.code in NON_RETRYABLE or not exc.retryable:
            raise AirflowFailException(str(exc)) from exc
        raise AirflowException(str(exc)) from exc


def validate_file_bronze_task(**context):
    from gx_airflow_adapter import GXAirflowError, NON_RETRYABLE
    from gx_bronze_gate import validate_file_bronze

    profile = context["ti"].xcom_pull(task_ids="profile_and_resolve_contract") or {}
    try:
        return validate_file_bronze(
            dataset_id=str(profile["dataset_id"]),
            run_id=str(context["run_id"]),
            path=str(profile["input_path"]),
            artifact_ref=str(profile.get("source_path") or profile["input_path"]),
        )
    except GXAirflowError as exc:
        if exc.code in NON_RETRYABLE or not exc.retryable:
            raise AirflowFailException(str(exc)) from exc
        raise AirflowException(str(exc)) from exc


def publish_file_candidate_task(**context):
    from gx_candidate_publisher import CandidatePublishError, publish_file_candidate

    profile = context["ti"].xcom_pull(task_ids="profile_and_resolve_contract") or {}
    result = context["ti"].xcom_pull(task_ids="gx_validate_file_candidate")
    try:
        return publish_file_candidate(
            dataset_id=str(profile["dataset_id"]),
            run_id=str(context["run_id"]),
            gx_result=result,
        )
    except CandidatePublishError as exc:
        raise AirflowFailException(str(exc)) from exc


def _profile(context):
    return context["ti"].xcom_pull(task_ids="profile_and_resolve_contract") or {}


def validate_gold_candidate(**context):
    from gx_dbt_candidate_gate import validate_dbt_gold_candidate

    return validate_dbt_gold_candidate(
        dataset_id=str(_profile(context)["dataset_id"]),
        run_id=str(context["run_id"]),
        source_kind="file",
    )


def reconcile_gold_candidate(**context):
    from gx_dbt_candidate_gate import reconcile_dbt_gold_candidate

    return reconcile_dbt_gold_candidate(
        dataset_id=str(_profile(context)["dataset_id"]),
        run_id=str(context["run_id"]),
        source_kind="file",
    )


def publish_gold_candidate(**context):
    from api_dataset_registry import get_dataset_config
    from dbt_gold_runtime import publish

    dataset_id = str(_profile(context)["dataset_id"])
    publish(
        get_dataset_config(dataset_id),
        str(context["run_id"]),
        gx_gold_result=context["ti"].xcom_pull(task_ids="gx_validate_gold_candidate"),
        gx_reconciliation_result=context["ti"].xcom_pull(task_ids="gx_reconcile_candidate"),
    )
    return {"status": "PUBLISHED", "dataset_id": dataset_id}


def runner_command(command: str) -> str:
    return (
        "cd /opt/airflow/spark && python api_dataset_orchestration.py "
        f"{command} "
        "--dataset-id '{{ ti.xcom_pull(task_ids=\"profile_and_resolve_contract\")[\"dataset_id\"] }}' "
        "--run-id '{{ run_id }}'"
    )


def provision_superset(**context):
    from ai_dataset_router import RoutingDecision
    from superset_dynamic_provisioner import provision_dynamic_dashboard

    decision = RoutingDecision(
        **json.loads(Path(_profile(context)["decision_file"]).read_text(encoding="utf-8"))
    )
    output_dir = Path("/opt/airflow/spark/.generated_exports")
    output_dir.mkdir(parents=True, exist_ok=True)
    return provision_dynamic_dashboard(
        decision=decision,
        output_dir=output_dir,
        auto_import=True,
        superset_url="http://superset:8088",
    )


def archive_source(**context):
    from env_config import S3_BUCKET_NAME
    from ozone_object_lifecycle import archive_object

    source = str(_profile(context).get("source_path") or "")
    key = source.replace(f"s3://{S3_BUCKET_NAME}/", "").lstrip("/")
    if not key.startswith("staging/"):
        return {"status": "NOT_APPLICABLE", "source": source}
    result = archive_object(
        __import__("boto3").client(
            "s3",
            endpoint_url=os.getenv("S3_ENDPOINT", "http://ozone-s3g:9878"),
            aws_access_key_id=os.getenv("S3_ACCESS_KEY", "lakehouse"),
            aws_secret_access_key=os.getenv("S3_SECRET_KEY", "lakehouse-secret"),
        ),
        bucket=S3_BUCKET_NAME,
        source_key=key,
        archive_prefix=os.getenv("ARCHIVE_PROCESSED_PREFIX", "archive/processed"),
    )
    return {"status": "ARCHIVED", "archive_key": result.archive_key}


with DAG(
    "file_upload_candidate_pipeline",
    default_args={
        "owner": "lakehouse",
        "retries": 0,
        "retry_delay": timedelta(minutes=1),
    },
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["lakehouse", "file", "gx", "candidate"],
) as dag:
    profile = PythonOperator(
        task_id="profile_and_resolve_contract",
        python_callable=profile_and_resolve_contract,
    )
    route = BranchPythonOperator(
        task_id="branch_contract_status", python_callable=branch_contract_status
    )
    prepare = BashOperator(
        task_id="prepare_file_silver_candidate",
        bash_command=(
            "cd /opt/airflow/spark && python file_candidate_writer.py "
            "--input '{{ ti.xcom_pull(task_ids=\"profile_and_resolve_contract\")[\"input_path\"] }}' "
            "--decision-file '{{ ti.xcom_pull(task_ids=\"profile_and_resolve_contract\")[\"decision_file\"] }}' "
            "--run-id '{{ run_id }}'"
        ),
    )
    gx_bronze = PythonOperator(
        task_id="gx_validate_bronze",
        python_callable=validate_file_bronze_task,
        retries=1,
    )
    gx = PythonOperator(
        task_id="gx_validate_file_candidate",
        python_callable=validate_file_candidate,
        retries=1,
    )
    publish = PythonOperator(
        task_id="publish_file_silver_candidate",
        python_callable=publish_file_candidate_task,
    )
    onboarding = PythonOperator(
        task_id="onboarding_required", python_callable=onboarding_required
    )
    validate_silver = BashOperator(
        task_id="validate_published_silver",
        bash_command=runner_command("validate-silver"),
    )
    prepare_gold = BashOperator(
        task_id="prepare_gold_candidate", bash_command=runner_command("prepare-gold")
    )
    build_gold = BashOperator(
        task_id="build_gold_candidate", bash_command=runner_command("process-gold")
    )
    validate_dbt = BashOperator(
        task_id="validate_dbt_candidate", bash_command=runner_command("validate-gold")
    )
    gx_gold = PythonOperator(
        task_id="gx_validate_gold_candidate", python_callable=validate_gold_candidate
    )
    gx_reconcile = PythonOperator(
        task_id="gx_reconcile_candidate", python_callable=reconcile_gold_candidate
    )
    publish_gold = PythonOperator(
        task_id="publish_gold_candidate", python_callable=publish_gold_candidate
    )
    trino = BashOperator(
        task_id="trino_smoke_test", bash_command=runner_command("trino-smoke-test")
    )
    superset = PythonOperator(
        task_id="provision_superset", python_callable=provision_superset
    )
    archive = PythonOperator(task_id="archive_source", python_callable=archive_source)

    profile >> route
    (
        route
        >> gx_bronze
        >> prepare
        >> gx
        >> publish
        >> validate_silver
        >> prepare_gold
        >> build_gold
        >> validate_dbt
        >> gx_gold
        >> gx_reconcile
        >> publish_gold
        >> trino
        >> superset
        >> archive
    )
    route >> onboarding
