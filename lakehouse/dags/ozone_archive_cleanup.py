"""Retention cleanup for source-file archives in Apache Ozone."""

from __future__ import annotations

import os
import sys
from datetime import datetime

import boto3
from airflow import DAG
from airflow.operators.python import PythonOperator
from botocore.config import Config

SPARK_DIR = "/opt/airflow/spark"
if SPARK_DIR not in sys.path:
    sys.path.insert(0, SPARK_DIR)

from ozone_object_lifecycle import delete_objects, list_expired_objects


def _enabled(name: str, default: str) -> bool:
    return os.environ.get(name, default).strip().lower() == "true"


def _client():
    return boto3.client(
        "s3",
        endpoint_url=os.environ["S3_ENDPOINT"],
        aws_access_key_id=os.environ["S3_ACCESS_KEY"],
        aws_secret_access_key=os.environ["S3_SECRET_KEY"],
        region_name=os.environ.get("S3_REGION", "us-east-1"),
        config=Config(s3={"addressing_style": "path"}),
    )


def load_config(**context):
    config = {
        "enabled": _enabled("ARCHIVE_CLEANUP_ENABLED", "false"),
        "dry_run": _enabled("ARCHIVE_CLEANUP_DRY_RUN", "true"),
        "bucket": os.environ.get("S3_BUCKET_NAME", "university-lakehouse"),
        "prefix": os.environ.get("ARCHIVE_PROCESSED_PREFIX", "archive/processed").strip("/"),
        "retention_days": int(os.environ.get("ARCHIVE_RETENTION_DAYS", "90")),
    }
    context["ti"].xcom_push(key="config", value=config)
    return config


def find_candidates(**context):
    config = context["ti"].xcom_pull(task_ids="load_config", key="config")
    candidates = []
    if config["enabled"]:
        candidates = list_expired_objects(
            _client(),
            bucket=config["bucket"],
            prefix=config["prefix"],
            retention_days=config["retention_days"],
        )
    context["ti"].xcom_push(key="candidates", value=candidates)
    return {
        "enabled": config["enabled"],
        "candidate_count": len(candidates),
        "candidate_bytes": sum(item["size"] for item in candidates),
    }


def cleanup(**context):
    ti = context["ti"]
    config = ti.xcom_pull(task_ids="load_config", key="config")
    candidates = ti.xcom_pull(task_ids="find_candidates", key="candidates") or []
    deleted = 0
    if config["enabled"] and not config["dry_run"]:
        deleted = delete_objects(
            _client(),
            bucket=config["bucket"],
            keys=[item["key"] for item in candidates],
        )
    return {
        "enabled": config["enabled"],
        "dry_run": config["dry_run"],
        "candidate_count": len(candidates),
        "deleted_count": deleted,
    }


with DAG(
    dag_id="ozone_archive_cleanup",
    description="Apply configurable retention to processed source archives in Apache Ozone",
    start_date=datetime(2026, 1, 1),
    schedule_interval="0 2 * * *",
    catchup=False,
    default_args={"owner": "lakehouse", "retries": 0},
    tags=["ozone", "archive", "retention"],
) as dag:
    config = PythonOperator(task_id="load_config", python_callable=load_config)
    candidates = PythonOperator(task_id="find_candidates", python_callable=find_candidates)
    delete = PythonOperator(task_id="delete_expired_objects", python_callable=cleanup)
    config >> candidates >> delete
