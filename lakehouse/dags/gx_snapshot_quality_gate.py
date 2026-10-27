"""Manual snapshot-aware GX gate; XCom contains only the durable summary reference."""

import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator


SPARK_DIR = "/opt/airflow/spark"
if SPARK_DIR not in sys.path:
    sys.path.insert(0, SPARK_DIR)

from gx_airflow_adapter import airflow_quality_gate


with DAG(
    "gx_snapshot_quality_gate",
    description="Validate one immutable Nessie/Iceberg candidate through external GX",
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=4,
    default_args={"owner": "data-platform", "retries": 0},
    params={
        "validation_request": Param(
            type="object",
            title="Snapshot-aware GX validation request",
            required=[
                "dataset_id", "data_layer", "contract_version", "source_kind",
                "run_id", "nessie_branch", "snapshot_id", "nessie_commit_hash",
            ],
        )
    },
    tags=["gx", "quality-gate", "snapshot-aware"],
) as dag:
    validate_snapshot = PythonOperator(
        task_id="validate_snapshot",
        python_callable=airflow_quality_gate,
        retries=2,
        retry_delay=timedelta(seconds=30),
        retry_exponential_backoff=True,
        max_retry_delay=timedelta(minutes=5),
        execution_timeout=timedelta(minutes=10),
        do_xcom_push=True,
    )
