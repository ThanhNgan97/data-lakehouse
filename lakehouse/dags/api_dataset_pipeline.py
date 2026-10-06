import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator


SPARK_DIR = "/opt/airflow/spark"

if SPARK_DIR not in sys.path:
    sys.path.insert(0, SPARK_DIR)

from api_dataset_registry import DATASETS


REGISTERED_DATASET_IDS = sorted(DATASETS)


default_args = {
    "owner": "lakehouse",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
}


COMMON_ENV = {
    "DATASET_ID": (
        "{{ dag_run.conf.get('dataset_id', '') "
        "if dag_run else '' }}"
    ),
    "AIRFLOW_RUN_ID": "{{ run_id }}",
}


def runner_command(command: str) -> str:
    return (
        "cd /opt/airflow/spark && "
        "python api_dataset_orchestration.py "
        f"{command} "
        '--dataset-id "$DATASET_ID" '
        '--run-id "$AIRFLOW_RUN_ID"'
    )


with DAG(
    "api_dataset_pipeline",
    default_args=default_args,
    description=(
        "Generic registered API dataset orchestration: "
        "API -> Bronze -> Silver -> Gold -> Trino"
    ),
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=[
        "lakehouse",
        "api",
        "multi-dataset",
    ],
    params={
        "dataset_id": Param(
            type="string",
            enum=REGISTERED_DATASET_IDS,
            title="Dataset",
            description=(
                "Choose the registered API dataset to process. "
                "The selected value is validated before the DAG run is created."
            ),
        ),
    },
) as dag:
    api_preflight = BashOperator(
        task_id="api_preflight",
        bash_command=runner_command(
            "api-preflight"
        ),
        env=COMMON_ENV,
        append_env=True,
    )

    ingest_bronze = BashOperator(
        task_id="ingest_bronze",
        bash_command=runner_command(
            "ingest-bronze"
        ),
        env=COMMON_ENV,
        append_env=True,
        skip_on_exit_code=99,
    )

    validate_bronze = BashOperator(
        task_id="validate_bronze",
        bash_command=runner_command(
            "validate-bronze"
        ),
        env=COMMON_ENV,
        append_env=True,
    )

    process_silver = BashOperator(
        task_id="process_silver",
        bash_command=runner_command(
            "process-silver"
        ),
        env=COMMON_ENV,
        append_env=True,
    )

    validate_silver = BashOperator(
        task_id="validate_silver",
        bash_command=runner_command(
            "validate-silver"
        ),
        env=COMMON_ENV,
        append_env=True,
    )

    process_gold = BashOperator(
        task_id="process_gold",
        bash_command=runner_command(
            "process-gold"
        ),
        env=COMMON_ENV,
        append_env=True,
    )

    validate_gold = BashOperator(
        task_id="validate_gold",
        bash_command=runner_command(
            "validate-gold"
        ),
        env=COMMON_ENV,
        append_env=True,
    )

    trino_smoke_test = BashOperator(
        task_id="trino_smoke_test",
        bash_command=runner_command(
            "trino-smoke-test"
        ),
        env=COMMON_ENV,
        append_env=True,
    )

    (
        api_preflight
        >> ingest_bronze
        >> validate_bronze
        >> process_silver
        >> validate_silver
        >> process_gold
        >> validate_gold
        >> trino_smoke_test
    )
