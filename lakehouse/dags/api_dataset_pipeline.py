import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


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
    "SOURCE_API_URL": (
        "{{ dag_run.conf.get('source_api_url', '') "
        "if dag_run else '' }}"
    ),
    "SOURCE_API_KEY": (
        "{{ dag_run.conf.get('source_api_key', '') "
        "if dag_run else '' }}"
    ),
}


def runner_command(command: str) -> str:
    return (
        "cd /opt/airflow/spark && "
        "python api_dataset_orchestration.py "
        f"{command} "
        '--dataset-id "$DATASET_ID" '
        '--run-id "$AIRFLOW_RUN_ID"'
    )


def run_gx_api_quality_gate(**context):
    """Validate the selected API dataset's current Silver snapshot."""
    from airflow.exceptions import AirflowException, AirflowFailException
    from gx_airflow_adapter import GXAirflowError, NON_RETRYABLE
    from gx_api_gate import validate_api_dataset

    dag_run = context.get("dag_run")
    dataset_id = dag_run.conf.get("dataset_id") if dag_run else None
    dataset_id = dataset_id or context.get("params", {}).get("dataset_id")
    if not dataset_id:
        raise ValueError("dataset_id is required for the GX API gate")
    try:
        return validate_api_dataset(
            dataset_id=str(dataset_id),
            run_id=str(context.get("run_id") or ""),
        )
    except GXAirflowError as exc:
        if exc.code in NON_RETRYABLE or not exc.retryable:
            raise AirflowFailException(str(exc)) from exc
        raise AirflowException(str(exc)) from exc


def run_gx_gold_quality_gate(**context):
    """Validate the Gold snapshot created by the dbt CAS publication."""
    from airflow.exceptions import AirflowException, AirflowFailException
    from gx_airflow_adapter import GXAirflowError, NON_RETRYABLE
    from gx_gold_gate import validate_published_gold

    dag_run = context.get("dag_run")
    dataset_id = dag_run.conf.get("dataset_id") if dag_run else None
    dataset_id = dataset_id or context.get("params", {}).get("dataset_id")
    if not dataset_id:
        raise ValueError("dataset_id is required for the GX Gold gate")
    try:
        return validate_published_gold(
            dataset_id=str(dataset_id),
            run_id=str(context.get("run_id") or ""),
            source_kind="api",
        )
    except GXAirflowError as exc:
        if exc.code in NON_RETRYABLE or not exc.retryable:
            raise AirflowFailException(str(exc)) from exc
        raise AirflowException(str(exc)) from exc


def run_gx_reconciliation_gate(**context):
    """Require Silver and Gold values/lineage to reconcile."""
    from airflow.exceptions import AirflowException, AirflowFailException
    from gx_airflow_adapter import GXAirflowError, NON_RETRYABLE
    from gx_reconciliation_gate import validate_silver_gold_reconciliation

    dag_run = context.get("dag_run")
    dataset_id = dag_run.conf.get("dataset_id") if dag_run else None
    dataset_id = dataset_id or context.get("params", {}).get("dataset_id")
    if not dataset_id:
        raise ValueError("dataset_id is required for GX reconciliation")
    try:
        return validate_silver_gold_reconciliation(
            dataset_id=str(dataset_id),
            run_id=str(context.get("run_id") or ""),
            source_kind="api",
        )
    except GXAirflowError as exc:
        if exc.code in NON_RETRYABLE or not exc.retryable:
            raise AirflowFailException(str(exc)) from exc
        raise AirflowException(str(exc)) from exc


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
    max_active_runs=1,
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

    gx_validate_silver = PythonOperator(
        task_id="gx_validate_silver",
        python_callable=run_gx_api_quality_gate,
        retries=1,
        retry_delay=timedelta(minutes=1),
    )

    prepare_gold = BashOperator(
        task_id="prepare_gold",
        bash_command=runner_command("prepare-gold"),
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

    publish_gold = BashOperator(
        task_id="publish_gold",
        bash_command=runner_command("publish-gold"),
        env=COMMON_ENV,
        append_env=True,
    )

    gx_validate_gold = PythonOperator(
        task_id="gx_validate_gold",
        python_callable=run_gx_gold_quality_gate,
        retries=1,
        retry_delay=timedelta(minutes=1),
    )

    gx_reconcile_silver_gold = PythonOperator(
        task_id="gx_reconcile_silver_gold",
        python_callable=run_gx_reconciliation_gate,
        retries=1,
        retry_delay=timedelta(minutes=1),
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
        >> gx_validate_silver
        >> prepare_gold
        >> process_gold
        >> validate_gold
        >> publish_gold
        >> gx_validate_gold
        >> gx_reconcile_silver_gold
        >> trino_smoke_test
    )
