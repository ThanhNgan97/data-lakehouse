import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from airflow.operators.python import ShortCircuitOperator
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import (
    SparkKubernetesOperator,
)


SPARK_DIR = "/opt/airflow/spark"

if SPARK_DIR not in sys.path:
    sys.path.insert(0, SPARK_DIR)

from api_dataset_registry import DATASETS, get_dataset_config
from api_dataset_orchestration import command_api_preflight


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


def api_preflight_should_continue(**context) -> bool:
    dag_run = context.get("dag_run")
    conf = dag_run.conf if dag_run and dag_run.conf else {}
    dataset_id = conf.get("dataset_id") or context["params"].get("dataset_id")
    config = get_dataset_config(dataset_id)
    result = command_api_preflight(
        config,
        context["run_id"],
        source_url_override=conf.get("source_api_url", ""),
        source_api_key_override=conf.get("source_api_key", ""),
    )
    has_changes = result.fetched_count > 0
    print(f"API_PREFLIGHT_HAS_CHANGES={has_changes}")
    return has_changes


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
    template_searchpath=["/opt/airflow/k8s/spark/templates"],
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
    api_preflight = ShortCircuitOperator(
        task_id="api_preflight",
        python_callable=api_preflight_should_continue,
    )

    ingest_bronze = SparkKubernetesOperator(
        task_id="ingest_bronze",
        application_file="spark-api-dataset-airflow.yaml",
        params={"command": "ingest-bronze"},
        namespace="lakehouse-spark",
        kubernetes_conn_id="kubernetes_default",
        get_logs=True,
        log_events_on_failure=True,
        startup_timeout_seconds=600,
        delete_on_termination=True,
        pool="spark_k8s",
    )

    validate_bronze = BashOperator(
        task_id="validate_bronze",
        bash_command=runner_command(
            "validate-bronze"
        ),
        env=COMMON_ENV,
        append_env=True,
    )

    process_silver = SparkKubernetesOperator(
        task_id="process_silver",
        application_file="spark-api-dataset-airflow.yaml",
        params={"command": "process-silver"},
        namespace="lakehouse-spark",
        kubernetes_conn_id="kubernetes_default",
        get_logs=True,
        log_events_on_failure=True,
        startup_timeout_seconds=600,
        delete_on_termination=True,
        pool="spark_k8s",
    )

    validate_silver = SparkKubernetesOperator(
        task_id="validate_silver",
        application_file="spark-api-dataset-airflow.yaml",
        params={"command": "validate-silver"},
        namespace="lakehouse-spark",
        kubernetes_conn_id="kubernetes_default",
        get_logs=True,
        log_events_on_failure=True,
        startup_timeout_seconds=600,
        delete_on_termination=True,
        pool="spark_k8s",
    )

<<<<<<< HEAD
    process_gold = SparkKubernetesOperator(
=======
    prepare_gold = BashOperator(
        task_id="prepare_gold",
        bash_command=runner_command("prepare-gold"),
        env=COMMON_ENV,
        append_env=True,
    )

    process_gold = BashOperator(
>>>>>>> origin/develop
        task_id="process_gold",
        application_file="spark-api-dataset-airflow.yaml",
        params={"command": "process-gold"},
        namespace="lakehouse-spark",
        kubernetes_conn_id="kubernetes_default",
        get_logs=True,
        log_events_on_failure=True,
        startup_timeout_seconds=600,
        delete_on_termination=True,
        pool="spark_k8s",
    )

    validate_gold = SparkKubernetesOperator(
        task_id="validate_gold",
        application_file="spark-api-dataset-airflow.yaml",
        params={"command": "validate-gold"},
        namespace="lakehouse-spark",
        kubernetes_conn_id="kubernetes_default",
        get_logs=True,
        log_events_on_failure=True,
        startup_timeout_seconds=600,
        delete_on_termination=True,
        pool="spark_k8s",
    )

    publish_gold = BashOperator(
        task_id="publish_gold",
        bash_command=runner_command("publish-gold"),
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
        >> prepare_gold
        >> process_gold
        >> validate_gold
        >> publish_gold
        >> trino_smoke_test
    )
