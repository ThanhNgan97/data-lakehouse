from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


default_args = {
    "owner": "lakehouse",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
}


with DAG(
    "lakehouse_pipeline",
    default_args=default_args,
    description=(
        "Upload Lakehouse pipeline: "
        "Document/MySQL dump -> Bronze -> Silver -> Gold"
    ),
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["lakehouse"],
) as dag:

    # Upload ingestion is routed by dag_run.conf.
    #
    # DOCUMENT_FILE:
    #   spark_ingest_bronze.py --object_key ...
    #
    # MYSQL_DUMP:
    #   spark_ingest_mysql_dump.py --object_key ...
    #   --upload_id ... --write_bronze
    #
    # Live MySQL connector synchronization is handled by its
    # dedicated mysql_connector_sync DAG and is intentionally
    # not part of this upload DAG.
    ingest_bronze = BashOperator(
        task_id="ingest_bronze",
        bash_command=r"""
set -euo pipefail

cd /opt/airflow/spark

case "$SOURCE_TYPE" in
    DOCUMENT_FILE)
        if [ -z "$OBJECT_KEY" ]; then
            echo "ERROR: DOCUMENT_FILE requires object_key." >&2
            exit 2
        fi

        echo "Upload source type: DOCUMENT_FILE"
        echo "Object key: $OBJECT_KEY"

        python spark_ingest_bronze.py             --object_key "$OBJECT_KEY"             --run_id "$AIRFLOW_RUN_ID"
        ;;

    MYSQL_DUMP)
        if [ -z "$OBJECT_KEY" ]; then
            echo "ERROR: MYSQL_DUMP requires object_key." >&2
            exit 2
        fi

        if [ -z "$UPLOAD_ID" ]; then
            echo "ERROR: MYSQL_DUMP requires upload_id." >&2
            exit 2
        fi

        echo "Upload source type: MYSQL_DUMP"
        echo "Object key: $OBJECT_KEY"
        echo "Upload ID: $UPLOAD_ID"

        python spark_ingest_mysql_dump.py             --object_key "$OBJECT_KEY"             --upload_id "$UPLOAD_ID"             --run_id "$AIRFLOW_RUN_ID"             --write_bronze
        ;;

    *)
        echo "ERROR: Unsupported or missing source_type: $SOURCE_TYPE" >&2
        exit 2
        ;;
esac
""",
        env={
            "SOURCE_TYPE": (
                "{{ dag_run.conf.get('source_type', '') "
                "if dag_run else '' }}"
            ),
            "OBJECT_KEY": (
                "{{ dag_run.conf.get('object_key', '') "
                "if dag_run else '' }}"
            ),
            "UPLOAD_ID": (
                "{{ dag_run.conf.get('upload_id', '') "
                "if dag_run else '' }}"
            ),
            "AIRFLOW_RUN_ID": "{{ run_id }}",
        },
        append_env=True,
    )

    bronze_to_silver = BashOperator(
        task_id="bronze_to_silver",
        bash_command=(
            "cd /opt/airflow/spark && "
            'python spark_bronze_to_silver.py --run_id "{{ run_id }}"'
        ),
    )

    silver_to_gold = BashOperator(
        task_id="silver_to_gold",
        bash_command=(
            "cd /opt/airflow/spark && "
            "python spark_silver_to_gold.py"
        ),
    )

    predictive_analysis = BashOperator(
        task_id="predictive_analysis",
        bash_command=(
            "cd /opt/airflow/spark && "
            "python spark_predictive_analysis.py"
        ),
    )

    (
        ingest_bronze
        >> bronze_to_silver
        >> silver_to_gold
        >> predictive_analysis
    )
