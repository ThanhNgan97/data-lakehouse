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
    "mysql_connector_sync",
    default_args=default_args,
    description="Sync selected MySQL Data Connector -> Bronze -> Silver -> Gold",
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["lakehouse", "mysql", "connector"],
) as dag:

    ingest_mysql = BashOperator(
        task_id="ingest_mysql",
        env={
            "CONNECTOR_ID": "{{ dag_run.conf.get('connector_id', '') }}",
        },
        append_env=True,
        bash_command=r'''
set -e

case "$CONNECTOR_ID" in
    ''|*[!0-9]*)
        echo "ERROR: connector_id must be a positive integer."
        exit 2
        ;;
esac

if [ "$CONNECTOR_ID" -le 0 ]; then
    echo "ERROR: connector_id must be greater than zero."
    exit 2
fi

cd /opt/airflow/spark

python spark_ingest_mysql.py \
    --connector_id "$CONNECTOR_ID" \
    --run_id "{{ run_id }}"
''',
    )

    bronze_to_silver = BashOperator(
        task_id="bronze_to_silver",
        bash_command=(
            'cd /opt/airflow/spark && '
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
        ingest_mysql
        >> bronze_to_silver
        >> silver_to_gold
        >> predictive_analysis
    )