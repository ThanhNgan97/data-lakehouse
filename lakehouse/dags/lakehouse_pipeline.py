from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import (
    SparkKubernetesOperator,
)

default_args = {
    'owner': 'lakehouse',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    'lakehouse_pipeline',
    default_args=default_args,
    description='Pipeline for Lakehouse: Bronze -> Silver -> Gold (Tối ưu hóa 4 bước)',
    schedule_interval=None, # Triggered externally via API
    start_date=datetime(2023, 1, 1),
    catchup=False,
    template_searchpath=['/opt/airflow/k8s/spark/templates'],
    tags=['lakehouse'],
) as dag:

    # Task 1: Ingest unstructured file to Parquet (Bronze) - Tối ưu đa luồng & Hybrid AI
    ingest_bronze = BashOperator(
        task_id='ingest_bronze',
        bash_command='cd /opt/airflow/spark && python spark_ingest_bronze.py --run_id {{ run_id }}',
    )

    # Task 2: Merge Parquet to Iceberg (Silver) with Nessie
    bronze_to_silver = SparkKubernetesOperator(
        task_id='bronze_to_silver',
        namespace='lakehouse-spark',
        application_file='spark-bronze-to-silver-airflow.yaml',
        kubernetes_conn_id='kubernetes_default',
        get_logs=True,
        log_events_on_failure=True,
        startup_timeout_seconds=600,
        delete_on_termination=True,
    )

    # Task 3: Aggregate Silver to Gold Data Marts
    silver_to_gold = BashOperator(
        task_id='silver_to_gold',
        bash_command='cd /opt/airflow/spark && python spark_silver_to_gold.py',
    )

    # Task 4: Predictive Analysis in Gold Layer
    predictive_analysis = BashOperator(
        task_id='predictive_analysis',
        bash_command='cd /opt/airflow/spark && python spark_predictive_analysis.py',
    )

    # Define the 4-step pipeline flow for Frontend & Airflow tracking
    ingest_bronze >> bronze_to_silver >> silver_to_gold >> predictive_analysis
