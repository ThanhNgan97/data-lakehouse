from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

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
    description='Multi-source Lakehouse: Document/MySQL -> Bronze -> Silver -> Gold',
    schedule_interval=None, # Triggered externally via API
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['lakehouse'],
) as dag:

    # Task 1: Ingest unstructured file to Parquet (Bronze) - Tối ưu đa luồng & Hybrid AI
    ingest_bronze = BashOperator(
        task_id='ingest_bronze',
        bash_command='cd /opt/airflow/spark && python spark_ingest_bronze.py --run_id {{ run_id }}',
    )

    # Task 1b: Ingest dữ liệu KPI có cấu trúc từ MySQL qua JDBC vào Bronze
    ingest_mysql = BashOperator(
        task_id='ingest_mysql',
        bash_command='cd /opt/airflow/spark && python spark_ingest_mysql.py --connector_id 1 --run_id "{{ run_id }}"',
    )

    # Task 2: Merge Parquet to Iceberg (Silver) with Nessie
    bronze_to_silver = BashOperator(
        task_id='bronze_to_silver',
        bash_command='cd /opt/airflow/spark && python spark_bronze_to_silver.py --run_id {{ run_id }}',
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

    # Hai nguồn ingest độc lập chạy song song; Silver chỉ chạy khi cả hai hoàn tất.
    [ingest_bronze, ingest_mysql] >> bronze_to_silver >> silver_to_gold >> predictive_analysis
