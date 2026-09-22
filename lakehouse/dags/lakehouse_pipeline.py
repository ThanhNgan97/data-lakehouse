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
    description='Pipeline for Lakehouse: Bronze -> Silver -> Gold (Bao gồm dữ liệu file và API trường)',
    schedule_interval=None, # Triggered externally via API hoặc bấm thủ công trên UI
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['lakehouse', 'api-integration'],
) as dag:

    # --- CÁC TASK CŨ (Dữ liệu file truyền thống) ---
    ingest_bronze = BashOperator(
        task_id='ingest_bronze',
        bash_command='cd /opt/airflow/spark && python spark_ingest_bronze.py --run_id {{ run_id }}',
    )

    bronze_to_silver = BashOperator(
        task_id='bronze_to_silver',
        bash_command='cd /opt/airflow/spark && python spark_bronze_to_silver.py --run_id {{ run_id }}',
    )

    silver_to_gold = BashOperator(
        task_id='silver_to_gold',
        bash_command='cd /opt/airflow/spark && python spark_silver_to_gold.py',
    )

    predictive_analysis = BashOperator(
        task_id='predictive_analysis',
        bash_command='cd /opt/airflow/spark && python spark_predictive_analysis.py',
    )

    # --- [MỚI] CÁC TASK CHO LUỒNG DỮ LIỆU API TRƯỜNG CUNG CẤP ---
    
    # Task A1: Xử lý Silver cho Tiến độ giảng dạy API
    api_teaching_silver = BashOperator(
        task_id='api_teaching_silver',
        bash_command='cd /opt/airflow/spark && python spark_api_teaching_silver.py',
    )

    # Task A2: Xử lý Silver cho Kết quả học tập API
    api_learning_silver = BashOperator(
        task_id='api_learning_silver',
        bash_command='cd /opt/airflow/spark && python spark_api_learning_silver.py',
    )

    # Task A3: Tổng hợp Gold Data Marts cho dữ liệu API
    api_gold_aggregation = BashOperator(
        task_id='api_gold_aggregation',
        bash_command='cd /opt/airflow/spark && python spark_api_gold_aggregation.py',
    )

    # --- ĐỊNH NGHĨA LUỒNG CHẠY (DAG DEPENDENCIES) ---
    
    # 1. Luồng dữ liệu cũ chạy trước hoặc song song
    ingest_bronze >> bronze_to_silver >> silver_to_gold >> predictive_analysis

    # 2. Luồng dữ liệu API mới: Ingest Bronze (hoặc fetch qua API) -> Silver (Teaching & Learning) -> Gold Aggregation
    # Bạn có thể cho luồng API chạy song song hoặc nối tiếp sau luồng cũ tùy ý. 
    # Ở đây ta thiết lập luồng API chạy độc lập hoặc sau bước bronze_to_silver:
    [ingest_bronze] >> api_teaching_silver >> api_gold_aggregation >> predictive_analysis
    [ingest_bronze] >> api_learning_silver >> api_gold_aggregation >> predictive_analysis