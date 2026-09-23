from datetime import datetime, timedelta
try:
    from airflow import DAG  # type: ignore # pyrefly: ignore [missing-import]
    from airflow.operators.bash import BashOperator  # type: ignore # pyrefly: ignore [missing-import]
    from airflow.operators.python import BranchPythonOperator  # type: ignore # pyrefly: ignore [missing-import]
except ImportError:
    pass

default_args = {
    'owner': 'lakehouse',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}

def check_data_source(**context):
    """
    Hàm phân định luồng dựa vào dag_run.conf:
    - {"source_type": "file"}         -> Chỉ chạy luồng File
    - {"source_type": "api_teaching"} -> Chỉ chạy luồng API Tiến độ giảng dạy
    - {"source_type": "api_learning"} -> Chỉ chạy luồng API Kết quả học tập
    - {"source_type": "api"}          -> Chạy cả 2 luồng API
    - Không truyền gì / 'both'        -> Chạy tất cả các luồng
    """
    dag_run = context.get('dag_run')
    dag_run_conf = (dag_run.conf if dag_run and dag_run.conf else {}) or {}
    source_type = dag_run_conf.get('source_type', 'both')
    
    if source_type == 'file':
        return ['ingest_bronze']
    elif source_type == 'api_teaching':
        return ['api_teaching_silver']
    elif source_type == 'api_learning':
        return ['api_learning_silver']
    elif source_type == 'api':
        return ['api_teaching_silver', 'api_learning_silver']
    else:
        return ['ingest_bronze', 'api_teaching_silver', 'api_learning_silver']

with DAG(
    'lakehouse_pipeline',
    default_args=default_args,
    description='Pipeline for Lakehouse: Tách biệt hoàn toàn Luồng File và Luồng API',
    schedule_interval=None, # Triggered externally via API hoặc bấm thủ công trên UI
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['lakehouse', 'api-integration'],
) as dag:

    # --- TASK RẼ NHÁNH ĐẦU TIÊN ---
    branch_task = BranchPythonOperator(
        task_id='check_data_source',
        python_callable=check_data_source,
    )

    # --- LUỒNG 1: DỮ LIỆU FILE TRUYỀN THỐNG ---
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

    # --- LUỒNG 2: DỮ LIỆU API TRƯỜNG CUNG CẤP (Độc lập hoàn toàn) ---
    api_teaching_silver = BashOperator(
        task_id='api_teaching_silver',
        bash_command='cd /opt/airflow/spark && python spark_api_teaching_silver.py',
    )

    api_learning_silver = BashOperator(
        task_id='api_learning_silver',
        bash_command='cd /opt/airflow/spark && python spark_api_learning_silver.py',
    )

    api_gold_aggregation = BashOperator(
        task_id='api_gold_aggregation',
        bash_command='cd /opt/airflow/spark && python spark_api_gold_aggregation.py',
        trigger_rule='none_failed_min_one_success',
    )

    # --- TASK HỘI TỤ CUỐI CÙNG ---
    predictive_analysis = BashOperator(
        task_id='predictive_analysis',
        bash_command='cd /opt/airflow/spark && python spark_predictive_analysis.py',
        trigger_rule='none_failed_min_one_success', # Cho phép chạy tiếp dù nhánh còn lại bị skip
    )

    # --- ĐỊNH NGHĨA QUAN HỆ PHỤ THUỘC (DEPENDENCIES) ---
    
    # Rẽ nhánh đến điểm xuất phát của từng luồng
    branch_task >> ingest_bronze
    branch_task >> api_teaching_silver
    branch_task >> api_learning_silver

    # Luồng File chạy tuần tự
    ingest_bronze >> bronze_to_silver >> silver_to_gold >> predictive_analysis

    # Luồng API chạy tuần tự
    api_teaching_silver >> api_gold_aggregation >> predictive_analysis
    api_learning_silver >> api_gold_aggregation >> predictive_analysis