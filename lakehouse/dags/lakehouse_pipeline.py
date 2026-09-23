from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

default_args = {
    'owner': 'lakehouse',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}


def choose_processing_branch(**kwargs):
    """
    Hàm kiểm tra `target_dataset` truyền từ API (dag_run.conf)
    để rẽ nhánh xử lý phù hợp mà KHÔNG ẢNH HƯỞNG TỚI CODE CỦ.
    """
    dag_run = kwargs.get('dag_run')
    conf = dag_run.conf if dag_run and dag_run.conf else {}
    target_dataset = conf.get('target_dataset', 'kpi_cusc')

    if target_dataset in ['learning_outcomes', 'teaching_progress', 'ctu_ioc']:
        return 'branch_ctu_ioc_silver'
    else:
        return 'branch_kpi_cusc_silver'


with DAG(
    'lakehouse_pipeline',
    default_args=default_args,
    description='Pipeline for Lakehouse: Bronze -> Silver -> Gold (Hỗ trợ Chia nhánh Dataset)',
    schedule_interval=None,  # External API Trigger
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['lakehouse'],
) as dag:

    # Task 1: Ingest thô dữ liệu vào MinIO Bronze
    ingest_bronze = BashOperator(
        task_id='ingest_bronze',
        bash_command='cd /opt/airflow/spark && python spark_ingest_bronze.py --run_id {{ run_id }}{% if dag_run.conf and dag_run.conf.get("file_key") %} --file_key "{{ dag_run.conf.get("file_key") }}"{% endif %}',
    )

    # Task 2: Chia nhánh xử lý dựa trên Dataset
    branching_node = BranchPythonOperator(
        task_id='branch_by_dataset',
        python_callable=choose_processing_branch,
        provide_context=True,
    )

    # Nhánh A: Xử lý dữ liệu KPI CUSC (Code cũ - Giữ nguyên 100%)
    branch_kpi_cusc_silver = BashOperator(
        task_id='branch_kpi_cusc_silver',
        bash_command='cd /opt/airflow/spark && python spark_bronze_to_silver.py --run_id {{ run_id }} --dataset kpi_cusc',
    )

    # Nhánh B: Xử lý dữ liệu CTU SMART IOC (Nhánh mới)
    branch_ctu_ioc_silver = BashOperator(
        task_id='branch_ctu_ioc_silver',
        bash_command='cd /opt/airflow/spark && python spark_bronze_to_silver.py --run_id {{ run_id }} --dataset ctu_ioc',
    )

    # Task 3: Hội tụ về tầng Gold (Trigger khi 1 trong các nhánh Silver thành công)
    silver_to_gold = BashOperator(
        task_id='silver_to_gold',
        bash_command='cd /opt/airflow/spark && python spark_silver_to_gold.py',
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    # Task 4: Predictive Analysis ở tầng Gold
    predictive_analysis = BashOperator(
        task_id='predictive_analysis',
        bash_command='cd /opt/airflow/spark && python spark_predictive_analysis.py',
    )

    # Định nghĩa luồng chạy chia nhánh trong Airflow Graph
    ingest_bronze >> branching_node
    branching_node >> branch_kpi_cusc_silver >> silver_to_gold
    branching_node >> branch_ctu_ioc_silver >> silver_to_gold
    silver_to_gold >> predictive_analysis
