# -*- coding: utf-8 -*-
import os
import psycopg2

def restore_logs():
    conn = psycopg2.connect(
        dbname="airflow_db",
        user="postgres",
        password="240203",
        host="postgres",
        port="5432"
    )
    cursor = conn.cursor()
    cursor.execute("""
        SELECT dag_id, run_id, task_id, state 
        FROM task_instance
    """)

    rows = cursor.fetchall()
    logs_dir = "/opt/airflow/logs"

    created_count = 0
    for dag_id, run_id, task_id, state in rows:
        task_dir = os.path.join(logs_dir, f"dag_id={dag_id}", f"run_id={run_id}", f"task_id={task_id}")
        os.makedirs(task_dir, exist_ok=True)
        log_file = os.path.join(task_dir, "attempt=1.log")
        if not os.path.exists(log_file):
            with open(log_file, "w", encoding="utf-8") as f:
                f.write(
                    f"[Airflow Log Restored]\n"
                    f"DAG ID: {dag_id}\n"
                    f"Run ID: {run_id}\n"
                    f"Task ID: {task_id}\n"
                    f"Task State: {state or 'N/A'}\n"
                    f"--------------------------------------------------\n"
                    f"File log được khởi tạo lại cho lượt chạy lịch sử trước khi restart container Airflow.\n"
                )
            created_count += 1

    print(f"✅ Đã tạo thành công {created_count} file log lịch sử còn thiếu trên ổ đĩa.")
    conn.close()

if __name__ == "__main__":
    restore_logs()
