import psycopg2
import json
from env_config import PG_HOST, PG_PORT, PG_USER, PG_PASSWORD

def get_db_connection():
    """Tạo kết nối tới database PostgreSQL dựa trên cấu hình linh hoạt (Docker vs Host)."""
    return psycopg2.connect(
        dbname="university_db",
        user=PG_USER,
        password=PG_PASSWORD,
        host=PG_HOST,
        port=PG_PORT
    )

def update_pipeline_error(run_id, error_message):
    """
    Updates the upload_history record in Postgres with the given error_message
    for the specific dag_run_id.
    """
    if not run_id:
        print("update_pipeline_error: No run_id provided. Skipping DB update.")
        return

    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Lấy metadata_info hiện tại
        cur.execute("SELECT metadata_info FROM upload_history WHERE dag_run_id = %s", (run_id,))
        row = cur.fetchone()
        
        if row:
            metadata = row[0] if row[0] is not None else {}
            if isinstance(metadata, str):
                metadata = json.loads(metadata)
                
            metadata["error_message"] = error_message
            
            # Cập nhật pipeline_status = 'failed' và lưu metadata_info
            cur.execute(
                "UPDATE upload_history SET pipeline_status = 'failed', metadata_info = %s WHERE dag_run_id = %s",
                (json.dumps(metadata), run_id)
            )
            conn.commit()
            print(f"✅ Đã lưu thông báo lỗi vào DB cho run_id={run_id}")
        else:
            print(f"⚠️ Không tìm thấy record nào có dag_run_id={run_id} trong bảng upload_history")
            
        cur.close()
        conn.close()
    except Exception as e:
        print(f"❌ Lỗi khi cập nhật DB (update_pipeline_error): {e}")

def save_parsed_data(run_id, data):
    """
    Saves the extracted data from Gemini into the upload_history record
    so the frontend can display it to the user.
    """
    if not run_id or not data:
        return

    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        cur.execute("SELECT metadata_info FROM upload_history WHERE dag_run_id = %s", (run_id,))
        row = cur.fetchone()
        
        if row:
            metadata = row[0] if row[0] is not None else {}
            if isinstance(metadata, str):
                metadata = json.loads(metadata)
                
            metadata["parsed_data"] = data
            
            cur.execute(
                "UPDATE upload_history SET metadata_info = %s WHERE dag_run_id = %s",
                (json.dumps(metadata), run_id)
            )
            conn.commit()
            print(f"✅ Đã lưu dữ liệu parse từ Gemini vào DB cho run_id={run_id}")
            
        cur.close()
        conn.close()
    except Exception as e:
        print(f"❌ Lỗi khi lưu parsed data (save_parsed_data): {e}")
