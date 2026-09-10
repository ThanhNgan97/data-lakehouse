import logging
import requests
import uuid
import os
from fastapi import APIRouter, Depends, File, UploadFile, HTTPException
from api.dependencies import get_current_user
from db.database import get_db
from sqlalchemy.orm import Session
from db.models import UploadHistory, User
from db.minio_client import minio_client
from core.config import MINIO_BUCKET_NAME
from core.config import AIRFLOW_WEBSERVER_URL
router = APIRouter()
@router.get("/")
def read_root():
    return {"message": "Welcome to the Lakehouse API!"}

@router.post("/upload")
@router.post("/upload/")
def upload_file(
    file: UploadFile = File(...),
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    try:
        # Validate extensions basic
        ext = os.path.splitext(file.filename)[1].lower()
        if not ext:
            raise HTTPException(status_code=400, detail="File must have an extension")
            
        file.file.seek(0, 2)
        file_size = file.file.tell()
        file.file.seek(0)
        
        # 50MB max file size
        MAX_FILE_SIZE = 50 * 1024 * 1024
        if file_size > MAX_FILE_SIZE:
            raise HTTPException(status_code=413, detail="File size exceeds the maximum limit of 50MB.")
       
        # Mọi file người dùng nạp vào đều phải qua tầng Staging.
        unique_id = uuid.uuid4().hex
        object_name = f"staging/{unique_id}_{file.filename}"

   
        minio_client.put_object(
            bucket_name=MINIO_BUCKET_NAME,
            object_name=object_name,
            data=file.file,
            length=file_size
        )

        # Lưu lịch sử upload vào DB
        user_id = current_user.id if hasattr(current_user, "id") else current_user.get("id")
        
        history_record = UploadHistory(
            user_id=user_id,
            filename=file.filename,
            file_size_bytes=file_size,
            file_type=file.content_type,
            s3_path=object_name,
            metadata_info={"source": "api", "bucket": MINIO_BUCKET_NAME},
            status="Uploaded"
        )
        db.add(history_record)
        db.commit()

        # Trigger Airflow Pipeline
       

        airflow_url = f"{AIRFLOW_WEBSERVER_URL}/api/v1/dags/lakehouse_pipeline/dagRuns"
        dag_run_id = None
        try:
            # Assuming airflow-init sets up admin user with 'airflow:airflow'
            resp = requests.post(
                airflow_url, 
                json={"conf": {"file_key": object_name}}, 
                auth=("airflow", "airflow"), 
                timeout=5
            )
            if resp.status_code in [200, 201]:
                logging.info("Airflow pipeline triggered successfully.")
                dag_run_id = resp.json().get("dag_run_id")
                history_record.dag_run_id = dag_run_id
                history_record.pipeline_status = "running"
                db.commit()
            else:
                logging.warning(f"Failed to trigger Airflow pipeline [{resp.status_code}]: {resp.text}")
                history_record.pipeline_status = "trigger_failed"
                db.commit()
        except Exception as e:
            logging.error(f"Error triggering Airflow pipeline at {airflow_url}: {e}")
            history_record.pipeline_status = "unreachable"
            db.commit()

        return {
            "message": f"Tải lên thành công! Hệ thống đang tự động xử lý và phân tích file {file.filename}.",
            "dag_run_id": dag_run_id
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Lỗi đẩy file  {str(e)}")

@router.get("/upload/history", summary="Lấy lịch sử tải lên dữ liệu")
def get_upload_history(
    limit: int = 100,
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user)
):
    """
    Lấy danh sách các file đã được upload lên hệ thống.
    Kèm theo thông tin người upload, metadata, kích thước...
    """
    try:
        from sqlalchemy.orm import joinedload
        user_id = current_user.id if hasattr(current_user, "id") else current_user.get("id")
        histories = db.query(UploadHistory).options(joinedload(UploadHistory.uploader)).filter(UploadHistory.user_id == user_id).order_by(UploadHistory.uploaded_at.desc()).limit(limit).all()
        # Lọc bỏ các bản ghi đã bị soft delete
        filtered = [record.to_dict() for record in histories if not (record.metadata_info and record.metadata_info.get("is_hidden"))]
        return filtered
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Lỗi lấy lịch sử: {str(e)}")

@router.get("/upload/pipeline-status/{dag_run_id}", summary="Lấy trạng thái step-by-step của Pipeline")
def get_pipeline_status(
    dag_run_id: str,
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user)
):
    """
    Gọi Airflow API để lấy trạng thái của DAG run và các task bên trong.
    """
    airflow_base = f"{AIRFLOW_WEBSERVER_URL}/api/v1/dags/lakehouse_pipeline/dagRuns/{dag_run_id}"
    try:
        # Lấy trạng thái tổng quan DAG run
        resp_dag = requests.get(airflow_base, auth=("airflow", "airflow"), timeout=5)
        state = "unknown"
        if resp_dag.status_code == 200:
            state = resp_dag.json().get("state", "unknown")
            
        # Lấy trạng thái các task (taskInstances)
        tasks = []
        resp_tasks = requests.get(f"{airflow_base}/taskInstances", auth=("airflow", "airflow"), timeout=5)
        if resp_tasks.status_code == 200:
            task_instances = resp_tasks.json().get("task_instances", [])
            for t in task_instances:
                tasks.append({
                    "task_id": t.get("task_id"),
                    "state": t.get("state"),
                    "start_date": t.get("start_date"),
                    "end_date": t.get("end_date")
                })
        
        # --- REALTIME SYNC FIX ---
        # Ép trạng thái DAG thành success nếu task cuối cùng đã hoàn thành
        final_task = next((t for t in tasks if t["task_id"] == "predictive_analysis"), None)
        if state not in ["success", "failed"] and final_task and final_task["state"] == "success":
            state = "success"

        # Cập nhật pipeline_status vào DB (nếu chưa bị set cứng từ Spark thành failed)
        record = db.query(UploadHistory).filter(UploadHistory.dag_run_id == dag_run_id).first()
        
        user_id = current_user.id if hasattr(current_user, "id") else current_user.get("id")
        if record and record.user_id != user_id:
            raise HTTPException(status_code=403, detail="Không có quyền truy cập.")
        
        # Nếu Spark đã đánh dấu failed trong DB (kèm error_message), ghi đè state bằng failed 
        # và lấy thông báo lỗi ra trả về FE.
        error_message = None
        parsed_data = None
        if record:
            if record.pipeline_status == "failed" and record.metadata_info:
                state = "failed"
                error_message = record.metadata_info.get("error_message")
                
                # --- REALTIME ERROR SYNC FIX ---
                # Đảm bảo hiển thị ĐÚNG bước nào bị lỗi (bước trước đó xanh, bước hiện tại đỏ).
                # Ta sắp xếp theo đúng thứ tự Pipeline để bắt chính xác Task đang chạy dở.
                task_order = {"ingest_bronze": 1, "bronze_to_silver": 2, "silver_to_gold": 3, "predictive_analysis": 4}
                tasks.sort(key=lambda x: task_order.get(x["task_id"], 99))
                
                # Kiểm tra xem Airflow đã tự set task nào là 'failed' chưa
                has_failed_task = any(t["state"] == "failed" for t in tasks)
                
                if not has_failed_task:
                    for t in tasks:
                        if t["state"] not in ["success"]:
                            t["state"] = "failed"
                            break

            elif state != "unknown" and record.pipeline_status != "failed":
                record.pipeline_status = state
                db.commit()
                
            if record.metadata_info:
                parsed_data = record.metadata_info.get("parsed_data")
            
        return {
            "dag_run_id": dag_run_id,
            "state": state,
            "tasks": tasks,
            "error_message": error_message,
            "parsed_data": parsed_data
        }
    except Exception as e:
        return {"state": "unreachable", "error": str(e), "tasks": []}

@router.delete("/upload/history/{record_id}", summary="Soft delete lịch sử tải lên")
def delete_history_record(
    record_id: int,
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user)
):
    record = db.query(UploadHistory).filter(UploadHistory.id == record_id).first()
    if not record:
        raise HTTPException(status_code=404, detail="Không tìm thấy bản ghi.")
    user_id = current_user.id if hasattr(current_user, "id") else current_user.get("id")
    if record.user_id != user_id:
        raise HTTPException(status_code=403, detail="Không có quyền thực hiện.")
    
    # Soft delete bằng cách thêm is_hidden = True vào metadata_info
    # Lưu ý: SQLAlchemy cần gán lại dict cho cột JSONB để nhận diện thay đổi
    meta = dict(record.metadata_info) if record.metadata_info else {}
    meta["is_hidden"] = True
    record.metadata_info = meta
    db.commit()
    return {"message": "Đã xóa bản ghi khỏi giao diện thành công."}

@router.post("/upload/retry/{record_id}", summary="Thử kích hoạt lại pipeline khi bị lỗi mạng/kết nối")
def retry_upload_pipeline(
    record_id: int,
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user)
):
    record = db.query(UploadHistory).filter(UploadHistory.id == record_id).first()
    if not record:
        raise HTTPException(status_code=404, detail="Không tìm thấy bản ghi.")
    user_id = current_user.id if hasattr(current_user, "id") else current_user.get("id")
    if record.user_id != user_id:
        raise HTTPException(status_code=403, detail="Không có quyền thực hiện.")
    
    if record.pipeline_status in ["running", "queued", "pending", "uploaded", "triggered", "success"]:
        raise HTTPException(status_code=400, detail="Không thể thử lại pipeline đang chạy hoặc đã thành công.")
        
    try:
        minio_client.stat_object(MINIO_BUCKET_NAME, record.s3_path)
    except Exception:
        from minio.commonconfig import CopySource
        archive_path = record.s3_path.replace("staging/", "archive/", 1)
        failed_path = record.s3_path.replace("staging/", "failed_staging/", 1)
        try:
            minio_client.copy_object(MINIO_BUCKET_NAME, record.s3_path, CopySource(MINIO_BUCKET_NAME, archive_path))
        except Exception:
            try:
                minio_client.copy_object(MINIO_BUCKET_NAME, record.s3_path, CopySource(MINIO_BUCKET_NAME, failed_path))
            except Exception:
                raise HTTPException(status_code=500, detail="Không tìm thấy file nguồn để retry.")

    airflow_url = f"{AIRFLOW_WEBSERVER_URL}/api/v1/dags/lakehouse_pipeline/dagRuns"
    try:
        resp = requests.post(
            airflow_url, 
            json={"conf": {"file_key": record.s3_path}}, 
            auth=("airflow", "airflow"), 
            timeout=5
        )
        if resp.status_code in [200, 201]:
            dag_run_id = resp.json().get("dag_run_id")
            
            meta = dict(record.metadata_info) if record.metadata_info else {}
            if record.dag_run_id:
                meta["old_dag_run_id"] = record.dag_run_id
            if "error_message" in meta:
                del meta["error_message"]
            record.metadata_info = meta
            
            record.dag_run_id = dag_run_id
            record.pipeline_status = "running"
            
            db.commit()
            return {"message": "Đã kích hoạt lại thành công.", "dag_run_id": dag_run_id}
        else:
            raise HTTPException(status_code=500, detail=f"Vẫn không thể kích hoạt [{resp.status_code}]")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Lỗi kết nối: {str(e)}")
