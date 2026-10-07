import logging
import requests
from threading import Thread
from fastapi import APIRouter, Depends, File, UploadFile, HTTPException
from api.dependencies import get_current_user
from db.database import get_db
from sqlalchemy.orm import Session
from db.models import ImportJob, ImportJobFile, SourceFile, UploadHistory, User
from db.minio_client import minio_client
from core.config import MAX_UPLOAD_FILE_BYTES, MINIO_BUCKET_NAME
from core.config import AIRFLOW_FILE_DAG_ID, AIRFLOW_WEBSERVER_URL
from services.url_ingestion.state import derive_job_status, utcnow
from services.url_ingestion.manager import process_import_job
router = APIRouter()
@router.get("/")
def read_root():
    return {"message": "Welcome to the Lakehouse API!"}


@router.post("/upload")
@router.post("/upload/")
async def upload_file(
    file: UploadFile = File(...),
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    try:
       
        # Mọi file người dùng nạp vào đều phải qua tầng Staging.
        object_name = f"staging/{file.filename}"

        file.file.seek(0, 2)
        file_size = file.file.tell()
        file.file.seek(0)

        if file_size > MAX_UPLOAD_FILE_BYTES:
            max_size_mb = MAX_UPLOAD_FILE_BYTES // (1024 * 1024)
            raise HTTPException(
                status_code=413,
                detail=f"File vượt giới hạn {max_size_mb} MB.",
            )

   
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
       

        airflow_url = f"{AIRFLOW_WEBSERVER_URL}/api/v1/dags/{AIRFLOW_FILE_DAG_ID}/dagRuns"
        dag_run_id = None
        try:
            # Truyền conf chứa vị trí staging và tên file để AI Semantic Router xử lý
            payload = {
                "conf": {
                    "input_path": object_name,
                    "source_name": file.filename
                }
            }
            resp = requests.post(airflow_url, json=payload, auth=("airflow", "airflow"), timeout=5)
            if resp.status_code in [200, 201]:
                logging.info("Universal Airflow pipeline triggered successfully.")
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
            "message": f"Đã đẩy trực tiếp file {file.filename} vào trạm {object_name} của MinIO và kích hoạt pipeline!",
            "dag_run_id": dag_run_id
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Lỗi đẩy file  {str(e)}")

@router.get("/upload/history", summary="Lấy lịch sử tải lên dữ liệu")
async def get_upload_history(
    limit: int = 100,
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user)
):
    """
    Lấy danh sách các file đã được upload lên hệ thống.
    Kèm theo thông tin người upload, metadata, kích thước...
    """
    try:
        histories = db.query(UploadHistory).join(User, UploadHistory.user_id == User.id).order_by(UploadHistory.uploaded_at.desc()).limit(limit)
        return [record.to_dict() for record in histories]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Lỗi lấy lịch sử: {str(e)}")

@router.get("/upload/pipeline-status/{dag_run_id}", summary="Lấy trạng thái step-by-step của Pipeline")
async def get_pipeline_status(
    dag_run_id: str,
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user)
):
    """
    Gọi Airflow API để lấy trạng thái của DAG run và các task bên trong.
    """
    airflow_base = f"{AIRFLOW_WEBSERVER_URL}/api/v1/dags/{AIRFLOW_FILE_DAG_ID}/dagRuns/{dag_run_id}"
    try:
        # Lấy trạng thái tổng quan DAG run (thử universal_lakehouse_pipeline trước, fallback về legacy)
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
        
        # Cập nhật pipeline_status vào DB (nếu chưa bị set cứng từ Spark thành failed)
        record = db.query(UploadHistory).filter(UploadHistory.dag_run_id == dag_run_id).first()
        
        # Nếu Spark đã đánh dấu failed trong DB (kèm error_message), ghi đè state bằng failed 
        # và lấy thông báo lỗi ra trả về FE.
        error_message = None
        parsed_data = None
        if record:
            if record.pipeline_status == "failed" and record.metadata_info:
                state = "failed"
                error_message = record.metadata_info.get("error_message")
            elif state != "unknown" and record.pipeline_status != "failed":
                record.pipeline_status = state
                db.commit()
                
            if record.metadata_info:
                parsed_data = record.metadata_info.get("parsed_data")

        job_files = db.query(ImportJobFile).filter(ImportJobFile.dag_run_id == dag_run_id).all()
        if job_files and state in {"success", "failed"}:
            transitioned_job_ids = set()
            for job_file in job_files:
                if job_file.status not in {"COMPLETED", "FAILED"}:
                    transitioned_job_ids.add(job_file.import_job_id)
                job_file.status = "COMPLETED" if state == "success" else "FAILED"
                job_file.completed_at = utcnow()
                if state == "failed" and not job_file.error_code:
                    job_file.error_code = "AIRFLOW_FAILED"
                    job_file.error_message = error_message or "Universal pipeline failed."
                if job_file.source_file_id:
                    source_file = db.query(SourceFile).filter(SourceFile.id == job_file.source_file_id).first()
                    if source_file:
                        source_file.processing_status = "COMPLETED" if state == "success" else "FAILED"
                        source_file.processed_at = utcnow()
            for job_id in {item.import_job_id for item in job_files}:
                job = db.query(ImportJob).filter(ImportJob.id == job_id).first()
                if not job:
                    continue
                states = [value[0] for value in db.query(ImportJobFile.status).filter(
                    ImportJobFile.import_job_id == job.id
                ).all()]
                job.status = derive_job_status(states, bool(job.cancel_requested_at))
                if job.status not in {"PENDING", "PROCESSING"}:
                    job.completed_at = utcnow()
            db.commit()
            for job_id in transitioned_job_ids:
                Thread(target=process_import_job, args=(job_id,), daemon=True).start()
            
        return {
            "dag_run_id": dag_run_id,
            "state": state,
            "tasks": tasks,
            "error_message": error_message,
            "parsed_data": parsed_data
        }
    except Exception as e:
        return {"state": "unreachable", "error": str(e), "tasks": []}
