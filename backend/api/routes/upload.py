import logging
from pathlib import Path
from uuid import uuid4

import requests
from fastapi import APIRouter, Depends, File, UploadFile, HTTPException
from api.dependencies import get_current_user
from db.database import get_db
from sqlalchemy.orm import Session
from db.models import UploadHistory, User
from db.minio_client import minio_client
from core.config import MINIO_BUCKET_NAME
from core.config import AIRFLOW_WEBSERVER_URL
router = APIRouter()

DOCUMENT_SOURCE_TYPE = "DOCUMENT_FILE"
MYSQL_DUMP_SOURCE_TYPE = "MYSQL_DUMP"


def _normalize_upload_filename(filename: str) -> str:
    """Return basename only so user input cannot control MinIO path."""
    raw_name = str(filename or "").replace("\\", "/")
    normalized = raw_name.rsplit("/", 1)[-1].strip()

    if not normalized:
        raise ValueError("Uploaded file must have a valid filename.")

    return normalized


def _classify_upload_source(filename: str) -> str:
    """Classify .sql as MySQL dump; preserve existing behavior otherwise."""
    extension = Path(filename).suffix.lower()

    if extension == ".sql":
        return MYSQL_DUMP_SOURCE_TYPE

    return DOCUMENT_SOURCE_TYPE


def _build_staging_object_name(
    filename: str,
    source_type: str,
) -> str:
    """Build a collision-safe raw staging key."""
    upload_token = uuid4().hex

    if source_type == MYSQL_DUMP_SOURCE_TYPE:
        return (
            f"staging/sql_dump/"
            f"{upload_token}/{filename}"
        )

    return (
        f"staging/uploads/"
        f"{upload_token}/{filename}"
    )
@router.get("/")
def read_root():
    return {"message": "Welcome to the Lakehouse API!"}

@router.post("/upload")
@router.post("/upload/")
async def upload_file(
    file: UploadFile = File(...),
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    try:
        filename = _normalize_upload_filename(file.filename)
        source_type = _classify_upload_source(filename)

        object_name = _build_staging_object_name(
            filename,
            source_type,
        )

        file.file.seek(0, 2)
        file_size = file.file.tell()
        file.file.seek(0)

        minio_client.put_object(
            bucket_name=MINIO_BUCKET_NAME,
            object_name=object_name,
            data=file.file,
            length=file_size,
        )

        user_id = (
            current_user.id
            if hasattr(current_user, "id")
            else current_user.get("id")
        )

        history_record = UploadHistory(
            user_id=user_id,
            filename=filename,
            file_size_bytes=file_size,
            file_type=file.content_type,
            s3_path=object_name,
            metadata_info={
                "source": "api",
                "bucket": MINIO_BUCKET_NAME,
                "source_type": source_type,
                "object_key": object_name,
                "original_filename": filename,
            },
            status="Uploaded",
        )

        db.add(history_record)
        db.commit()
        db.refresh(history_record)

        upload_id = history_record.id

        dag_conf = {
            "upload_id": upload_id,
            "object_key": object_name,
            "filename": filename,
            "source_type": source_type,
        }

        airflow_url = (
            f"{AIRFLOW_WEBSERVER_URL}"
            "/api/v1/dags/lakehouse_pipeline/dagRuns"
        )

        dag_run_id = None

        try:
            # Existing Airflow credentials are intentionally unchanged
            # in this feature checkpoint.
            resp = requests.post(
                airflow_url,
                json={"conf": dag_conf},
                auth=("airflow", "airflow"),
                timeout=5,
            )

            if resp.status_code in [200, 201]:
                logging.info(
                    "Airflow pipeline triggered successfully "
                    "for upload_id=%s source_type=%s object_key=%s",
                    upload_id,
                    source_type,
                    object_name,
                )

                dag_run_id = resp.json().get("dag_run_id")
                history_record.dag_run_id = dag_run_id
                history_record.pipeline_status = "running"
                db.commit()

            else:
                logging.warning(
                    "Failed to trigger Airflow pipeline [%s]: %s",
                    resp.status_code,
                    resp.text,
                )
                history_record.pipeline_status = "trigger_failed"
                db.commit()

        except Exception as exc:
            logging.error(
                "Error triggering Airflow pipeline at %s: %s",
                airflow_url,
                exc,
            )
            history_record.pipeline_status = "unreachable"
            db.commit()

        return {
            "message": (
                f"Uploaded {filename} to {object_name} "
                "and requested pipeline execution."
            ),
            "upload_id": upload_id,
            "dag_run_id": dag_run_id,
            "object_key": object_name,
            "source_type": source_type,
        }

    except HTTPException:
        raise

    except Exception as exc:
        db.rollback()
        raise HTTPException(
            status_code=500,
            detail=f"Upload failed: {str(exc)}",
        )


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
            
        return {
            "dag_run_id": dag_run_id,
            "state": state,
            "tasks": tasks,
            "error_message": error_message,
            "parsed_data": parsed_data
        }
    except Exception as e:
        return {"state": "unreachable", "error": str(e), "tasks": []}
