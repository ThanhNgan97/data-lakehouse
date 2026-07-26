import logging
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
        try:
            # Assuming airflow-init sets up admin user with 'airflow:airflow'
            resp = requests.post(airflow_url, json={}, auth=("airflow", "airflow"), timeout=5)
            if resp.status_code in [200, 201]:
                logging.info("Airflow pipeline triggered successfully.")
            else:
                logging.warning(f"Failed to trigger Airflow pipeline [{resp.status_code}]: {resp.text}")
        except Exception as e:
            logging.error(f"Error triggering Airflow pipeline at {airflow_url}: {e}")

        return {"message": f"Đã đẩy trực tiếp file {file.filename} vào trạm {object_name} của MinIO và kích hoạt pipeline!"}
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
