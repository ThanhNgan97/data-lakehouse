"""
api/routes/kpi_api_ingress.py
------------------------------------------------------------
Router FastAPI: Tiếp nhận dữ liệu KPI từ API do nhà trường cung cấp,
thực hiện kiểm định bằng Pydantic, sinh mã băm chống trùng lặp (SHA-256)
và lưu trữ thô trực tiếp vào tầng Bronze trên MinIO.
------------------------------------------------------------
"""

import os
import json
import hashlib
from datetime import datetime
from fastapi import APIRouter, HTTPException, status
from minio import Minio
from io import BytesIO

from api.schemas.kpi_api_schemas import TeachingProgressPayload, LearningOutcomesPayload

router = APIRouter(prefix="/api/v1/lakehouse/ingest", tags=["API Ingestion for Lakehouse"])

# --- Đọc cấu hình MinIO khớp với project hiện có ---
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
minio_host = MINIO_ENDPOINT.replace("http://", "").replace("https://", "")

MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.getenv("MINIO_BUCKET_NAME", "university-lakehouse")

def get_minio_client():
    return Minio(
        minio_host,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )

def generate_checksum(unique_string: str) -> str:
    """Tạo mã băm SHA-256 làm khóa định danh nghiệp vụ chống trùng lặp"""
    return hashlib.sha256(unique_string.encode('utf-8')).hexdigest()


@router.post("/teaching-progress", status_code=status.HTTP_201_CREATED)
async def ingest_teaching_progress(payload: TeachingProgressPayload):
    try:
        data_dict = payload.dict()
        
        # Tạo khóa định danh duy nhất (Checksum)
        raw_key = f"{data_dict['ky_danh_gia']}_{data_dict['don_vi_dao_tao']}"
        checksum = generate_checksum(raw_key)
        data_dict["checksum_sha256"] = checksum
        data_dict["ingested_at"] = datetime.utcnow().isoformat()

        # Đóng gói dữ liệu JSON thô
        json_data = json.dumps(data_dict, ensure_ascii=False).encode('utf-8')
        file_stream = BytesIO(json_data)
        
        timestamp_str = datetime.utcnow().strftime('%Y%m%d%H%M%S')
        file_name = f"teaching_progress_{data_dict['don_vi_dao_tao']}_{timestamp_str}.json"
        object_path = f"bronze/api_teaching/{file_name}"

        # Đẩy file lên MinIO
        minio_client = get_minio_client()
        if not minio_client.bucket_exists(MINIO_BUCKET):
            minio_client.make_bucket(MINIO_BUCKET)

        minio_client.put_object(
            bucket_name=MINIO_BUCKET,
            object_name=object_path,
            data=file_stream,
            length=len(json_data),
            content_type="application/json"
        )

        return {
            "status": "success",
            "message": "Đã tiếp nhận và lưu trữ thô dữ liệu tiến độ giảng dạy thành công vào MinIO (Bronze Layer).",
            "checksum": checksum,
            "minio_path": f"s3a://{MINIO_BUCKET}/{object_path}"
        }

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Lỗi khi xử lý dữ liệu API tiến độ giảng dạy: {str(e)}"
        )


@router.post("/learning-outcomes", status_code=status.HTTP_201_CREATED)
async def ingest_learning_outcomes(payload: LearningOutcomesPayload):
    try:
        data_dict = payload.dict()
        
        # Tạo khóa định danh duy nhất (Checksum)
        raw_key = f"{data_dict['ky_danh_gia']}_{data_dict['chuong_trinh']}"
        checksum = generate_checksum(raw_key)
        data_dict["checksum_sha256"] = checksum
        data_dict["ingested_at"] = datetime.utcnow().isoformat()

        json_data = json.dumps(data_dict, ensure_ascii=False).encode('utf-8')
        file_stream = BytesIO(json_data)
        
        timestamp_str = datetime.utcnow().strftime('%Y%m%d%H%M%S')
        file_name = f"learning_outcomes_{data_dict['chuong_trinh']}_{timestamp_str}.json"
        object_path = f"bronze/api_learning/{file_name}"

        minio_client = get_minio_client()
        if not minio_client.bucket_exists(MINIO_BUCKET):
            minio_client.make_bucket(MINIO_BUCKET)

        minio_client.put_object(
            bucket_name=MINIO_BUCKET,
            object_name=object_path,
            data=file_stream,
            length=len(json_data),
            content_type="application/json"
        )

        return {
            "status": "success",
            "message": "Đã tiếp nhận và lưu trữ thô kết quả học tập thành công vào MinIO (Bronze Layer).",
            "checksum": checksum,
            "minio_path": f"s3a://{MINIO_BUCKET}/{object_path}"
        }

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Lỗi khi xử lý dữ liệu API kết quả học tập: {str(e)}"
        )