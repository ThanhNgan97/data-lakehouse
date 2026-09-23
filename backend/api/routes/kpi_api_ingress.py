"""
api/routes/kpi_api_ingress.py
------------------------------------------------------------
Router FastAPI: Tiếp nhận dữ liệu KPI từ API do nhà trường cung cấp (Hỗ trợ cả 1 phần tử lẫn Danh sách mảng [...] nhiều phần tử cùng lúc),
thực hiện chuẩn hóa dữ liệu linh hoạt, sinh mã băm SHA-256, lưu trữ thô vào MinIO (Bronze Layer)
VÀ TỰ ĐỘNG KÍCH HOẠT AIRFLOW DAG PIPELINE!
------------------------------------------------------------
"""

import os
import json
import hashlib
import requests
from datetime import datetime
from typing import Union, List, Dict, Any
from fastapi import APIRouter, HTTPException, status
from minio import Minio
from io import BytesIO

from api.schemas.kpi_api_schemas import TeachingProgressPayload, LearningOutcomesPayload

router = APIRouter(prefix="/api/v1/lakehouse/ingest", tags=["API Ingestion for Lakehouse"])

# --- Cấu hình MinIO & Airflow ---
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
minio_host = MINIO_ENDPOINT.replace("http://", "").replace("https://", "")

MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.getenv("MINIO_BUCKET_NAME", "university-lakehouse")

AIRFLOW_WEBSERVER_URL = os.getenv("AIRFLOW_WEBSERVER_URL", "http://localhost:8080")

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

def trigger_airflow_api_pipeline(source_type: str = "api"):
    """Tự động gọi Airflow REST API để kích hoạt DAG lakehouse_pipeline với source_type cụ thể"""
    try:
        url = f"{AIRFLOW_WEBSERVER_URL}/api/v1/dags/lakehouse_pipeline/dagRuns"
        payload = {"conf": {"source_type": source_type}}
        requests.post(url, json=payload, auth=("airflow", "airflow"), timeout=5)
        print(f"🚀 Đã tự động kích hoạt Airflow DAG lakehouse_pipeline (source_type = {source_type})!")
    except Exception as exc:
        print(f"⚠️ Không thể kích hoạt Airflow tự động: {exc}")

def normalize_teaching_item(item: dict) -> dict:
    """Chuẩn hóa dữ liệu tiến độ giảng dạy linh hoạt (nhận cả alias)"""
    data = dict(item)
    ky_danh_gia = data.get("ky_danh_gia") or "Q2/2026"
    don_vi = data.get("don_vi_dao_tao") or data.get("don_vi") or "Đơn vị không xác định"
    
    dung_tien_do = data.get("dung_tien_do_pct") if "dung_tien_do_pct" in data else data.get("dung_tien_do", 0.0)
    hien_dien = data.get("hien_dien_pct") if "hien_dien_pct" in data else data.get("hien_dien", 0.0)
    nhap_diem = data.get("nhap_diem_pct") if "nhap_diem_pct" in data else data.get("nhap_diem", 0.0)
    phan_hoi = str(data.get("diem_phan_hoi_sv") if "diem_phan_hoi_sv" in data else str(data.get("phan_hoi_sv", "4.0/5"))).replace(",", ".")
    
    normalized = {
        "ky_danh_gia": str(ky_danh_gia),
        "don_vi_dao_tao": str(don_vi),
        "lop_hp": int(data.get("lop_hp", 0)),
        "dung_tien_do_pct": float(dung_tien_do),
        "hien_dien_pct": float(hien_dien),
        "nhap_diem_pct": float(nhap_diem),
        "doi_lich": int(data.get("doi_lich", 0)),
        "diem_phan_hoi_sv": str(phan_hoi),
        "danh_gia": str(data.get("danh_gia", "Đạt"))
    }
    for k, v in data.items():
        if k not in normalized and k not in ["dung_tien_do", "hien_dien", "nhap_diem", "phan_hoi_sv"]:
            normalized[k] = v
            
    raw_key = f"{normalized['ky_danh_gia']}_{normalized['don_vi_dao_tao']}"
    normalized["checksum_sha256"] = generate_checksum(raw_key)
    normalized["ingested_at"] = datetime.utcnow().isoformat()
    return normalized

def normalize_learning_item(item: dict) -> dict:
    """Chuẩn hóa dữ liệu kết quả học tập linh hoạt"""
    data = dict(item)
    ky_danh_gia = data.get("ky_danh_gia") or "Q2/2026"
    chuong_trinh = data.get("chuong_trinh") or "Chương trình chưa xác định"
    
    dung_tien_do = data.get("dung_tien_do_pct") if "dung_tien_do_pct" in data else data.get("dung_tien_do", 0.0)
    qua_hp = data.get("qua_hp_pct") if "qua_hp_pct" in data else data.get("qua_hp", 0.0)
    gpa = data.get("gpa_trung_binh") if "gpa_trung_binh" in data else data.get("gpa", 3.0)
    
    can_bao_val = data.get("canh_bao") if "canh_bao" in data else (data.get("can_bao") if "can_bao" in data else data.get("can_bao_hoc_vu", 0))
    nguy_co_val = data.get("nguy_co_nghi") if "nguy_co_nghi" in data else data.get("nguy_co_nghi_hoc", 0)

    normalized = {
        "ky_danh_gia": str(ky_danh_gia),
        "chuong_trinh": str(chuong_trinh),
        "sv_theo_hoc": int(data.get("sv_theo_hoc", 0)),
        "qua_hp_pct": float(qua_hp),
        "gpa_trung_binh": float(gpa),
        "can_bao_hoc_vu": int(can_bao_val),
        "nguy_co_nghi_hoc": int(nguy_co_val),
        "dung_tien_do_pct": float(dung_tien_do),
        "xu_huong": str(data.get("xu_huong", "Ổn định"))
    }
    for k, v in data.items():
        if k not in normalized:
            normalized[k] = v

    raw_key = f"{normalized['ky_danh_gia']}_{normalized['chuong_trinh']}"
    normalized["checksum_sha256"] = generate_checksum(raw_key)
    normalized["ingested_at"] = datetime.utcnow().isoformat()
    return normalized


@router.post("/teaching-progress", status_code=status.HTTP_201_CREATED)
async def ingest_teaching_progress(payload: Union[List[Dict[str, Any]], Dict[str, Any], TeachingProgressPayload, List[TeachingProgressPayload]]):
    """
    Tiếp nhận dữ liệu tiến độ giảng dạy.
    Hỗ trợ nhận 1 phần tử dạng {...} HOẶC danh sách mảng [...] nhiều phần tử cùng lúc.
    TỰ ĐỘNG TRIGGER AIRFLOW PIPELINE LUỒNG API.
    """
    try:
        if isinstance(payload, list):
            items_raw = [item.dict() if hasattr(item, "dict") else item for item in payload]
        else:
            items_raw = [payload.dict() if hasattr(payload, "dict") else payload]

        normalized_items = [normalize_teaching_item(item) for item in items_raw]
        minio_client = get_minio_client()

        if not minio_client.bucket_exists(MINIO_BUCKET):
            minio_client.make_bucket(MINIO_BUCKET)

        saved_paths = []
        for data_dict in normalized_items:
            json_data = json.dumps(data_dict, ensure_ascii=False).encode('utf-8')
            file_stream = BytesIO(json_data)
            
            timestamp_str = datetime.utcnow().strftime('%Y%m%d%H%M%S%f')
            file_name = f"teaching_progress_{data_dict['don_vi_dao_tao']}_{timestamp_str}.json"
            object_path = f"bronze/api_teaching/{file_name}"

            minio_client.put_object(
                bucket_name=MINIO_BUCKET,
                object_name=object_path,
                data=file_stream,
                length=len(json_data),
                content_type="application/json"
            )
            saved_paths.append(f"s3a://{MINIO_BUCKET}/{object_path}")

        # --- TỰ ĐỘNG TÍCH HỢP KÍCH HOẠT AIRFLOW ---
        trigger_airflow_api_pipeline("api_teaching")

        return {
            "status": "success",
            "message": f"Đã tiếp nhận {len(normalized_items)} bản ghi và tự động kích hoạt Airflow Pipeline!",
            "total_records": len(normalized_items),
            "minio_paths": saved_paths
        }

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Lỗi khi xử lý dữ liệu API tiến độ giảng dạy: {str(e)}"
        )


@router.post("/learning-outcomes", status_code=status.HTTP_201_CREATED)
async def ingest_learning_outcomes(payload: Union[List[Dict[str, Any]], Dict[str, Any], LearningOutcomesPayload, List[LearningOutcomesPayload]]):
    """
    Tiếp nhận dữ liệu kết quả học tập.
    Hỗ trợ nhận 1 phần tử dạng {...} HOẶC danh sách mảng [...] nhiều phần tử cùng lúc.
    TỰ ĐỘNG TRIGGER AIRFLOW PIPELINE LUỒNG API.
    """
    try:
        if isinstance(payload, list):
            items_raw = [item.dict() if hasattr(item, "dict") else item for item in payload]
        else:
            items_raw = [payload.dict() if hasattr(payload, "dict") else payload]

        normalized_items = [normalize_learning_item(item) for item in items_raw]
        minio_client = get_minio_client()

        if not minio_client.bucket_exists(MINIO_BUCKET):
            minio_client.make_bucket(MINIO_BUCKET)

        saved_paths = []
        for data_dict in normalized_items:
            json_data = json.dumps(data_dict, ensure_ascii=False).encode('utf-8')
            file_stream = BytesIO(json_data)
            
            timestamp_str = datetime.utcnow().strftime('%Y%m%d%H%M%S%f')
            file_name = f"learning_outcomes_{data_dict['chuong_trinh']}_{timestamp_str}.json"
            object_path = f"bronze/api_learning/{file_name}"

            minio_client.put_object(
                bucket_name=MINIO_BUCKET,
                object_name=object_path,
                data=file_stream,
                length=len(json_data),
                content_type="application/json"
            )
            saved_paths.append(f"s3a://{MINIO_BUCKET}/{object_path}")

        # --- TỰ ĐỘNG TÍCH HỢP KÍCH HOẠT AIRFLOW ---
        trigger_airflow_api_pipeline("api_learning")

        return {
            "status": "success",
            "message": f"Đã tiếp nhận {len(normalized_items)} bản ghi và tự động kích hoạt Airflow Pipeline!",
            "total_records": len(normalized_items),
            "minio_paths": saved_paths
        }

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Lỗi khi xử lý dữ liệu API kết quả học tập: {str(e)}"
        )