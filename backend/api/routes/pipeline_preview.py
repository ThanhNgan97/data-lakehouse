# -*- coding: utf-8 -*-
"""
api/routes/pipeline_preview.py
------------------------------------------------------------
Router FastAPI: cung cấp dữ liệu xem trước (preview) cho từng tầng
Bronze / Silver / Gold, phục vụ UI "Pipeline Data Explorer" ở frontend.

[CẬP NHẬT - Drill-down theo đơn vị]
  Thêm query param `nhom_don_vi` (tùy chọn) cho endpoint /gold/preview.
  Khi FE truyền tham số này (VD người dùng click vào 1 dòng của bảng
  'kpi_tong_hop_don_vi'), API sẽ lọc thêm WHERE nhom_don_vi = '<...>'
  trên bảng Gold tương ứng, để hiển thị đúng phần chi tiết của đơn vị đó
  (từ tổng -> chi tiết, không cần đi qua Superset).

  Vì `nhom_don_vi` là input tự do từ URL (không phải identifier như
  `table` đã được whitelist qua GOLD_TABLES), giá trị này được validate
  chặt bằng regex trước khi đưa vào câu SQL, để tránh SQL injection
  (không dùng f-string nối trực tiếp input người dùng vào SQL).
------------------------------------------------------------
"""

import io
import os
import re
import boto3
import pandas as pd
from fastapi import APIRouter, HTTPException, Depends, Query
from trino.dbapi import connect as trino_connect

from api.dependencies import get_current_user

router = APIRouter()

# --- Cấu hình (đọc từ biến môi trường, khớp docker-compose hiện có) ---
MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET     = os.getenv("MINIO_BUCKET_NAME", "university-lakehouse")

TRINO_HOST     = os.getenv("TRINO_HOST", "trino")
TRINO_PORT     = int(os.getenv("TRINO_PORT", "8080"))
TRINO_USER     = os.getenv("TRINO_USER", "admin")
TRINO_CATALOG  = "lakehouse"

# Whitelist bảng Gold mặc định (dành cho legacy KPI)
GOLD_TABLES = {
    "kpi_tong_hop_don_vi":     "Tổng hợp KPI theo đơn vị",
    "kpi_chi_tiet_dashboard":  "Chi tiết đầy đủ KPI",
    "kpi_so_sanh_ky":          "So sánh tăng/giảm giữa các kỳ",
    "dm_chi_tieu":             "Chú thích / Data Dictionary mã chỉ tiêu",
    "kpi_du_doan_tuong_lai":   "Dự đoán kết quả tương lai",
}

# Regex kiểm tra an toàn tên bảng động (chống SQL injection)
TABLE_NAME_PATTERN = re.compile(r"^[a-zA-Z0-9_]{1,64}$")

# [MỚI] Regex validate giá trị nhom_don_vi truyền vào từ query param.
NHOM_DON_VI_PATTERN = re.compile(r"^[A-ZĐ]{1,20}$")


def _validate_table_name(table_name: str) -> str:
    """Xác thực tên bảng an toàn theo chuẩn identifier."""
    clean_name = table_name.strip()
    if not TABLE_NAME_PATTERN.match(clean_name):
        raise HTTPException(
            status_code=400,
            detail=f"Tên bảng '{table_name}' không hợp lệ. Chỉ chấp nhận chữ cái, số và dấu gạch dưới.",
        )
    return clean_name


def _validate_nhom_don_vi(value: str) -> str:
    """Kiểm tra định dạng mã đơn vị, raise HTTPException nếu không hợp lệ."""
    if not NHOM_DON_VI_PATTERN.match(value):
        raise HTTPException(
            status_code=400,
            detail=(
                f"Mã đơn vị '{value}' không hợp lệ. "
                f"Chỉ chấp nhận chữ hoa (VD: PM, HT, VP, QTCL, RD, ĐT)."
            ),
        )
    return value


def _get_s3_client():
    return boto3.client(
        "s3", endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY, aws_secret_access_key=MINIO_SECRET_KEY,
    )


def _run_trino_query(sql: str):
    conn = trino_connect(host=TRINO_HOST, port=TRINO_PORT, user=TRINO_USER, catalog=TRINO_CATALOG)
    cur = conn.cursor()
    cur.execute(sql)
    columns = [desc[0] for desc in cur.description]
    rows = cur.fetchall()
    return columns, rows


@router.get("/status")
def get_pipeline_status(current_user=Depends(get_current_user)):
    """Trạng thái từng tầng (có dữ liệu hay chưa), để frontend tô xanh/xám
    từng node giống Airflow Graph View."""
    status = {"bronze": False, "silver": False, "gold": False}

    try:
        s3 = _get_s3_client()
        resp = s3.list_objects_v2(Bucket=MINIO_BUCKET, Prefix="bronze/data_extracted_", MaxKeys=1)
        status["bronze"] = "Contents" in resp and len(resp["Contents"]) > 0
    except Exception:
        status["bronze"] = False

    try:
        _, rows = _run_trino_query("SELECT COUNT(*) FROM silver.kpi_cusc_master")
        status["silver"] = rows[0][0] > 0
    except Exception:
        status["silver"] = False

    try:
        _, rows = _run_trino_query("SELECT COUNT(*) FROM gold.kpi_chi_tiet_dashboard")
        status["gold"] = rows[0][0] > 0
    except Exception:
        status["gold"] = False

    return status


@router.get("/bronze/preview")
def preview_bronze(limit: int = Query(50, ge=1, le=500), current_user=Depends(get_current_user)):
    """Đọc file Parquet Bronze MỚI NHẤT trên MinIO (không qua Trino được vì
    Bronze chỉ là file thô, chưa phải bảng Iceberg)."""
    s3 = _get_s3_client()
    resp_bronze = s3.list_objects_v2(Bucket=MINIO_BUCKET, Prefix="bronze/data_extracted_")
    resp_archive = s3.list_objects_v2(Bucket=MINIO_BUCKET, Prefix="bronze_archive/data_extracted_")

    contents = []
    if "Contents" in resp_bronze and resp_bronze["Contents"]:
        contents.extend(resp_bronze["Contents"])
    if "Contents" in resp_archive and resp_archive["Contents"]:
        contents.extend(resp_archive["Contents"])

    if not contents:
        raise HTTPException(status_code=404, detail="Chưa có file Parquet nào ở tầng Bronze hoặc Archive.")

    latest = max(contents, key=lambda o: o["LastModified"])
    file_bytes = s3.get_object(Bucket=MINIO_BUCKET, Key=latest["Key"])["Body"].read()
    df = pd.read_parquet(io.BytesIO(file_bytes))
    return {
        "layer": "bronze",
        "source_file": latest["Key"],
        "last_modified": latest["LastModified"].isoformat(),
        "total_rows": len(df),
        "columns": df.columns.tolist(),
        "rows": df.head(limit).fillna("").to_dict(orient="records"),
    }


@router.get("/silver/preview")
def preview_silver(limit: int = Query(50, ge=1, le=500), current_user=Depends(get_current_user)):
    """Query trực tiếp bảng Iceberg Silver qua Trino."""
    try:
        columns, rows = _run_trino_query(
            f"SELECT * FROM silver.kpi_cusc_master ORDER BY thoi_gian_ingest_silver DESC LIMIT {limit}"
        )
        _, count_rows = _run_trino_query("SELECT COUNT(*) FROM silver.kpi_cusc_master")
        total_rows = count_rows[0][0]
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Không truy vấn được bảng Silver qua Trino: {e}")

    return {
        "layer": "silver",
        "source_table": "lakehouse.silver.kpi_cusc_master",
        "total_rows": total_rows,
        "columns": columns,
        "rows": [dict(zip(columns, r)) for r in rows],
    }


@router.get("/gold/preview")
def preview_gold(
    table: str = Query(..., description="Tên bảng trong schema gold"),
    limit: int = Query(50, ge=1, le=500),
    nhom_don_vi: str | None = Query(
        default=None,
        description="[Tùy chọn] Lọc kết quả theo mã đơn vị (VD: PM, HT, VP, QTCL, RD, ĐT).",
    ),
    current_user=Depends(get_current_user),
):
    """Query bất kỳ bảng Gold nào qua Trino (hỗ trợ cả bảng KPI cũ lẫn bảng động mới sinh từ AI)."""
    safe_table = _validate_table_name(table)

    where_clause = ""
    if nhom_don_vi:
        safe_value = _validate_nhom_don_vi(nhom_don_vi.strip().upper())
        where_clause = f"WHERE nhom_don_vi = '{safe_value}'"

    try:
        # Kiểm tra xem bảng có tồn tại trong Trino gold schema không
        _, check_rows = _run_trino_query(f"SHOW TABLES FROM gold LIKE '{safe_table}'")
        if not check_rows:
            raise HTTPException(status_code=404, detail=f"Bảng 'gold.{safe_table}' không tồn tại trong Trino catalog.")

        # Lấy danh sách cột để sắp xếp phù hợp
        columns, sample_rows = _run_trino_query(f"SELECT * FROM gold.{safe_table} {where_clause} LIMIT {limit}")
        _, count_rows = _run_trino_query(f"SELECT COUNT(*) FROM gold.{safe_table} {where_clause}")
        total_rows = count_rows[0][0] if count_rows else 0
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Không truy vấn được bảng gold.{safe_table} qua Trino: {e}")

    table_label = GOLD_TABLES.get(safe_table, f"Bảng dữ liệu {safe_table}")

    return {
        "layer": "gold",
        "source_table": f"lakehouse.gold.{safe_table}",
        "table_label": table_label,
        "total_rows": total_rows,
        "columns": columns,
        "rows": [dict(zip(columns, r)) for r in sample_rows],
        "filter_applied": {"nhom_don_vi": nhom_don_vi} if nhom_don_vi else None,
    }


@router.get("/gold/tables")
def list_gold_tables(current_user=Depends(get_current_user)):
    """Danh sách các bảng Gold hiện có trong Trino (kết hợp bảng mặc định và bảng mới sinh)."""
    tables = []
    seen = set()
    for k, v in GOLD_TABLES.items():
        tables.append({"table": k, "label": v, "is_legacy": True})
        seen.add(k)

    try:
        _, dynamic_rows = _run_trino_query("SHOW TABLES FROM gold")
        for row in dynamic_rows:
            tbl_name = row[0]
            if tbl_name not in seen:
                tables.append({"table": tbl_name, "label": f"Bảng tổng hợp {tbl_name}", "is_legacy": False})
                seen.add(tbl_name)
    except Exception:
        pass  # Nếu Trino chưa sẵn sàng thì trả về bảng mặc định

    return tables


@router.get("/catalog/tables")
def list_all_catalog_tables(current_user=Depends(get_current_user)):
    """[MỚI] Tự động khám phá toàn bộ các bảng trong Lakehouse (cả Silver và Gold)."""
    catalog = {"silver": [], "gold": []}
    try:
        _, silver_rows = _run_trino_query("SHOW TABLES FROM silver")
        catalog["silver"] = [r[0] for r in silver_rows]
    except Exception:
        pass

    try:
        _, gold_rows = _run_trino_query("SHOW TABLES FROM gold")
        catalog["gold"] = [r[0] for r in gold_rows]
    except Exception:
        pass

    return catalog


@router.get("/gold/units")
def list_gold_units(current_user=Depends(get_current_user)):
    """[MỚI] Danh sách các mã đơn vị (nhom_don_vi) + tên phòng ban đầy đủ hiện có
    trong bảng kpi_chi_tiet_dashboard, để frontend hiện dropdown lọc nhanh
    (không bắt buộc phải click từ bảng tổng hợp mới lọc được)."""
    try:
        columns, rows = _run_trino_query(
            "SELECT DISTINCT nhom_don_vi, ten_phong_ban FROM gold.kpi_chi_tiet_dashboard "
            "ORDER BY nhom_don_vi"
        )
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Không lấy được danh sách đơn vị: {e}")

    return [{"nhom_don_vi": r[0], "ten_phong_ban": r[1]} for r in rows]