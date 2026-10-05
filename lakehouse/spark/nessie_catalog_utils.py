# -*- coding: utf-8 -*-
"""
nessie_catalog_utils.py
------------------------------------------------------------
Các hàm tiện ích dùng chung cho việc quản lý phiên bản catalog
qua Nessie (branch / merge / tag) trong các pipeline Spark.

[CẬP NHẬT theo Yêu cầu 4] check_quality_silver() bổ sung 2 kiểm tra mới:
  - Trùng khóa nghiệp vụ (ma_chi_tieu, quy_danh_gia): phát hiện lỗi ingest lặp
    cho cùng 1 kỳ (khác với checksum trùng ở chỗ: đây là trùng logic, không
    nhất thiết trùng checksum).
  - quy_danh_gia = 'UNKNOWN_KY': dữ liệu không xác định được kỳ đánh giá từ
    nội dung file (xem spark_ingest_bronze.py) -> KHÔNG merge vào main, giữ
    branch để admin kiểm tra thủ công qua CatalogHistoryTimeline.jsx.
------------------------------------------------------------
"""

from datetime import datetime

import requests
from env_config import NESSIE_API_URL

CATALOG_NAME = "lakehouse"  # trùng với spark.sql.catalog.lakehouse
QUY_DANH_GIA_UNKNOWN = "UNKNOWN_KY"


def delete_nessie_orphaned_key(table_name: str, branch_name: str = "main"):
    """
    Tự động xóa orphaned content key trong Nessie Catalog khi file metadata S3 bị mất/hỏng.
    table_name ví dụ: "lakehouse.silver.kpi_cusc_master" -> key elements: ["silver", "kpi_cusc_master"]
    """
    try:
        parts = table_name.split(".")
        if len(parts) >= 2:
            key_elements = parts[1:]
        else:
            key_elements = parts

        url_tree = f"{NESSIE_API_URL}/trees/tree/{branch_name}"
        res = requests.get(url_tree, timeout=5)
        if res.status_code != 200:
            return False
        hash_val = res.json().get("hash")

        commit_url = f"{NESSIE_API_URL}/trees/branch/{branch_name}/commit?expectedHash={hash_val}"
        payload = {
            "commitMeta": {"message": f"Auto-purge orphaned key {table_name}"},
            "operations": [
                {
                    "type": "DELETE",
                    "key": {"elements": key_elements}
                }
            ]
        }
        resp = requests.post(commit_url, json=payload, timeout=5)
        if resp.status_code in [200, 204]:
            print(f"🗑️ Đã tự động xóa orphaned key '{key_elements}' khỏi Nessie branch '{branch_name}'.")
            return True
    except Exception as e:
        print(f"⚠️ Không thể xóa orphaned key qua Nessie API: {e}")
    return False


def make_branch_name(prefix: str) -> str:
    """Sinh tên branch duy nhất theo thời điểm chạy job.
    Nessie ref name không được chứa dấu ':' nên dùng định dạng YYYYMMDD_HHMMSS."""
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"{prefix}_{ts}"


def create_branch(spark, branch_name: str, from_ref: str = "main"):
    """Tạo branch mới trong Nessie, xuất phát từ 1 ref có sẵn (mặc định main)."""
    print(f"🌿 Đang tạo branch Nessie: '{branch_name}' (từ '{from_ref}')...")
    spark.sql(f"CREATE BRANCH IF NOT EXISTS {branch_name} IN {CATALOG_NAME} FROM {from_ref}")
    print(f"✅ Đã tạo branch '{branch_name}'.")


def use_branch(spark, branch_name: str):
    """Chuyển context làm việc của Spark session sang branch chỉ định."""
    spark.sql(f"USE REFERENCE {branch_name} IN {CATALOG_NAME}")
    print(f"➡️  Đang làm việc trên branch: '{branch_name}'")


def use_main(spark):
    """Quay trở lại làm việc trên nhánh main."""
    spark.sql(f"USE REFERENCE main IN {CATALOG_NAME}")


def merge_branch_to_main(spark, branch_name: str):
    """Merge branch vào main sau khi đã pass data quality check."""
    print(f"🔀 Đang merge branch '{branch_name}' vào 'main'...")
    spark.sql(f"MERGE BRANCH {branch_name} INTO main IN {CATALOG_NAME}")
    print(f"✅ Đã merge '{branch_name}' -> 'main' thành công.")


def drop_branch(spark, branch_name: str):
    """Xoá branch (TUỲ CHỌN, dùng khi muốn dọn dẹp sau khi đã merge xong)."""
    spark.sql(f"DROP BRANCH IF EXISTS {branch_name} IN {CATALOG_NAME}")
    print(f"🗑️  Đã xoá branch '{branch_name}'.")


def create_tag(spark, tag_name: str, from_ref: str = "main"):
    """Gắn tag (mốc phiên bản cố định) lên 1 ref."""
    print(f"🏷️  Đang tạo tag '{tag_name}' từ '{from_ref}'...")
    spark.sql(f"CREATE TAG IF NOT EXISTS {tag_name} IN {CATALOG_NAME} FROM {from_ref}")
    print(f"✅ Đã tạo tag '{tag_name}'. Time-travel bằng: SELECT * FROM <bảng>@{tag_name}")


class DataQualityError(Exception):
    """Raise khi dữ liệu trên branch không đạt yêu cầu chất lượng tối thiểu."""
    pass


def check_quality_silver(spark, table_name: str):
    """
    Kiểm tra chất lượng dữ liệu tối thiểu cho bảng Silver TRÊN BRANCH hiện tại:
      1. Bảng phải có ít nhất 1 dòng dữ liệu (không rỗng)
      2. Không có bản ghi thiếu ma_chi_tieu hoặc ket_qua_he_thong (NULL)
      3. Không có checksum_sha256 bị trùng lặp
      4. [MỚI] Không có tổ hợp (ma_chi_tieu, quy_danh_gia) bị trùng -> nghi ngờ
         ingest lặp cho cùng 1 kỳ đánh giá.
      5. [MỚI] Không còn bản ghi nào có quy_danh_gia = 'UNKNOWN_KY' -> không
         xác định được kỳ đánh giá từ nội dung file nguồn, cần admin xử lý
         thủ công trước khi cho phép merge vào main.
    Raise DataQualityError nếu bất kỳ điều kiện nào không đạt.
    """
    print("🔍 Đang kiểm tra chất lượng dữ liệu Silver trên branch hiện tại...")
    df = spark.table(table_name)

    total_rows = df.count()
    if total_rows == 0:
        raise DataQualityError("Bảng Silver rỗng, không có dữ liệu để merge.")

    null_key_rows = df.filter(
        "ma_chi_tieu IS NULL OR ket_qua_he_thong IS NULL"
    ).count()
    if null_key_rows > 0:
        raise DataQualityError(
            f"Phát hiện {null_key_rows} bản ghi thiếu ma_chi_tieu hoặc ket_qua_he_thong."
        )

    total_checksum = df.select("checksum_sha256").count()
    distinct_checksum = df.select("checksum_sha256").distinct().count()
    if total_checksum != distinct_checksum:
        dup_count = total_checksum - distinct_checksum
        raise DataQualityError(
            f"Phát hiện {dup_count} bản ghi có checksum_sha256 bị trùng lặp."
        )

    # [MỚI - Yêu cầu 4] Khóa nghiệp vụ thật sự là (ma_chi_tieu, quy_danh_gia)
    dup_key_rows = (
        df.groupBy("ma_chi_tieu", "quy_danh_gia")
        .count()
        .filter("count > 1")
        .count()
    )
    if dup_key_rows > 0:
        raise DataQualityError(
            f"Phát hiện {dup_key_rows} tổ hợp (ma_chi_tieu, quy_danh_gia) bị trùng — "
            f"có thể do dữ liệu bị nạp lại/ghi đè cho cùng 1 kỳ đánh giá. "
            f"Kiểm tra lại nguồn dữ liệu trước khi merge."
        )

    # [MỚI - Yêu cầu 4] Không cho phép merge dữ liệu chưa xác định được kỳ đánh giá
    unknown_ky_rows = df.filter(f"quy_danh_gia = '{QUY_DANH_GIA_UNKNOWN}'").count()
    if unknown_ky_rows > 0:
        raise DataQualityError(
            f"Phát hiện {unknown_ky_rows} bản ghi không xác định được kỳ đánh giá "
            f"(quy_danh_gia = '{QUY_DANH_GIA_UNKNOWN}'). Cần kiểm tra thủ công file "
            f"nguồn (xem cột file_nguon) trước khi merge vào main."
        )

    print(f"✅ Dữ liệu đạt chất lượng: {total_rows} dòng, không NULL khoá chính, "
          f"không trùng checksum, không trùng (ma_chi_tieu, quy_danh_gia), "
          f"không còn kỳ đánh giá UNKNOWN_KY.")
    return True


def check_quality_gold(spark, summary_table: str, detail_table: str):
    """
    Kiểm tra chất lượng tối thiểu cho 2 bảng Gold TRÊN BRANCH hiện tại:
      1. Bảng tổng hợp không rỗng
      2. Tỷ lệ hoàn thành (%) phải nằm trong khoảng hợp lệ [0, 100]
      3. Bảng chi tiết không rỗng
    """
    print("🔍 Đang kiểm tra chất lượng Data Mart Gold trên branch hiện tại...")

    df_summary = spark.table(summary_table)
    summary_count = df_summary.count()
    if summary_count == 0:
        raise DataQualityError("Bảng Gold tổng hợp (kpi_tong_hop_don_vi) rỗng.")

    invalid_ratio_rows = df_summary.filter(
        "ty_le_hoan_thanh_phan_tram < 0 OR ty_le_hoan_thanh_phan_tram > 100"
    ).count()
    if invalid_ratio_rows > 0:
        raise DataQualityError(
            f"Phát hiện {invalid_ratio_rows} dòng có tỷ lệ hoàn thành ngoài khoảng [0, 100]."
        )

    df_detail = spark.table(detail_table)
    detail_count = df_detail.count()
    if detail_count == 0:
        raise DataQualityError("Bảng Gold chi tiết (kpi_chi_tiet_dashboard) rỗng.")

    print(f"✅ Data Mart Gold đạt chất lượng: "
          f"{summary_count} dòng tổng hợp, {detail_count} dòng chi tiết.")
    return True