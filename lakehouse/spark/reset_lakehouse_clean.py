# -*- coding: utf-8 -*-
"""
reset_lakehouse_clean.py
------------------------------------------------------------
Reset ĐỒNG BỘ cả Nessie catalog (Postgres) và dữ liệu MinIO, để
tránh tình trạng "orphaned table" (Nessie nhớ bảng cũ nhưng file
vật lý đã bị xoá) — vốn xảy ra khi bạn xoá dữ liệu MinIO thủ công
mà không xoá luôn lịch sử catalog tương ứng trong Nessie.

Script này làm 2 việc, LUÔN CÙNG LÚC:
  1. Xoá sạch nội dung bucket 'university-lakehouse' trên MinIO
     (chỉ xoá object, KHÔNG xoá bucket, để khỏi phải tạo lại).
  2. Reset database 'nessie_db' trong Postgres về trạng thái ban đầu
     (DROP + CREATE lại) -> Nessie sẽ tự tạo lại schema + branch
     'main' sạch khi container Nessie khởi động lại.

SAU KHI CHẠY SCRIPT NÀY, PHẢI RESTART CONTAINER NESSIE để nó tạo
lại schema mới trong database vừa reset:
    docker compose restart nessie

Cách dùng:
    pip install boto3 psycopg2-binary
    python reset_lakehouse_clean.py

CẢNH BÁO: Thao tác này xoá TOÀN BỘ dữ liệu Bronze/Silver/Gold và
lịch sử branch/tag Nessie. KHÔNG đụng tới Superset (database riêng
'superset_db' trong cùng Postgres, không bị ảnh hưởng).
------------------------------------------------------------
"""

import sys
import boto3
import psycopg2
from psycopg2 import sql

from env_config import (
    MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_BUCKET_NAME,
    PG_HOST, PG_PORT, PG_USER, PG_PASSWORD, PG_MAINTENANCE_DB, NESSIE_DB,
)

# --- Cấu hình MinIO ---
BUCKET_NAME = MINIO_BUCKET_NAME


def clear_minio_bucket():
    print(f"🗑️  Đang xoá toàn bộ object trong bucket '{BUCKET_NAME}'...")
    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )
    paginator = s3.get_paginator("list_objects_v2")
    total_deleted = 0
    for page in paginator.paginate(Bucket=BUCKET_NAME):
        objects = page.get("Contents", [])
        if not objects:
            continue
        keys = [{"Key": obj["Key"]} for obj in objects]
        s3.delete_objects(Bucket=BUCKET_NAME, Delete={"Objects": keys})
        total_deleted += len(keys)
    print(f"✅ Đã xoá {total_deleted} object khỏi MinIO.")


def reset_nessie_database():
    print(f"🗑️  Đang reset database '{NESSIE_DB}' trong Postgres...")
    conn = psycopg2.connect(
        host=PG_HOST, port=PG_PORT, user=PG_USER, password=PG_PASSWORD,
        dbname=PG_MAINTENANCE_DB,
    )
    conn.autocommit = True  # DROP/CREATE DATABASE không chạy được trong transaction
    cur = conn.cursor()

    # Ngắt hết kết nối đang mở tới nessie_db trước khi DROP (nếu không sẽ báo lỗi "in use")
    cur.execute(sql.SQL("""
        SELECT pg_terminate_backend(pid) FROM pg_stat_activity
        WHERE datname = %s AND pid <> pg_backend_pid();
    """), [NESSIE_DB])

    cur.execute(sql.SQL("DROP DATABASE IF EXISTS {}").format(sql.Identifier(NESSIE_DB)))
    cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(NESSIE_DB)))
    cur.execute(sql.SQL("GRANT ALL PRIVILEGES ON DATABASE {} TO {}").format(
        sql.Identifier(NESSIE_DB), sql.Identifier(PG_USER)
    ))

    cur.close()
    conn.close()
    print(f"✅ Đã reset '{NESSIE_DB}' về trạng thái sạch (chưa có schema, Nessie sẽ tự tạo lại khi khởi động).")


def main():
    print("=" * 60)
    print("⚠️  RESET ĐỒNG BỘ LAKEHOUSE (MinIO + Nessie catalog)")
    print("    Toàn bộ dữ liệu Bronze/Silver/Gold và lịch sử branch/tag sẽ bị xoá.")
    print("=" * 60)
    confirm = input("Gõ 'YES' để xác nhận tiếp tục: ")
    if confirm.strip() != "YES":
        print("❌ Đã huỷ, không có gì bị xoá.")
        sys.exit(0)

    clear_minio_bucket()
    reset_nessie_database()

    print("\n🌟 HOÀN TẤT. Bước tiếp theo BẮT BUỘC phải làm:")
    print("   1. docker compose restart nessie   (để Nessie tạo lại schema trong DB sạch)")
    print("   2. Chạy lại pipeline từ đầu: spark_ingest_bronze.py -> spark_bronze_to_silver.py -> spark_silver_to_gold.py")


if __name__ == "__main__":
    main()