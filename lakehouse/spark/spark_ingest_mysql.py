# -*- coding: utf-8 -*-
"""
spark_ingest_mysql.py
------------------------------------------------------------

Luồng:
MySQL 8.0 (3 bảng quan hệ)
    -> PySpark JDBC
    -> Join + chuẩn hóa schema Bronze
    -> Parquet single-object
    -> MinIO: bronze/data_mysql_extracted_<timestamp>.parquet

Ghi chú:
- Các giá trị nghiệp vụ thô như "100%", "3,5", "Chưa đến kỳ đánh giá"
  được giữ nguyên ở các cột text.
- muc_dang_ky_numeric / muc_dat_numeric chỉ được parse khi toàn bộ giá trị
  là một số đơn giản (có thể có % và dấu phẩy thập phân).
- Business merge vào Iceberg Silver được thực hiện ở spark_bronze_to_silver.py,
  không thực hiện tại job ingestion này.
"""

import argparse
import io
import os
import sys
from datetime import datetime

import boto3
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    concat_ws,
    coalesce,
    current_timestamp,
    lit,
    lower,
    regexp_replace,
    sha2,
    trim,
    upper,
    when,
)

from env_config import (
    MINIO_ACCESS_KEY,
    MINIO_BUCKET_NAME,
    MINIO_ENDPOINT,
    MINIO_SECRET_KEY,
    MYSQL_DATABASE,
    MYSQL_HOST,
    MYSQL_JDBC_JAR,
    MYSQL_PASSWORD,
    MYSQL_PORT,
    MYSQL_USER,
)

MYSQL_DRIVER = "com.mysql.cj.jdbc.Driver"
MYSQL_SOURCE_URI = f"mysql://{MYSQL_DATABASE}/ket_qua_danh_gia"
BRONZE_PREFIX = "bronze/"
SOURCE_NAME = "MYSQL_RDBMS"


def get_spark_session():
    """Khởi tạo Spark có nạp MySQL Connector/J."""
    if not os.path.exists(MYSQL_JDBC_JAR):
        raise FileNotFoundError(
            f"Không tìm thấy MySQL JDBC driver: {MYSQL_JDBC_JAR}. "
            "Hãy rebuild custom-airflow image từ airflow.Dockerfile."
        )

    spark = (
        SparkSession.builder
        .appName("MySQL_To_Bronze")
        .config("spark.jars", MYSQL_JDBC_JAR)
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        verify=False,
    )


def read_mysql_table(spark, table_name):
    jdbc_url = (
        f"jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}"
        "?useUnicode=true&characterEncoding=UTF-8"
        "&serverTimezone=Asia/Ho_Chi_Minh"
        "&useSSL=false&allowPublicKeyRetrieval=true"
    )

    print(f"📥 JDBC read: {MYSQL_DATABASE}.{table_name} @ {MYSQL_HOST}:{MYSQL_PORT}")

    return (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", table_name)
        .option("user", MYSQL_USER)
        .option("password", MYSQL_PASSWORD)
        .option("driver", MYSQL_DRIVER)
        .load()
    )


def parse_simple_numeric(column_name):
    """
    Chỉ parse khi toàn bộ chuỗi là một số đơn giản, ví dụ:
      100% -> 100.0
      3,69 -> 3.69
      9.5 -> 9.5

    Chuỗi nghiệp vụ phức tạp như:
      "Không phát sinh sự cố"
      "99,5% (...) / 100% (...)"
    sẽ trả NULL.
    """
    cleaned = regexp_replace(trim(col(column_name)), ",", ".")
    cleaned = regexp_replace(cleaned, "%", "")

    return when(
        cleaned.rlike(r"^-?[0-9]+(?:\.[0-9]+)?$"),
        cleaned.cast("double"),
    ).otherwise(lit(None).cast("double"))


def normalize_status(column_name):
    raw = upper(trim(col(column_name)))

    return (
        when(raw.contains("CHƯA ĐẾN KỲ"), lit("CHƯA ĐẾN KỲ ĐÁNH GIÁ"))
        .when(raw.contains("KHÔNG ĐẠT"), lit("KHÔNG ĐẠT"))
        .when(raw.contains("ĐẠT"), lit("ĐẠT"))
        .otherwise(raw)
    )


def build_bronze_dataframe(spark, run_id):
    """
    Join 3 bảng MySQL thành schema KPI tương thích với Bronze hiện tại.
    """
    df_kq = read_mysql_table(spark, "ket_qua_danh_gia").alias("kq")
    df_mt = read_mysql_table(spark, "muc_tieu_kpi").alias("mt")
    df_dv = read_mysql_table(spark, "don_vi").alias("dv")

    joined = (
        df_kq
        .join(df_mt, col("kq.ma_chi_tieu") == col("mt.ma_chi_tieu"), "left")
        .join(df_dv, col("kq.ma_don_vi") == col("dv.ma_don_vi"), "left")
    )

    df = joined.select(
        upper(trim(col("kq.ma_chi_tieu"))).alias("ma_chi_tieu"),
        upper(trim(col("kq.ma_don_vi"))).alias("nhom_don_vi"),
        upper(trim(col("kq.quy_danh_gia"))).alias("quy_danh_gia"),
        trim(col("mt.noi_dung")).alias("noi_dung_muc_tieu"),
        trim(col("mt.dinh_ky")).alias("dinh_ky_thu_thap"),
        trim(col("kq.muc_dang_ky")).alias("muc_dang_ky"),
        trim(col("kq.muc_dat")).alias("muc_dat"),
        normalize_status("kq.ket_qua_he_thong").alias("ket_qua_he_thong"),
        coalesce(trim(col("kq.nguyen_nhan")), lit("")).alias("nguyen_nhan"),
        col("kq.updated_at").alias("source_updated_at"),
    )

    df = (
        df
        .withColumn("muc_dang_ky_numeric", parse_simple_numeric("muc_dang_ky"))
        .withColumn("muc_dat_numeric", parse_simple_numeric("muc_dat"))
        .withColumn("hanh_dong_khac_phuc", lit(""))
        .withColumn("file_nguon", lit(MYSQL_SOURCE_URI))
        .withColumn("minh_chung_type", lit("mysql"))
        .withColumn("minh_chung_path", lit(MYSQL_SOURCE_URI))
        .withColumn("nguon_du_lieu", lit(SOURCE_NAME))
        .withColumn("thoi_gian_ingest_bronze", current_timestamp())
        .withColumn("run_id", lit(run_id or ""))
    )

    # Checksum ổn định theo nội dung nghiệp vụ của bản ghi MySQL.
    df = df.withColumn(
        "checksum_sha256",
        sha2(
            concat_ws(
                "||",
                lit(SOURCE_NAME),
                col("ma_chi_tieu"),
                col("nhom_don_vi"),
                col("quy_danh_gia"),
                coalesce(col("muc_dang_ky"), lit("")),
                coalesce(col("muc_dat"), lit("")),
                coalesce(col("ket_qua_he_thong"), lit("")),
                coalesce(col("nguyen_nhan"), lit("")),
            ),
            256,
        ),
    )

    return df.select(
        "file_nguon",
        "nguon_du_lieu",
        "ma_chi_tieu",
        "nhom_don_vi",
        "quy_danh_gia",
        "noi_dung_muc_tieu",
        "dinh_ky_thu_thap",
        "muc_dang_ky",
        "muc_dang_ky_numeric",
        "muc_dat",
        "muc_dat_numeric",
        "ket_qua_he_thong",
        "nguyen_nhan",
        "hanh_dong_khac_phuc",
        "minh_chung_type",
        "minh_chung_path",
        "checksum_sha256",
        "source_updated_at",
        "thoi_gian_ingest_bronze",
        "run_id",
    )


def write_single_parquet_to_minio(df):
    """
    Giữ cùng convention với spark_ingest_bronze.py hiện tại:
    tạo một object Parquet trực tiếp trong bronze/.
    Với batch KPI nhỏ/OLTP PoC, cách này đơn giản và dễ archive ở bước Silver.
    """
    row_count = df.count()
    if row_count == 0:
        print("ℹ️ MySQL không có dữ liệu KPI để ingest.")
        return None, 0

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_key = f"{BRONZE_PREFIX}data_mysql_extracted_{timestamp}.parquet"

    pandas_df = df.toPandas()
    parquet_buffer = io.BytesIO()
    pandas_df.to_parquet(
        parquet_buffer,
        index=False,
        engine="pyarrow",
        coerce_timestamps="us",
        allow_truncated_timestamps=True,
    )

    s3_client = get_s3_client()
    s3_client.put_object(
        Bucket=MINIO_BUCKET_NAME,
        Key=output_key,
        Body=parquet_buffer.getvalue(),
        ContentType="application/octet-stream",
    )

    print(f"✅ MySQL -> Bronze: {output_key} ({row_count} dòng)")
    return output_key, row_count


def main():
    parser = argparse.ArgumentParser(description="MySQL OLTP -> MinIO Bronze")
    parser.add_argument("--run_id", type=str, default="", help="Airflow DAG Run ID")
    args = parser.parse_args()

    sys.stdout.reconfigure(encoding="utf-8")

    spark = get_spark_session()
    try:
        df_bronze = build_bronze_dataframe(spark, args.run_id)

        print("📊 Preview dữ liệu MySQL sau mapping:")
        df_bronze.select(
            "ma_chi_tieu",
            "nhom_don_vi",
            "quy_danh_gia",
            "muc_dang_ky",
            "muc_dat",
            "muc_dat_numeric",
            "ket_qua_he_thong",
            "nguon_du_lieu",
        ).show(10, truncate=False)

        output_key, row_count = write_single_parquet_to_minio(df_bronze)

        if output_key:
            print(
                f"✨ HOÀN THÀNH MYSQL INGEST: {row_count} dòng, "
                f"s3://{MINIO_BUCKET_NAME}/{output_key}"
            )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
