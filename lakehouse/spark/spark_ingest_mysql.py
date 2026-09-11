# -*- coding: utf-8 -*-
"""
spark_ingest_mysql.py
------------------------------------------------------------

Luồng:
Data Connector (connector_id)
    -> PostgreSQL metadata + Fernet decrypt
    -> MySQL 8.0 (3 bảng quan hệ)
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

from env_config import (
    MINIO_ACCESS_KEY,
    MINIO_BUCKET_NAME,
    MINIO_ENDPOINT,
    MINIO_SECRET_KEY,
    MYSQL_JDBC_JAR,
)
from connector_runtime import load_connector, resolve_mysql_runtime_host
from kpi_bronze_mapping import build_kpi_bronze_dataframe

MYSQL_DRIVER = "com.mysql.cj.jdbc.Driver"
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


def read_mysql_table(spark, table_name, connector):
    """
    Đọc một bảng MySQL bằng credential của Data Connector tại runtime.

    Password chỉ tồn tại trong memory và không được ghi ra log.
    """
    runtime_host = resolve_mysql_runtime_host(connector)

    jdbc_url = (
        f"jdbc:mysql://{runtime_host}:{connector.port}/{connector.database_name}"
        "?useUnicode=true&characterEncoding=UTF-8"
        "&serverTimezone=Asia/Ho_Chi_Minh"
        "&useSSL=false&allowPublicKeyRetrieval=true"
    )

    print(
        f"📥 JDBC read: {connector.database_name}.{table_name} "
        f"@ {runtime_host}:{connector.port} "
        f"(connector_id={connector.id})"
    )

    return (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", table_name)
        .option("user", connector.username)
        .option("password", connector.password)
        .option("driver", MYSQL_DRIVER)
        .load()
    )


def build_bronze_dataframe(spark, run_id, connector):
    """Read the three KPI tables from live MySQL and map them to canonical Bronze."""
    source_config = connector.source_config or {}

    table_kq = "ket_qua_danh_gia"
    table_mt = "muc_tieu_kpi"
    table_dv = "don_vi"
    primary_table = source_config.get("primary_table") or table_kq

    df_kq = read_mysql_table(spark, table_kq, connector)
    df_mt = read_mysql_table(spark, table_mt, connector)
    df_dv = read_mysql_table(spark, table_dv, connector)

    mysql_source_uri = (
        f"mysql://{connector.database_name}/{primary_table}"
        f"?connector_id={connector.id}"
    )

    return build_kpi_bronze_dataframe(
        df_kq,
        df_mt,
        df_dv,
        run_id=run_id,
        source_name=SOURCE_NAME,
        source_uri=mysql_source_uri,
        evidence_type="mysql",
        source_identity=str(connector.id),
        source_connector_id=connector.id,
        source_connector_name=connector.name,
        source_table=table_kq,
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
    parser = argparse.ArgumentParser(
        description="Dynamic MySQL Data Connector -> MinIO Bronze"
    )
    parser.add_argument(
        "--run_id",
        type=str,
        default="",
        help="Airflow DAG Run ID",
    )
    parser.add_argument(
        "--connector_id",
        type=int,
        required=True,
        help="ID của Data Connector trong PostgreSQL",
    )
    args = parser.parse_args()

    sys.stdout.reconfigure(encoding="utf-8")

    # Đọc + decrypt credential trước khi khởi tạo Spark để fail-fast.
    connector = load_connector(args.connector_id)

    if connector.connector_type.strip().upper() != "MYSQL":
        raise RuntimeError(
            f"Connector ID {connector.id} không phải MYSQL."
        )

    runtime_host = resolve_mysql_runtime_host(connector)

    print("🔌 Dynamic MySQL Connector")
    print(f"   connector_id: {connector.id}")
    print(f"   name: {connector.name}")
    print(f"   database: {connector.database_name}")
    print(f"   runtime_host: {runtime_host}:{connector.port}")
    print("   credential: encrypted-at-rest / decrypted-in-memory")

    spark = get_spark_session()
    try:
        df_bronze = build_bronze_dataframe(
            spark,
            args.run_id,
            connector,
        )

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
            "source_connector_id",
            "source_connector_name",
        ).show(10, truncate=False)

        output_key, row_count = write_single_parquet_to_minio(df_bronze)

        if output_key:
            print(
                f"✨ HOÀN THÀNH MYSQL INGEST: {row_count} dòng, "
                f"s3://{MINIO_BUCKET_NAME}/{output_key} "
                f"(connector_id={connector.id})"
            )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
