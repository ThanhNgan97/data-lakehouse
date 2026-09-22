# -*- coding: utf-8 -*-
"""
nessie_tag_release.py
------------------------------------------------------------
Script chạy TAY (hoặc lên lịch bằng Task Scheduler/cron) vào cuối mỗi
kỳ báo cáo, dùng để gắn TAG cố định lên nhánh 'main' của Nessie, phục vụ
time-travel và làm mốc audit chính thức.

Cách dùng:
    python nessie_tag_release.py Q1_2026_final

Sau khi có tag, có thể truy vấn dữ liệu tại đúng thời điểm đó bằng:
    SELECT * FROM lakehouse.silver.kpi_cusc_master@Q1_2026_final
    SELECT * FROM lakehouse.gold.kpi_tong_hop_don_vi@Q1_2026_final

Hoặc rollback (đưa main quay lại trạng thái của tag) bằng lệnh Nessie CLI/API
ASSIGN BRANCH main TO Q1_2026_final IN lakehouse (dùng khi phát hiện lỗi sau
khi đã merge và cần khôi phục khẩn cấp).
------------------------------------------------------------
"""

import os
import sys
from pyspark.sql import SparkSession

from nessie_catalog_utils import create_tag
from env_config import (
    MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_ENDPOINT,
    NESSIE_API_URL, HADOOP_HOME, SPARK_LOCAL_IP,
)

os.environ["HADOOP_HOME"]           = HADOOP_HOME
os.environ["PATH"]                  = os.path.join(HADOOP_HOME, "bin") + ";" + os.environ.get("PATH", "")
os.environ["AWS_ACCESS_KEY_ID"]     = MINIO_ACCESS_KEY
os.environ["AWS_SECRET_ACCESS_KEY"] = MINIO_SECRET_KEY
os.environ["SPARK_LOCAL_IP"]        = SPARK_LOCAL_IP
os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3,"
    "org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.77.1 "
    "pyspark-shell"
)


def get_spark_session():
    return SparkSession.builder \
        .appName("Nessie_Tag_Release") \
        .config("spark.driver.host", SPARK_LOCAL_IP) \
        .config("spark.driver.bindAddress", SPARK_LOCAL_IP) \
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,"
                "org.projectnessie.spark.extensions.NessieSparkSessionExtensions") \
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.lakehouse.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog") \
        .config("spark.sql.catalog.lakehouse.uri", NESSIE_API_URL) \
        .config("spark.sql.catalog.lakehouse.warehouse", "s3a://university-lakehouse/iceberg-lakehouse") \
        .config("spark.sql.catalog.lakehouse.s3.endpoint", MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .getOrCreate()


def main():
    if len(sys.argv) < 2:
        print("❌ Thiếu tên tag. Cách dùng: python nessie_tag_release.py <ten_tag>")
        print("   Ví dụ: python nessie_tag_release.py Q1_2026_final")
        sys.exit(1)

    tag_name = sys.argv[1]
    spark = get_spark_session()
    try:
        create_tag(spark, tag_name, from_ref="main")
        print("\n💡 Time-travel ví dụ:")
        print(f"   SELECT * FROM lakehouse.silver.kpi_cusc_master@{tag_name}")
        print(f"   SELECT * FROM lakehouse.gold.kpi_tong_hop_don_vi@{tag_name}")
        print(f"   SELECT * FROM lakehouse.gold.kpi_chi_tiet_dashboard@{tag_name}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()