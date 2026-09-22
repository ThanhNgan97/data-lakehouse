# -*- coding: utf-8 -*-
"""
spark_api_gold_aggregation.py (Có Nessie Catalog Versioning)
------------------------------------------------------------
TẦNG GOLD (APACHE SPARK + ICEBERG) - TỔNG HỢP DỮ LIỆU API (TIẾN ĐỘ & KẾT QUẢ HỌC TẬP)
"""

import os
import sys
from datetime import datetime
import argparse

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
    "--driver-java-options \"-Djava.net.preferIPv4Stack=true\" "
    "--packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3,"
    "org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.77.1,"
    "org.apache.hadoop:hadoop-aws:3.3.4 "
    "pyspark-shell"
)

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, sum, count, round, current_timestamp

from nessie_catalog_utils import (
    make_branch_name,
    create_branch,
    use_branch,
    use_main,
    merge_branch_to_main,
    delete_nessie_orphaned_key,
)
from openmetadata_lineage_utils import get_client, push_lineage_safe

GOLD_TEACHING_SUMMARY = "lakehouse.gold.kpi_api_teaching_summary"
GOLD_LEARNING_SUMMARY = "lakehouse.gold.kpi_api_learning_summary"

def get_spark_session():
    print("Khởi tạo Spark Engine tính toán số liệu Gold cho API...")
    spark = (
        SparkSession.builder
        .appName("API_Silver_To_Gold_DataMart")
        .config("spark.driver.host", SPARK_LOCAL_IP)
        .config("spark.driver.bindAddress", SPARK_LOCAL_IP)
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,"
                "org.projectnessie.spark.extensions.NessieSparkSessionExtensions")
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog")
        .config("spark.sql.catalog.lakehouse.uri", NESSIE_API_URL)
        .config("spark.sql.catalog.lakehouse.ref", "main")
        .config("spark.sql.catalog.lakehouse.warehouse", "s3a://university-lakehouse/iceberg-warehouse")
        .config("spark.sql.catalog.lakehouse.cache-enabled", "false")
        .config("spark.sql.catalogImplementation", "in-memory")
        .config("spark.sql.catalog.lakehouse.s3.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.fast.upload", "true")
        .config("spark.hadoop.fs.s3a.connection.maximum", "100")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark

def preflight_clean_orphaned_tables(spark):
    use_main(spark)
    for table_name in [GOLD_TEACHING_SUMMARY, GOLD_LEARNING_SUMMARY]:
        try:
            spark.table(table_name).limit(1).collect()
        except Exception as exc:
            if any(token in str(exc).lower() for token in ["notfoundexception", "no such file or directory", "failed to open input stream"]):
                print(f"⚠️ [Preflight] Bảng Gold '{table_name}' bị orphaned metadata. Đang dọn dẹp...")
                delete_nessie_orphaned_key(table_name, "main")

def safe_write_gold_table(df, table_name, branch_name):
    try:
        df.writeTo(table_name).createOrReplace()
    except Exception as exc:
        if not any(token in str(exc).lower() for token in ["notfoundexception", "no such file or directory", "failed to open input stream"]):
            raise
        print(f"⚠️ Bảng '{table_name}' bị orphaned trên branch '{branch_name}'. Đang dọn dẹp và ghi lại...")
        delete_nessie_orphaned_key(table_name, branch_name)
        df.writeTo(table_name).createOrReplace()

def run_api_gold_aggregation(spark):
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.gold")
    use_main(spark)

    # Kiểm tra sự tồn tại của các bảng Silver nguồn từ API
    has_teaching = spark.catalog.tableExists("lakehouse.silver.api_teaching_master")
    has_learning = spark.catalog.tableExists("lakehouse.silver.api_learning_master")

    if not has_teaching and not has_learning:
        print("ℹ️ Không tìm thấy bảng Silver API nào (teaching hoặc learning). Bỏ qua bước Gold API.")
        return True

    branch_name = make_branch_name("api_silver_gold")

    try:
        preflight_clean_orphaned_tables(spark)
        create_branch(spark, branch_name, from_ref="main")
        use_branch(spark, branch_name)

        # 1. Tổng hợp Data Mart Tiến độ giảng dạy
        if has_teaching:
            print("⚙️ Đang tổng hợp Data Mart: Tiến độ giảng dạy theo đơn vị...")
            df_teaching = spark.read.table("lakehouse.silver.api_teaching_master")
            df_teaching_summary = df_teaching.groupBy("don_vi_dao_tao", "ky_danh_gia").agg(
                sum("lop_hp").alias("tong_lop_hp"),
                round(avg("dung_tien_do_pct"), 2).alias("tb_dung_tien_do_pct"),
                round(avg("hien_dien_pct"), 2).alias("tb_hien_dien_pct"),
                round(avg("nhap_diem_pct"), 2).alias("tb_nhap_diem_pct"),
                sum("doi_lich").alias("tong_doi_lich"),
                current_timestamp().alias("thoi_gian_dong_goi_gold")
            )
            safe_write_gold_table(df_teaching_summary, GOLD_TEACHING_SUMMARY, branch_name)
            print(f"✅ Đã ghi xong bảng {GOLD_TEACHING_SUMMARY}")

        # 2. Tổng hợp Data Mart Kết quả học tập
        if has_learning:
            print("⚙️ Đang tổng hợp Data Mart: Kết quả học tập theo chương trình...")
            df_learning = spark.read.table("lakehouse.silver.api_learning_master")
            df_learning_summary = df_learning.groupBy("chuong_trinh", "ky_danh_gia").agg(
                sum("sv_theo_hoc").alias("tong_sv_theo_hoc"),
                round(avg("qua_hp_pct"), 2).alias("tb_qua_hp_pct"),
                round(avg("gpa_trung_binh"), 2).alias("tb_gpa_trung_binh"),
                sum("can_bao_hoc_vu").alias("tong_can_bao_hoc_vu"),
                sum("nguy_co_nghi_hoc").alias("tong_nguy_co_nghi_hoc"),
                current_timestamp().alias("thoi_gian_dong_goi_gold")
            )
            safe_write_gold_table(df_learning_summary, GOLD_LEARNING_SUMMARY, branch_name)
            print(f"✅ Đã ghi xong bảng {GOLD_LEARNING_SUMMARY}")

        # Merge nhánh tạm vào main
        merge_branch_to_main(spark, branch_name)
        use_main(spark)
        print("\n🌟 HOÀN THÀNH TẦNG GOLD CHO DỮ LIỆU API!")

        # Đẩy Lineage lên OpenMetadata (nếu có)
        try:
            om_client = get_client()
            push_lineage_safe(
                om_client,
                "lakehouse-trino.lakehouse.silver.api_teaching_master",
                f"lakehouse-trino.{GOLD_TEACHING_SUMMARY}",
                sql_query="INSERT OVERWRITE ... SELECT ... FROM silver.api_teaching_master",
                description="Tổng hợp Data Mart tiến độ giảng dạy từ API",
            )
        except Exception as e:
            print(f"⚠️ Lineage update warning (bỏ qua): {e}")

        return True

    except Exception as e:
        use_main(spark)
        print(f"❌ Thất bại ở tiến trình Gold API: {str(e)}")
        raise e

def main():
    sys.stdout.reconfigure(encoding="utf-8")
    spark = get_spark_session()
    try:
        run_api_gold_aggregation(spark)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()