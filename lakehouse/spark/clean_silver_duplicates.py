# -*- coding: utf-8 -*-
"""
clean_silver_duplicates.py
------------------------------------------------------------
Script PySpark dùng để làm sạch (deduplicate) các bản ghi bị trùng lặp
khóa nghiệp vụ (nhom_don_vi, ma_chi_tieu, quy_danh_gia) đang tồn tại
trực tiếp trên nhánh 'main' của Nessie Catalog.
"""

import os
import sys
from pyspark.sql import SparkSession
from env_config import (
    MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY,
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

SILVER_TABLE = "lakehouse.silver.kpi_cusc_master"


def get_spark_session():
    return SparkSession.builder \
        .appName("Clean_Silver_Duplicates") \
        .config("spark.driver.host", SPARK_LOCAL_IP) \
        .config("spark.driver.bindAddress", SPARK_LOCAL_IP) \
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,"
                "org.projectnessie.spark.extensions.NessieSparkSessionExtensions") \
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.lakehouse.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog") \
        .config("spark.sql.catalog.lakehouse.uri", NESSIE_API_URL) \
        .config("spark.sql.catalog.lakehouse.ref", "main") \
        .config("spark.sql.catalog.lakehouse.warehouse", "s3a://university-lakehouse/iceberg-warehouse") \
        .config("spark.sql.catalog.lakehouse.cache-enabled", "false") \
        .config("spark.sql.catalogImplementation", "in-memory") \
        .config("spark.sql.catalog.lakehouse.s3.endpoint", MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .config("spark.hadoop.fs.s3a.fast.upload", "true") \
        .config("spark.hadoop.fs.s3a.connection.maximum", "100") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.sql.parquet.enableVectorizedReader", "false") \
        .getOrCreate()


def clean_duplicates():
    sys.stdout.reconfigure(encoding='utf-8')
    spark = get_spark_session()
    try:
        spark.sql("USE REFERENCE main IN lakehouse")
        print(f"🔍 Đang kiểm tra bảng {SILVER_TABLE} trên Nessie 'main'...")
        
        df = spark.table(SILVER_TABLE)
        total_before = df.count()
        
        dup_count_old_key = (
            df.groupBy("ma_chi_tieu", "quy_danh_gia")
            .count()
            .filter("count > 1")
            .count()
        )
        
        dup_count_new_key = (
            df.groupBy("nhom_don_vi", "ma_chi_tieu", "quy_danh_gia")
            .count()
            .filter("count > 1")
            .count()
        )

        print(f"📊 Tổng số dòng trước khi làm sạch: {total_before}")
        print(f"⚠️ Phát hiện theo khóa cũ (ma_chi_tieu, quy_danh_gia): {dup_count_old_key} nhóm bị trùng.")
        print(f"⚠️ Phát hiện theo khóa mới (nhom_don_vi, ma_chi_tieu, quy_danh_gia): {dup_count_new_key} nhóm bị trùng.")

        if total_before == 0:
            print("ℹ️ Bảng Silver đang rỗng, không cần làm sạch.")
            return

        print("🧹 Đang dọn dẹp và giữ lại bản ghi mới nhất cho mỗi (nhom_don_vi, ma_chi_tieu, quy_danh_gia)...")
        spark.sql(f"""
            CREATE OR REPLACE TEMP VIEW clean_silver_view AS
            SELECT file_nguon, ma_chi_tieu, nhom_don_vi, quy_danh_gia, noi_dung_muc_tieu,
                   dinh_ky_thu_thap, muc_dang_ky, muc_dang_ky_numeric, muc_dat, muc_dat_numeric,
                   ket_qua_he_thong, nguyen_nhan, hanh_dong_khac_phuc, minh_chung_type, minh_chung_path,
                   checksum_sha256, thoi_gian_ingest_silver
            FROM (
                SELECT *, ROW_NUMBER() OVER (
                    PARTITION BY nhom_don_vi, ma_chi_tieu, quy_danh_gia 
                    ORDER BY thoi_gian_ingest_silver DESC
                ) as _rn
                FROM {SILVER_TABLE}
            ) WHERE _rn = 1
        """)

        df_clean = spark.table("clean_silver_view")
        total_after = df_clean.count()
        
        df_clean.write.format("iceberg").mode("overwrite").saveAsTable(SILVER_TABLE)
        print(f"✅ ĐÃ LÀM SẠCH BẢNG SILVER THÀNH CÔNG!")
        print(f"📈 Số dòng ban đầu: {total_before} -> Số dòng sau khi dọn dẹp: {total_after} (đã xóa {total_before - total_after} dòng trùng).")

    except Exception as e:
        print(f"❌ Lỗi trong quá trình dọn dẹp: {e}")
        raise e
    finally:
        spark.stop()


if __name__ == "__main__":
    clean_duplicates()
