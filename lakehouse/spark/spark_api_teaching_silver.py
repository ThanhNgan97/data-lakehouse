# -*- coding: utf-8 -*-
"""
spark/spark_api_teaching_silver.py
------------------------------------------------------------
PySpark Job: Xử lý dữ liệu Tiến độ giảng dạy từ API (Bronze JSON)
lên tầng Silver (Bảng Apache Iceberg) có tích hợp Nessie Versioning,
Lineage, Schema Evolution động và cơ chế Khử trùng lặp (Deduplication).
------------------------------------------------------------
"""

import os
import sys
from datetime import datetime
import boto3
import argparse

from env_config import (
    MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_BUCKET_NAME,
    NESSIE_API_URL, HADOOP_HOME, SPARK_LOCAL_IP,
)

os.environ["HADOOP_HOME"] = HADOOP_HOME
os.environ["PATH"] = os.path.join(HADOOP_HOME, "bin") + ";" + os.environ.get("PATH", "")
os.environ["AWS_ACCESS_KEY_ID"] = MINIO_ACCESS_KEY
os.environ["AWS_SECRET_ACCESS_KEY"] = MINIO_SECRET_KEY
os.environ["SPARK_LOCAL_IP"] = SPARK_LOCAL_IP
os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--driver-java-options \"-Djava.net.preferIPv4Stack=true\" "
    "--packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3,"
    "org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.77.1,"
    "org.apache.hadoop:hadoop-aws:3.3.4 "
    "pyspark-shell"
)

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp

from nessie_catalog_utils import (
    make_branch_name,
    create_branch,
    use_branch,
    use_main,
    merge_branch_to_main,
    check_quality_api_silver,
    DataQualityError,
    delete_nessie_orphaned_key,
)
from openmetadata_lineage_utils import get_client, ensure_bronze_table, push_lineage_safe

SILVER_TABLE = "lakehouse.silver.api_teaching_master"
BRONZE_API_TEACHING_PREFIX = "bronze/api_teaching/"


def get_spark_session():
    return SparkSession.builder \
        .appName("API_Teaching_Progress_To_Silver_Dynamic") \
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
        .config("spark.sql.iceberg.schema.auto-conversion", "true") \
        .getOrCreate()


def get_s3_client():
    return boto3.client(
        "s3", endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY, aws_secret_access_key=MINIO_SECRET_KEY,
        verify=False
    )


def init_table_if_needed(spark, branch_name="main"):
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.silver")
    create_sql = f"""
        CREATE TABLE IF NOT EXISTS {SILVER_TABLE} (
            don_vi_dao_tao STRING,
            ky_danh_gia STRING,
            lop_hp INT,
            dung_tien_do_pct DOUBLE,
            hien_dien_pct DOUBLE,
            nhap_diem_pct DOUBLE,
            doi_lich INT,
            diem_phan_hoi_sv STRING,
            danh_gia STRING,
            checksum_sha256 STRING,
            thoi_gian_ingest_silver TIMESTAMP
        ) USING iceberg
        PARTITIONED BY (ky_danh_gia)
    """
    try:
        spark.sql(create_sql)
    except Exception as exc:
        if "notfoundexception" in str(exc).lower():
            delete_nessie_orphaned_key(SILVER_TABLE, branch_name)
            spark.sql(f"DROP TABLE IF EXISTS {SILVER_TABLE}")
            spark.sql(create_sql)
        else:
            raise


def evolve_table_schema_if_needed(spark, source_df):
    try:
        table_cols = [
            row.col_name.lower() 
            for row in spark.sql(f"DESCRIBE {SILVER_TABLE}").collect() 
            if row.col_name and not row.col_name.startswith("#")
        ]
        ignored_cols = ['partition', 'index', 'name', '_metadata']
        schema_changed = False
        
        for field in source_df.schema.fields:
            col_name = field.name.lower()
            col_type = field.dataType.simpleString()
            
            if col_name not in table_cols and col_name not in ignored_cols:
                print(f"🔄 Phát hiện trường dữ liệu mới từ nguồn: '{col_name}' ({col_type}). Đang tiến hành cập nhật Schema bảng Silver...")
                spark.sql(f"ALTER TABLE {SILVER_TABLE} ADD COLUMN {col_name} {col_type}")
                print(f"✅ Đã thêm cột '{col_name}' vào bảng {SILVER_TABLE} thành công!")
                schema_changed = True
        
        # 🛠️ Refresh metadata bảng ngay lập tức để tránh lỗi xung đột commit khi thực hiện MERGE
        if schema_changed:
            spark.sql(f"REFRESH TABLE {SILVER_TABLE}")
            spark.catalog.clearCache()

    except Exception as e:
        print(f"⚠️ Cảnh báo trong quá trình tự động cập nhật schema: {str(e)}")


def run_pipeline(spark):
    init_table_if_needed(spark, "main")
    s3_client = get_s3_client()
    
    resp = s3_client.list_objects_v2(Bucket=MINIO_BUCKET_NAME, Prefix=BRONZE_API_TEACHING_PREFIX)
    if "Contents" not in resp:
        print("ℹ️ Không tìm thấy dữ liệu tiến độ giảng dạy API nào ở tầng Bronze.")
        return

    branch_name = make_branch_name("api_teaching_silver")
    try:
        spark.catalog.clearCache()
        create_branch(spark, branch_name, from_ref="main")
        use_branch(spark, branch_name)
        init_table_if_needed(spark, branch_name)

        bronze_path = f"s3a://{MINIO_BUCKET_NAME}/{BRONZE_API_TEACHING_PREFIX}*.json"
        raw_df = spark.read.option("inferSchema", "true").json(bronze_path)
        dedup_df = raw_df.dropDuplicates(["don_vi_dao_tao", "ky_danh_gia"])

        evolve_table_schema_if_needed(spark, dedup_df)

        select_exprs = []
        for field in dedup_df.schema.fields:
            c = field.name
            c_low = c.lower()
            if c_low == "lop_hp":
                select_exprs.append(col("lop_hp").cast("integer"))
            elif c_low in ["dung_tien_do_pct", "hien_dien_pct", "nhap_diem_pct"]:
                select_exprs.append(col(c).cast("double"))
            elif c_low == "doi_lich":
                select_exprs.append(col("doi_lich").cast("integer"))
            else:
                select_exprs.append(col(c))
        
        cleaned_df = dedup_df.select(*select_exprs, current_timestamp().alias("thoi_gian_ingest_silver"))
        cleaned_df.createOrReplaceTempView("source_teaching_api")

        table_cols = [
            row.col_name.lower() 
            for row in spark.sql(f"DESCRIBE {SILVER_TABLE}").collect() 
            if row.col_name and not row.col_name.startswith("#")
        ]
        
        update_set_clauses = []
        for c in table_cols:
            if c not in ["don_vi_dao_tao", "ky_danh_gia", "thoi_gian_ingest_silver", "partition", "index"]:
                update_set_clauses.append(f"t.{c} = s.{c}")
        update_set_clauses.append("t.thoi_gian_ingest_silver = s.thoi_gian_ingest_silver")
        update_sql_str = ",\n                ".join(update_set_clauses)

        spark.sql(f"""
            MERGE INTO {SILVER_TABLE} t
            USING source_teaching_api s
            ON t.don_vi_dao_tao = s.don_vi_dao_tao AND t.ky_danh_gia = s.ky_danh_gia
            WHEN MATCHED THEN
              UPDATE SET
                {update_sql_str}
            WHEN NOT MATCHED THEN
              INSERT *
        """)

        check_quality_api_silver(spark, SILVER_TABLE)
        merge_branch_to_main(spark, branch_name)
        use_main(spark)
        print("✅ Đã đẩy dữ liệu tiến độ giảng dạy API lên Silver (main) thành công qua Nessie branch với Dynamic Schema Evolution!")

    except Exception as e:
        use_main(spark)
        raise e


def main():
    sys.stdout.reconfigure(encoding='utf-8')
    spark = get_spark_session()
    try:
        run_pipeline(spark)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()