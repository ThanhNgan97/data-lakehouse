# -*- coding: utf-8 -*-
import os
import sys

from env_config import (
    MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_ENDPOINT,
    NESSIE_API_URL, HADOOP_HOME, SPARK_LOCAL_IP,
)

os.environ["HADOOP_HOME"]           = HADOOP_HOME
os.environ["PATH"]                  = os.path.join(HADOOP_HOME, "bin") + ";" + os.environ.get("PATH", "")
os.environ["AWS_ACCESS_KEY_ID"]     = MINIO_ACCESS_KEY
os.environ["AWS_SECRET_ACCESS_KEY"] = MINIO_SECRET_KEY
os.environ["SPARK_LOCAL_IP"]        = SPARK_LOCAL_IP
os.environ["PYSPARK_SUBMIT_ARGS"]   = (
    "--driver-java-options \"-Djava.net.preferIPv4Stack=true\" "
    "--packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3,"
    "org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.77.1,"
    "org.apache.hadoop:hadoop-aws:3.3.4 "
    "pyspark-shell"
)

from pyspark.sql import SparkSession
from nessie_catalog_utils import use_main

def get_spark():
    return SparkSession.builder \
        .appName("Inspect_Iceberg") \
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
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.fast.upload", "true") \
        .config("spark.hadoop.fs.s3a.connection.maximum", "100") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()

def main():
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    
    spark = get_spark()
    try:
        use_main(spark)
        
        print("=== 1. QUARANTINE TABLE ===")
        if spark.catalog.tableExists("lakehouse.silver.kpi_cusc_quarantine"):
            df_q = spark.read.table("lakehouse.silver.kpi_cusc_quarantine")
            print(f"Quarantine count: {df_q.count()}")
            df_q.show(20, truncate=False)
        else:
            print("Quarantine table does not exist.")

        print("\n=== 2. SILVER MASTER TABLE ===")
        if spark.catalog.tableExists("lakehouse.silver.kpi_cusc_master"):
            df_s = spark.read.table("lakehouse.silver.kpi_cusc_master")
            print(f"Silver Master count: {df_s.count()}")
            df_s.show(20, truncate=False)
        else:
            print("Silver Master table does not exist.")

        print("\n=== 3. GOLD SUMMARY TABLE ===")
        if spark.catalog.tableExists("lakehouse.gold.kpi_tong_hop_don_vi"):
            df_gs = spark.read.table("lakehouse.gold.kpi_tong_hop_don_vi")
            print(f"Gold Summary count: {df_gs.count()}")
            df_gs.show(20, truncate=False)
        else:
            print("Gold Summary table does not exist.")

        print("\n=== 4. GOLD DETAIL TABLE ===")
        if spark.catalog.tableExists("lakehouse.gold.kpi_chi_tiet_dashboard"):
            df_gd = spark.read.table("lakehouse.gold.kpi_chi_tiet_dashboard")
            print(f"Gold Detail count: {df_gd.count()}")
            df_gd.show(20, truncate=False)
        else:
            print("Gold Detail table does not exist.")
            
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
