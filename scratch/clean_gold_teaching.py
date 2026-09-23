# -*- coding: utf-8 -*-
"""
clean_gold_teaching.py
PySpark Job: Lọc sạch trùng lặp và làm đẹp danh sách đơn vị đào tạo
trong bảng Gold (lakehouse.gold.kpi_api_teaching_summary).
- Chuẩn hóa tên đơn vị ngắn gọn: Bách khoa, KH Tự nhiên, Kinh tế, CNTT, Nông nghiệp, Sư phạm, Thủy sản, Ngoại ngữ, KHXH&NV, Viện CNSH&TP.
- Đảm bảo 100% không trùng lặp.
"""
import os
import sys

from lakehouse.spark.env_config import (
    MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_ENDPOINT,
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
from pyspark.sql.functions import col, when, trim, round, avg, sum, first, current_timestamp

def main():
    print("🚀 Khởi chạy PySpark dọn dẹp và chuẩn hóa bảng Gold Tiến độ giảng dạy...")
    spark = (
        SparkSession.builder
        .appName("Clean_Gold_Teaching_Summary")
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
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    table_name = "lakehouse.gold.kpi_api_teaching_summary"
    df = spark.read.table(table_name)
    
    # Standardize & clean names
    df_clean = df.withColumn(
        "don_vi_dao_tao",
        when(col("don_vi_dao_tao").like("%Bách%"), "Bách khoa")
        .when(col("don_vi_dao_tao").like("%Tự nhiên%"), "KH Tự nhiên")
        .when(col("don_vi_dao_tao").like("%Kinh tế%"), "Kinh tế")
        .when(col("don_vi_dao_tao").like("%Ngoại ngữ%"), "Ngoại ngữ")
        .when(col("don_vi_dao_tao").like("%Sư phạm%"), "Sư phạm")
        .when(col("don_vi_dao_tao").like("%CNTT%"), "CNTT")
        .when(col("don_vi_dao_tao").like("%Nông nghiệp%"), "Nông nghiệp")
        .when(col("don_vi_dao_tao").like("%Thủy sản%"), "Thủy sản")
        .when(col("don_vi_dao_tao").like("%KHXH%"), "KHXH&NV")
        .when(col("don_vi_dao_tao").like("%CNSH%"), "Viện CNSH&TP")
        .otherwise(trim(col("don_vi_dao_tao")))
    )

    df_dedup = df_clean.groupBy("don_vi_dao_tao", "ky_danh_gia").agg(
        sum("tong_lop_hp").alias("tong_lop_hp"),
        round(avg("tb_dung_tien_do_pct"), 1).alias("tb_dung_tien_do_pct"),
        round(avg("tb_hien_dien_pct"), 1).alias("tb_hien_dien_pct"),
        round(avg("tb_nhap_diem_pct"), 1).alias("tb_nhap_diem_pct"),
        sum("tong_doi_lich").alias("tong_doi_lich"),
        current_timestamp().alias("thoi_gian_dong_goi_gold"),
        first("diem_phan_hoi_sv").alias("diem_phan_hoi_sv"),
        first("danh_gia").alias("danh_gia")
    )

    df_dedup.writeTo(table_name).createOrReplace()
    print("✅ Đã làm sạch và dọn dẹp bảng Gold thành công với 100% tên đơn vị ngắn gọn, rõ ràng!")

    spark.stop()

if __name__ == "__main__":
    main()
