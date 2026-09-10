# -*- coding: utf-8 -*-
"""
spark_predictive_analysis.py
------------------------------------------------------------
TẦNG GOLD (APACHE SPARK + ICEBERG) - PREDICTIVE ANALYSIS
Dự báo kết quả các tháng/kỳ tới cho từng mã chỉ tiêu.
"""

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
os.environ["SPARK_LOCAL_IP"] = SPARK_LOCAL_IP
os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--driver-java-options \"-Djava.net.preferIPv4Stack=true\" "
    "--packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3,"
    "org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.77.1,"
    "org.apache.hadoop:hadoop-aws:3.3.4 "
    "pyspark-shell"
)

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, count, round, current_timestamp, lag, lit,
    first, max as spark_max, avg, concat, cast,
    row_number, sum as spark_sum
)
from pyspark.sql.window import Window

from nessie_catalog_utils import (
    make_branch_name,
    create_branch,
    use_branch,
    use_main,
    merge_branch_to_main,
    delete_nessie_orphaned_key,
)

GOLD_COMPARISON_TABLE = "lakehouse.gold.kpi_so_sanh_ky"
GOLD_PREDICT_TABLE    = "lakehouse.gold.kpi_du_doan_tuong_lai"


def get_spark_session():
    print("Khởi tạo Spark Engine tính toán dự đoán tương lai...")
    spark = (
        SparkSession.builder
        .appName("Gold_Predictive_Analysis")
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


def preflight_clean_predict_table(spark):
    use_main(spark)
    try:
        spark.table(GOLD_PREDICT_TABLE).limit(1).collect()
    except Exception as exc:
        error_text = str(exc).lower()
        if any(token in error_text for token in ["notfoundexception", "no such file or directory", "failed to open input stream"]):
            print(f" [Preflight] Bảng '{GOLD_PREDICT_TABLE}' bị orphaned metadata. Đang dọn dẹp key...")
            delete_nessie_orphaned_key(GOLD_PREDICT_TABLE, "main")


def safe_write_predict_table(df, table_name, branch_name):
    try:
        df.writeTo(table_name).createOrReplace()
    except Exception as exc:
        error_text = str(exc).lower()
        if any(token in error_text for token in ["notfoundexception", "no such file or directory", "failed to open input stream"]):
            print(f" Bảng '{table_name}' bị orphaned metadata trên branch '{branch_name}'. Đang tự động dọn dẹp và ghi lại...")
            delete_nessie_orphaned_key(table_name, branch_name)
            df.writeTo(table_name).createOrReplace()
            print(f" Đã phục hồi và ghi lại thành công bảng '{table_name}'.")
        else:
            raise


def run_predictive_analysis(spark):
    """Thực thi toàn bộ luồng dự báo KPI tầng Gold trên SparkSession."""
    use_main(spark)

    try:
        if not spark.catalog.tableExists(GOLD_COMPARISON_TABLE):
            print(f" Bảng {GOLD_COMPARISON_TABLE} chưa tồn tại. Bỏ qua bước dự đoán.")
            return True
        
        if spark.table(GOLD_COMPARISON_TABLE).count() == 0:
            print(f" Bảng {GOLD_COMPARISON_TABLE} đang rỗng (0 dòng). Bỏ qua bước dự đoán.")
            return True
    except Exception as e:
        print(f" Không thể truy vấn bảng {GOLD_COMPARISON_TABLE}: {e}. Bỏ qua bước dự đoán.")
        return True

    branch_name = make_branch_name("predictive_analysis")

    try:
        preflight_clean_predict_table(spark)

        create_branch(spark, branch_name, from_ref="main")
        use_branch(spark, branch_name)

        df_history = spark.read.table(GOLD_COMPARISON_TABLE)

        # Tính toán mức độ tăng trưởng theo WMA
        window_time = Window.partitionBy("ma_chi_tieu").orderBy("quy_danh_gia")
        df_weighted = df_history.filter(col("tang_truong_phan_tram").isNotNull()) \
            .withColumn("weight", row_number().over(window_time))
        
        df_avg_growth = df_weighted.groupBy("ma_chi_tieu").agg(
            (spark_sum(col("tang_truong_phan_tram") * col("weight")) / spark_sum(col("weight"))).alias("avg_growth_pct")
        )
        
        # Lấy giá trị kỳ gần nhất
        df_latest = df_history.withColumn("is_latest", col("quy_danh_gia") == spark_max("quy_danh_gia").over(Window.partitionBy("ma_chi_tieu")))
        df_latest = df_latest.filter(col("is_latest") == True).select(
            "ma_chi_tieu", "nhom_don_vi", "ten_phong_ban", "quy_danh_gia", "muc_dat_numeric"
        )
        
        # Join để tính dự báo kỳ tiếp theo
        df_predict = df_latest.join(df_avg_growth, on="ma_chi_tieu", how="left")
        df_predict.createOrReplaceTempView("latest_data")
        df_next = spark.sql("""
            SELECT 
                ma_chi_tieu, nhom_don_vi, ten_phong_ban,
                quy_danh_gia as quy_hien_tai,
                muc_dat_numeric as muc_dat_hien_tai,
                COALESCE(avg_growth_pct, 0) as avg_growth_pct,
                CASE 
                    WHEN CAST(SUBSTRING(quy_danh_gia, 2, 1) AS INT) = 4 
                        THEN CONCAT('Q1/', CAST(SUBSTRING(quy_danh_gia, 4, 4) AS INT) + 1)
                    ELSE CONCAT('Q', CAST(SUBSTRING(quy_danh_gia, 2, 1) AS INT) + 1, '/', SUBSTRING(quy_danh_gia, 4, 4))
                END as quy_du_doan
            FROM latest_data
        """)
        
        df_next = df_next.withColumn(
            "muc_dat_du_doan",
            round(col("muc_dat_hien_tai") * (1 + col("avg_growth_pct") / 100), 2)
        ).withColumn("thoi_gian_du_doan", current_timestamp())
        
        df_next = df_next.select(
            "ma_chi_tieu", "nhom_don_vi", "ten_phong_ban",
            "quy_hien_tai", "muc_dat_hien_tai",
            "quy_du_doan", "muc_dat_du_doan", "avg_growth_pct",
            "thoi_gian_du_doan"
        )
        
        print(f" Đang ghi Data Mart Dự đoán lên branch '{branch_name}'...")
        safe_write_predict_table(df_next, GOLD_PREDICT_TABLE, branch_name)

        merge_branch_to_main(spark, branch_name)
        use_main(spark)
        
        print("\n HOÀN THÀNH TÍNH TOÁN DỰ ĐOÁN!")
        return True
        
    except Exception as e:
        use_main(spark)
        print(f" Thất bại ở bước dự đoán: {str(e)}")
        raise e


def main():
    sys.stdout.reconfigure(encoding="utf-8")
    spark = get_spark_session()
    try:
        run_predictive_analysis(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
