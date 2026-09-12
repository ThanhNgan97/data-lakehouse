# -*- coding: utf-8 -*-
"""
spark_silver_to_gold.py (Có Nessie Catalog Versioning)
------------------------------------------------------------
TẦNG GOLD (APACHE SPARK + ICEBERG) - ĐỀ TÀI DATA LAKEHOUSE CUSC
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
    col, when, count, round, current_timestamp, lag, create_map, lit,
    first, min as spark_min, max as spark_max, struct, regexp_extract,
)
from pyspark.sql.window import Window
from itertools import chain

from nessie_catalog_utils import (
    make_branch_name,
    create_branch,
    use_branch,
    use_main,
    merge_branch_to_main,
    check_quality_gold,
    DataQualityError,
    delete_nessie_orphaned_key,
)
from openmetadata_lineage_utils import get_client, push_lineage_safe

GOLD_SUMMARY_TABLE    = "lakehouse.gold.kpi_tong_hop_don_vi"
GOLD_DETAIL_TABLE     = "lakehouse.gold.kpi_chi_tiet_dashboard"
GOLD_COMPARISON_TABLE = "lakehouse.gold.kpi_so_sanh_ky"
GOLD_DICT_TABLE       = "lakehouse.gold.dm_chi_tieu"

GOLD_COMPARISON_PARTITION_COLUMNS = [
    "file_nguon",
    "ma_chi_tieu",
    "nhom_don_vi",
]

GOLD_DICT_JOIN_KEY = "ma_chi_tieu,nhom_don_vi"

GOLD_SUMMARY_COLUMNS = [
    # Giữ nguồn dữ liệu trong mart tổng hợp để các chart Superset dùng
    # kpi_tong_hop_don_vi cũng nhận được native filter `file_nguon`.
    "file_nguon", "quy_danh_gia", "nhom_don_vi",
    "tong_chi_tieu_danh_gia", "so_chi_tieu_dat", "so_chi_tieu_khong_dat",
    "ty_le_hoan_thanh_phan_tram", "thoi_gian_dong_goi_gold",
]
GOLD_DETAIL_COLUMNS = [
    "ma_chi_tieu", "nhom_don_vi", "ten_phong_ban", "quy_danh_gia",
    "noi_dung_muc_tieu", "dinh_ky_thu_thap",
    "muc_dang_ky", "muc_dat", "muc_dat_numeric", "ket_qua_he_thong",
    "nguyen_nhan", "hanh_dong_khac_phuc",
    "file_nguon", "minh_chung_type", "minh_chung_path", "thoi_gian_dong_goi_gold",
]
GOLD_COMPARISON_COLUMNS = [
    # Tương tự mart tổng hợp, tránh việc một chart dùng mart so sánh
    # bị bỏ ra ngoài khi dashboard lọc theo file.
    "file_nguon", "ma_chi_tieu", "nhom_don_vi", "ten_phong_ban",
    "quy_danh_gia", "muc_dat_numeric",
    "quy_danh_gia_ky_truoc", "muc_dat_numeric_ky_truoc",
    "tang_truong_phan_tram", "thoi_gian_dong_goi_gold",
]
GOLD_DICT_COLUMNS = [
    "ma_chi_tieu", "nhom_don_vi", "ten_phong_ban",
    "noi_dung_muc_tieu",
    "ky_dau_tien_xuat_hien", "ky_gan_nhat_cap_nhat",
    "nguon_bang_chi_tiet", "nguon_bang_tong_hop", "nguon_bang_so_sanh_ky",
    "cot_khoa_join", "thoi_gian_dong_goi_gold",
]

PHONG_BAN_MAP = {
    "ĐT":   "Phòng Đào tạo",
    "PM":   "Trung tâm Phần mềm (mảng dự án/phát triển phần mềm)",
    "QTCL": "Bộ phận Quản trị Chất lượng",
    "VP":   "Văn phòng",
    "RD":   "Phòng Nghiên cứu & Phát triển (R&D)",
    "HT":   "Phòng Hạ tầng - An ninh mạng (QTANM)",
}


def add_quy_danh_gia_sort_key(df, col_name="quy_danh_gia"):
    quy = regexp_extract(col(col_name), r"Q(\d)/(\d{4})", 1).cast("int")
    nam = regexp_extract(col(col_name), r"Q(\d)/(\d{4})", 2).cast("int")
    return df.withColumn("quy_danh_gia_sort_key", nam * 10 + quy)


def get_spark_session():
    print("Khởi tạo Spark Engine tính toán số liệu tầng Gold...")
    spark = (
        SparkSession.builder
        .appName("Silver_To_Gold_DataMart")
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


def with_ten_phong_ban(df):
    mapping_expr = create_map([lit(x) for x in chain(*PHONG_BAN_MAP.items())])
    return df.withColumn("ten_phong_ban", mapping_expr[col("nhom_don_vi")])


def preflight_clean_orphaned_gold_tables(spark):
    use_main(spark)
    gold_tables = [
        GOLD_SUMMARY_TABLE, GOLD_DETAIL_TABLE, GOLD_COMPARISON_TABLE, GOLD_DICT_TABLE,
    ]
    for table_name in gold_tables:
        try:
            spark.table(table_name).limit(1).collect()
        except Exception as exc:
            error_text = str(exc).lower()
            is_orphaned = any(
                token in error_text
                for token in [
                    "notfoundexception",
                    "no such file or directory",
                    "failed to open input stream",
                ]
            )
            if is_orphaned:
                print(f"⚠️ [Preflight] Bảng '{table_name}' bị orphaned metadata. Đang dọn dẹp key...")
                delete_nessie_orphaned_key(table_name, "main")


def safe_write_gold_table(df, table_name, branch_name):
    try:
        df.writeTo(table_name).createOrReplace()
    except Exception as exc:
        error_text = str(exc).lower()
        is_orphaned = any(
            token in error_text
            for token in [
                "notfoundexception",
                "no such file or directory",
                "failed to open input stream",
            ]
        )
        if not is_orphaned:
            raise

        print(f"⚠️ Bảng '{table_name}' bị orphaned metadata trên branch '{branch_name}'. Đang tự động dọn dẹp và ghi lại...")
        delete_nessie_orphaned_key(table_name, branch_name)
        df.writeTo(table_name).createOrReplace()
        print(f"✅ Đã phục hồi và ghi lại thành công bảng '{table_name}'.")


def run_silver_to_gold(spark):
    """Thực thi toàn bộ luồng Silver -> Gold Data Marts trên SparkSession."""
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.gold")
    use_main(spark)

    # 1. Kiểm tra an toàn xem bảng Silver có tồn tại và có dữ liệu không
    try:
        if not spark.catalog.tableExists("lakehouse.silver.kpi_cusc_master"):
            print("ℹ️ Bảng Silver (lakehouse.silver.kpi_cusc_master) chưa tồn tại. Bỏ qua bước Gold.")
            return True
        
        silver_count = spark.table("lakehouse.silver.kpi_cusc_master").count()
        if silver_count == 0:
            print("ℹ️ Bảng Silver (lakehouse.silver.kpi_cusc_master) đang rỗng (0 dòng). Bỏ qua bước Gold.")
            return True
    except Exception as e:
        print(f"ℹ️ Không thể truy vấn bảng Silver: {e}. Bỏ qua bước Gold.")
        return True

    branch_name = make_branch_name("silver_gold")

    try:
        preflight_clean_orphaned_gold_tables(spark)

        create_branch(spark, branch_name, from_ref="main")
        use_branch(spark, branch_name)

        print(f"📊 Đang đọc dữ liệu sạch ({silver_count} dòng) từ lakehouse.silver.kpi_cusc_master...")
        df_silver = spark.read.table("lakehouse.silver.kpi_cusc_master")
        df_silver = add_quy_danh_gia_sort_key(df_silver)

        # DATA MART 1: TỔNG HỢP KPI THEO PHÒNG BAN
        print("⚙️ Nghiệp vụ 1: Tính toán tỷ lệ hoàn thành KPI nghiệp vụ...")
        df_filtered = df_silver.filter(col("ket_qua_he_thong") != "CHƯA ĐẾN KỲ ĐÁNH GIÁ")

        # file_nguon phải là một phần của group key. Nếu không, số liệu của
        # nhiều file sẽ bị cộng chung và Superset không thể lọc đúng theo file.
        df_summary = df_filtered.groupBy(
            "file_nguon", "quy_danh_gia", "nhom_don_vi"
        ).agg(
            count("*").alias("tong_chi_tieu_danh_gia"),
            count(when(col("ket_qua_he_thong") == "ĐẠT", True)).alias("so_chi_tieu_dat"),
            count(when(col("ket_qua_he_thong") == "KHÔNG ĐẠT", True)).alias("so_chi_tieu_khong_dat")
        )

        df_summary = df_summary.withColumn(
            "ty_le_hoan_thanh_phan_tram",
            round((col("so_chi_tieu_dat") / col("tong_chi_tieu_danh_gia")) * 100, 2)
        ).withColumn("thoi_gian_dong_goi_gold", current_timestamp())

        df_summary = df_summary.select(*GOLD_SUMMARY_COLUMNS)

        # DATA MART 2: CHI TIẾT ĐẦY ĐỦ KPI + tên phòng ban
        print("⚙️ Nghiệp vụ 2: Đồng bộ danh sách Rich Schema phục vụ tra cứu...")
        df_detail = df_silver.select(
            "ma_chi_tieu", "nhom_don_vi", "quy_danh_gia",
            "noi_dung_muc_tieu", "dinh_ky_thu_thap",
            "muc_dang_ky", "muc_dat", "muc_dat_numeric", "ket_qua_he_thong",
            "nguyen_nhan", "hanh_dong_khac_phuc", "file_nguon",
            "minh_chung_type", "minh_chung_path"
        ).withColumn("thoi_gian_dong_goi_gold", current_timestamp())

        df_detail = with_ten_phong_ban(df_detail)
        df_detail = df_detail.select(*GOLD_DETAIL_COLUMNS)

        # DATA MART 3: SO SÁNH GIỮA CÁC KỲ
        print("⚙️ Nghiệp vụ 3: Tính tăng/giảm % của từng mã chỉ tiêu...")
        window_spec = (
            Window.partitionBy(
                *GOLD_COMPARISON_PARTITION_COLUMNS
            )
            .orderBy("quy_danh_gia_sort_key")
        )

        df_comparison = (
            df_silver
            # So sánh kỳ chỉ có ý nghĩa trong cùng một file nguồn.
            .withColumn(
                "quy_danh_gia_ky_truoc",
                lag("quy_danh_gia").over(window_spec),
            )
            .withColumn(
                "muc_dat_numeric_ky_truoc",
                lag("muc_dat_numeric").over(window_spec),
            )
            .withColumn(
                "tang_truong_phan_tram",
                when(
                    (col("muc_dat_numeric_ky_truoc").isNotNull()) & (col("muc_dat_numeric_ky_truoc") != 0),
                    round(
                        (col("muc_dat_numeric") - col("muc_dat_numeric_ky_truoc"))
                        / col("muc_dat_numeric_ky_truoc") * 100, 2
                    )
                ).otherwise(None)
            )
            .withColumn("thoi_gian_dong_goi_gold", current_timestamp())
        )
        df_comparison = with_ten_phong_ban(df_comparison)
        df_comparison = df_comparison.select(*GOLD_COMPARISON_COLUMNS)

        # DATA MART 4: DATA DICTIONARY CHO MÃ CHỈ TIÊU
        print("⚙️ Nghiệp vụ 4: Xây bảng chú thích (data dictionary)...")
        df_keyed = df_silver.withColumn(
            "ky_struct", struct(col("quy_danh_gia_sort_key"), col("quy_danh_gia"))
        )

        df_dict = (
            df_keyed.groupBy("ma_chi_tieu", "nhom_don_vi")
            .agg(
                first("noi_dung_muc_tieu", ignorenulls=True).alias("noi_dung_muc_tieu"),
                spark_min("ky_struct").alias("_ky_dau_tien_struct"),
                spark_max("ky_struct").alias("_ky_gan_nhat_struct"),
            )
            .withColumn("ky_dau_tien_xuat_hien", col("_ky_dau_tien_struct.quy_danh_gia"))
            .withColumn("ky_gan_nhat_cap_nhat", col("_ky_gan_nhat_struct.quy_danh_gia"))
            .drop("_ky_dau_tien_struct", "_ky_gan_nhat_struct")
        )

        df_dict = with_ten_phong_ban(df_dict)
        df_dict = (
            df_dict
            .withColumn("nguon_bang_chi_tiet", lit(GOLD_DETAIL_TABLE))
            .withColumn("nguon_bang_tong_hop", lit(GOLD_SUMMARY_TABLE))
            .withColumn("nguon_bang_so_sanh_ky", lit(GOLD_COMPARISON_TABLE))
            .withColumn("cot_khoa_join", lit(GOLD_DICT_JOIN_KEY))
            .withColumn("thoi_gian_dong_goi_gold", current_timestamp())
            .select(*GOLD_DICT_COLUMNS)
        )

        print(f"🧊 Đang ghi Data Mart Tổng hợp lên branch '{branch_name}'...")
        safe_write_gold_table(df_summary, GOLD_SUMMARY_TABLE, branch_name)

        print(f"🧊 Đang ghi Data Mart Chi tiết lên branch '{branch_name}'...")
        safe_write_gold_table(df_detail, GOLD_DETAIL_TABLE, branch_name)

        print(f"🧊 Đang ghi Data Mart So sánh kỳ lên branch '{branch_name}'...")
        safe_write_gold_table(df_comparison, GOLD_COMPARISON_TABLE, branch_name)

        print(f"🧊 Đang ghi Data Dictionary lên branch '{branch_name}'...")
        safe_write_gold_table(df_dict, GOLD_DICT_TABLE, branch_name)

        check_quality_gold(spark, GOLD_SUMMARY_TABLE, GOLD_DETAIL_TABLE)
        merge_branch_to_main(spark, branch_name)
        use_main(spark)

        print("\n🌟 HOÀN THÀNH TẦNG GOLD!")

        try:
            om_client = get_client()
            silver_fqn = "lakehouse-trino.lakehouse.silver.kpi_cusc_master"
            gold_summary_fqn = "lakehouse-trino.lakehouse.gold.kpi_tong_hop_don_vi"
            push_lineage_safe(
                om_client, silver_fqn, gold_summary_fqn,
                sql_query="INSERT OVERWRITE gold.kpi_tong_hop_don_vi SELECT ... FROM silver.kpi_cusc_master",
                description="Tổng hợp tỷ lệ hoàn thành KPI theo đơn vị",
            )
        except Exception as e:
            print(f"⚠️ Lineage update warning (bỏ qua): {e}")

        return True

    except DataQualityError as dqe:
        use_main(spark)
        print(f"⚠️ DỮ LIỆU GOLD KHÔNG ĐẠT CHẤT LƯỢNG: {dqe}")
        raise dqe
    except Exception as e:
        use_main(spark)
        print(f"❌ Thất bại ở tiến trình Gold: {str(e)}")
        raise e


def main():
    sys.stdout.reconfigure(encoding="utf-8")
    spark = get_spark_session()
    try:
        run_silver_to_gold(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
