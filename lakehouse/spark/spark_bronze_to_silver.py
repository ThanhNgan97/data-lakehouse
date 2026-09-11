# -*- coding: utf-8 -*-
"""
spark_bronze_to_silver.py (Có Nessie Catalog Versioning)
------------------------------------------------------------
TẦNG SILVER - GHI ĐẦY ĐỦ CỘT NGHIỆP VỤ VÀO APACHE ICEBERG
"""

import os
import sys
from datetime import datetime
import boto3
import argparse
from db_utils import update_pipeline_error
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import row_number, desc, lit, col, trim, when

from nessie_catalog_utils import (
    make_branch_name,
    create_branch,
    use_branch,
    use_main,
    merge_branch_to_main,
    check_quality_silver,
    DataQualityError,
    delete_nessie_orphaned_key,
)
from openmetadata_lineage_utils import get_client, ensure_bronze_table, push_lineage_safe
from env_config import (
    MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_BUCKET_NAME,
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
BRONZE_PREFIX          = "bronze/"
BRONZE_ARCHIVE_PREFIX  = "bronze_archive/"
BRONZE_DISCARDED_PREFIX = "bronze_discarded_duplicates/"
DOCUMENT_SOURCE = "DOCUMENT_FILE"
MYSQL_SOURCE = "MYSQL_RDBMS"
MYSQL_DUMP_SOURCE = "MYSQL_DUMP"

DOCUMENT_BRONZE_PREFIX = f"{BRONZE_PREFIX}data_extracted_"
MYSQL_BRONZE_PREFIX = f"{BRONZE_PREFIX}data_mysql_extracted_"
MYSQL_DUMP_BRONZE_PREFIX = f"{BRONZE_PREFIX}data_mysql_dump_extracted_"


def get_spark_session():
    return SparkSession.builder \
        .appName("Bronze_To_Silver_Full_Schema") \
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


def get_s3_client():
    return boto3.client(
        "s3", endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY, aws_secret_access_key=MINIO_SECRET_KEY,
        verify=False
    )


def init_silver_table_if_needed(spark, branch_name="main"):
    """Đảm bảo namespace + bảng Silver tồn tại, và tự bổ sung cột mới nếu cần."""
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.silver")
    create_sql = f"""
        CREATE TABLE IF NOT EXISTS {SILVER_TABLE} (
            file_nguon STRING,
            nguon_du_lieu STRING,
            source_connector_id BIGINT,
            source_connector_name STRING,
            source_file_name STRING,
            source_upload_id BIGINT,
            source_table STRING,
            ma_chi_tieu STRING,
            nhom_don_vi STRING,
            quy_danh_gia STRING,
            noi_dung_muc_tieu STRING,
            dinh_ky_thu_thap STRING,
            muc_dang_ky STRING,
            muc_dang_ky_numeric DOUBLE,
            muc_dat STRING,
            muc_dat_numeric DOUBLE,
            ket_qua_he_thong STRING,
            hanh_dong_khac_phuc STRING,
            minh_chung_type STRING,
            minh_chung_path STRING,
            checksum_sha256 STRING,
            thoi_gian_ingest_silver TIMESTAMP
        ) USING iceberg
        PARTITIONED BY (quy_danh_gia)
    """

    try:
        spark.sql(create_sql)
        print(f"✅ Đã đảm bảo bảng {SILVER_TABLE} tồn tại.")
    except Exception as exc:
        error_text = str(exc).lower()
        if any(token in error_text for token in ["notfoundexception", "metadata", "input stream", "no such file or directory"]):
            print(f"⚠️ Bảng {SILVER_TABLE} có metadata Iceberg bị hỏng hoặc thiếu. Đang dọn dẹp key trong Nessie catalog...")
            delete_nessie_orphaned_key(SILVER_TABLE, branch_name)
            delete_nessie_orphaned_key(SILVER_TABLE, "main")
            try:
                spark.sql(f"DROP TABLE IF EXISTS {SILVER_TABLE}")
            except Exception:
                pass
            spark.sql(create_sql)
            print(f"✅ Đã tạo lại bảng {SILVER_TABLE}.")
        else:
            raise

    # Schema evolution
    try:
        existing_columns = {f.name for f in spark.table(SILVER_TABLE).schema.fields}
        new_columns = {
            "noi_dung_muc_tieu": "STRING",
            "nguyen_nhan": "STRING",
            "hanh_dong_khac_phuc": "STRING",
            "muc_dang_ky_numeric": "DOUBLE",
            "muc_dat_numeric": "DOUBLE",
            "minh_chung_type": "STRING",
            "minh_chung_path": "STRING",
            "nguon_du_lieu": "STRING",
            "source_connector_id": "BIGINT",
            "source_connector_name": "STRING",
            "source_file_name": "STRING",
            "source_upload_id": "BIGINT",
            "source_table": "STRING",
        }
        for col_name, col_type in new_columns.items():
            if col_name not in existing_columns:
                print(f"🔧 Đang bổ sung cột '{col_name}' vào bảng {SILVER_TABLE}...")
                spark.sql(f"ALTER TABLE {SILVER_TABLE} ADD COLUMN {col_name} {col_type}")
    except Exception:
        pass


def dedup_by_business_key(df_bronze):
    """
    Dedup theo khóa nghiệp vụ (ma_chi_tieu, quy_danh_gia, nhom_don_vi).

    Nếu có nhiều snapshot Bronze cho cùng một khóa, ưu tiên file Bronze có tên/path
    mới hơn. Tên file của cả pipeline tài liệu và MySQL đều chứa timestamp nên thứ tự
    này ổn định hơn việc chỉ dùng current_timestamp() cho toàn batch.
    """
    # Dùng literal timestamp tạo ở Python để MERGE Iceberg nhận source plan là deterministic.
    batch_ingest_ts = datetime.now()
    df_with_ts = df_bronze.withColumn(
        "thoi_gian_ingest_silver",
        lit(batch_ingest_ts).cast("timestamp"),
    )
    w = (
        Window.partitionBy("ma_chi_tieu", "quy_danh_gia", "nhom_don_vi")
        .orderBy(desc("_bronze_input_path"), desc("checksum_sha256"))
    )
    df_ranked = df_with_ts.withColumn("_rn", row_number().over(w))

    df_staging = df_ranked.filter("_rn = 1").drop("_rn")
    df_discarded = df_ranked.filter("_rn > 1").drop("_rn")

    return df_staging, df_discarded


def save_discarded_duplicates(df_discarded, s3_client):
    """Ghi lại các bản ghi bị loại do trùng khóa nghiệp vụ."""
    dup_count = df_discarded.count()
    if dup_count == 0:
        return

    timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    discard_path = f"s3a://university-lakehouse/{BRONZE_DISCARDED_PREFIX}discarded_{timestamp_str}.parquet"

    print(
        f"⚠️ CẢNH BÁO: phát hiện {dup_count} bản ghi trùng (ma_chi_tieu, quy_danh_gia, nhom_don_vi). "
        f"Chỉ giữ lại snapshot Bronze mới nhất theo đường dẫn/timestamp file."
    )
    df_discarded.write.mode("overwrite").parquet(discard_path)
    print(f"📝 Đã ghi {dup_count} bản ghi bị loại vào '{discard_path}'.")


def list_new_bronze_parquet_keys(s3_client):
    """List Bronze Parquet files supported by Silver."""
    paginator = s3_client.get_paginator("list_objects_v2")

    document_keys = []
    mysql_keys = []
    mysql_dump_keys = []

    for page in paginator.paginate(
        Bucket=MINIO_BUCKET_NAME,
        Prefix=BRONZE_PREFIX,
    ):
        for obj in page.get("Contents", []):
            key = obj["Key"]

            if not key.endswith(".parquet"):
                continue

            # Check the more specific dump prefix first.
            if key.startswith(MYSQL_DUMP_BRONZE_PREFIX):
                mysql_dump_keys.append(key)
            elif key.startswith(DOCUMENT_BRONZE_PREFIX):
                document_keys.append(key)
            elif key.startswith(MYSQL_BRONZE_PREFIX):
                mysql_keys.append(key)

    return (
        sorted(document_keys),
        sorted(mysql_keys),
        sorted(mysql_dump_keys),
    )


def read_multisource_bronze(
    spark,
    document_keys,
    mysql_keys,
    mysql_dump_keys,
):
    """Read supported Bronze sources and normalize provenance columns."""
    dataframes = []

    if document_keys:
        for key in document_keys:
            path = f"s3a://{MINIO_BUCKET_NAME}/{key}"

            df_document = (
                spark.read.parquet(path)
                .withColumn("_bronze_input_path", lit(key))
                .withColumn("nguon_du_lieu", lit(DOCUMENT_SOURCE))
            )

            dataframes.append(df_document)

        print(
            f"Bronze documents: {len(document_keys)} Parquet file(s)."
        )

    if mysql_keys:
        for key in mysql_keys:
            path = f"s3a://{MINIO_BUCKET_NAME}/{key}"

            df_mysql = (
                spark.read.parquet(path)
                .withColumn("_bronze_input_path", lit(key))
            )

            if "nguon_du_lieu" not in df_mysql.columns:
                df_mysql = df_mysql.withColumn(
                    "nguon_du_lieu",
                    lit(MYSQL_SOURCE),
                )
            else:
                df_mysql = df_mysql.withColumn(
                    "nguon_du_lieu",
                    when(
                        col("nguon_du_lieu").isNull()
                        | (trim(col("nguon_du_lieu")) == ""),
                        lit(MYSQL_SOURCE),
                    ).otherwise(col("nguon_du_lieu")),
                )

            dataframes.append(df_mysql)

        print(
            f"Bronze live MySQL: {len(mysql_keys)} Parquet file(s)."
        )

    if mysql_dump_keys:
        for key in mysql_dump_keys:
            path = f"s3a://{MINIO_BUCKET_NAME}/{key}"

            df_dump = (
                spark.read.parquet(path)
                .withColumn("_bronze_input_path", lit(key))
            )

            if "nguon_du_lieu" not in df_dump.columns:
                df_dump = df_dump.withColumn(
                    "nguon_du_lieu",
                    lit(MYSQL_DUMP_SOURCE),
                )
            else:
                df_dump = df_dump.withColumn(
                    "nguon_du_lieu",
                    when(
                        col("nguon_du_lieu").isNull()
                        | (trim(col("nguon_du_lieu")) == ""),
                        lit(MYSQL_DUMP_SOURCE),
                    ).otherwise(col("nguon_du_lieu")),
                )

            dataframes.append(df_dump)

        print(
            f"Bronze MySQL dump: {len(mysql_dump_keys)} Parquet file(s)."
        )

    if not dataframes:
        return None

    df_bronze = dataframes[0]

    for df_next in dataframes[1:]:
        df_bronze = df_bronze.unionByName(
            df_next,
            allowMissingColumns=True,
        )

    # Nullable provenance fields. Explicit types avoid NullType when a
    # batch contains only a source that does not provide a given field.
    nullable_lineage_columns = {
        "source_connector_id": "long",
        "source_connector_name": "string",
        "source_file_name": "string",
        "source_upload_id": "long",
        "source_table": "string",
    }

    for column_name, column_type in nullable_lineage_columns.items():
        if column_name not in df_bronze.columns:
            df_bronze = df_bronze.withColumn(
                column_name,
                lit(None).cast(column_type),
            )

    required_columns = [
        "file_nguon",
        "nguon_du_lieu",
        "source_connector_id",
        "source_connector_name",
        "source_file_name",
        "source_upload_id",
        "source_table",
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
        "_bronze_input_path",
    ]

    # Older Bronze files remain compatible.
    for column_name in required_columns:
        if column_name not in df_bronze.columns:
            df_bronze = df_bronze.withColumn(
                column_name,
                lit(None),
            )

    return df_bronze.select(*required_columns)


def backfill_existing_source_labels(spark):
    """Gán nhãn nguồn cho các dòng Silver cũ được tạo trước khi có nguon_du_lieu."""
    spark.sql(f"""
        UPDATE {SILVER_TABLE}
        SET nguon_du_lieu = '{DOCUMENT_SOURCE}'
        WHERE nguon_du_lieu IS NULL OR TRIM(nguon_du_lieu) = ''
    """)
    print(f"🏷️ Đã bảo đảm các dòng Silver cũ có nguon_du_lieu='{DOCUMENT_SOURCE}'.")


def archive_processed_bronze_files(s3_client, processed_keys):
    """Chỉ archive đúng các file đã tham gia batch Silver thành công."""
    if not processed_keys:
        return

    for key in processed_keys:
        archive_key = key.replace(BRONZE_PREFIX, BRONZE_ARCHIVE_PREFIX, 1)
        s3_client.copy_object(
            Bucket=MINIO_BUCKET_NAME,
            CopySource=f"{MINIO_BUCKET_NAME}/{key}",
            Key=archive_key,
        )
        s3_client.delete_object(Bucket=MINIO_BUCKET_NAME, Key=key)

    print(f"🗑️ Đã archive {len(processed_keys)} file Bronze đã xử lý sang '{BRONZE_ARCHIVE_PREFIX}'.")

def run_bronze_to_silver(spark, run_id=""):
    """Thực thi toàn bộ luồng Bronze -> Silver trên SparkSession được truyền vào."""
    # Đảm bảo bảng Silver luôn tồn tại trên main
    init_silver_table_if_needed(spark, "main")

    s3_client = get_s3_client()
    document_keys, mysql_keys, mysql_dump_keys = (
        list_new_bronze_parquet_keys(s3_client)
    )
    parquet_contents = (
        document_keys
        + mysql_keys
        + mysql_dump_keys
    )

    if not parquet_contents:
        print(f"ℹ️ Không tìm thấy Bronze Parquet mới từ tài liệu hoặc MySQL trong '{BRONZE_PREFIX}'. Bỏ qua bước Silver.")
        return True

    print(
        f"📁 Tìm thấy {len(parquet_contents)} file Bronze cần nạp "
        f"({len(document_keys)} documents, "
        f"{len(mysql_keys)} MySQL live, "
        f"{len(mysql_dump_keys)} MySQL dump)."
    )
    branch_name = make_branch_name("ingest_bronze_silver")

    try:
        spark.catalog.clearCache()
        create_branch(spark, branch_name, from_ref="main")
        use_branch(spark, branch_name)

        init_silver_table_if_needed(spark, branch_name)
        backfill_existing_source_labels(spark)

        df_bronze = read_multisource_bronze(
            spark,
            document_keys,
            mysql_keys,
            mysql_dump_keys,
        )
        df_staging, df_discarded = dedup_by_business_key(df_bronze)

        save_discarded_duplicates(df_discarded, s3_client)

        df_staging.createOrReplaceTempView("bronze_staging_view")

        spark.sql(f"""
            MERGE INTO {SILVER_TABLE} t
            USING bronze_staging_view s
            ON t.ma_chi_tieu = s.ma_chi_tieu
               AND t.quy_danh_gia = s.quy_danh_gia
               AND t.nhom_don_vi = s.nhom_don_vi
            WHEN MATCHED THEN
              UPDATE SET
                t.file_nguon = s.file_nguon,
                t.nguon_du_lieu = s.nguon_du_lieu,
                t.source_connector_id = s.source_connector_id,
                t.source_connector_name = s.source_connector_name,
                t.source_file_name = s.source_file_name,
                t.source_upload_id = s.source_upload_id,
                t.source_table = s.source_table,
                t.nhom_don_vi = s.nhom_don_vi,
                t.noi_dung_muc_tieu = s.noi_dung_muc_tieu,
                t.dinh_ky_thu_thap = s.dinh_ky_thu_thap,
                t.muc_dang_ky = s.muc_dang_ky,
                t.muc_dang_ky_numeric = s.muc_dang_ky_numeric,
                t.muc_dat = s.muc_dat,
                t.muc_dat_numeric = s.muc_dat_numeric,
                t.ket_qua_he_thong = s.ket_qua_he_thong,
                t.nguyen_nhan = s.nguyen_nhan,
                t.hanh_dong_khac_phuc = s.hanh_dong_khac_phuc,
                t.minh_chung_type = s.minh_chung_type,
                t.minh_chung_path = s.minh_chung_path,
                t.checksum_sha256 = s.checksum_sha256,
                t.thoi_gian_ingest_silver = s.thoi_gian_ingest_silver
            WHEN NOT MATCHED THEN
              INSERT (
                file_nguon, nguon_du_lieu, source_connector_id, source_connector_name,
                source_file_name, source_upload_id, source_table,
                ma_chi_tieu, nhom_don_vi, quy_danh_gia, noi_dung_muc_tieu,
                dinh_ky_thu_thap, muc_dang_ky, muc_dang_ky_numeric, muc_dat, muc_dat_numeric,
                ket_qua_he_thong, nguyen_nhan, hanh_dong_khac_phuc, minh_chung_type, minh_chung_path, checksum_sha256,
                thoi_gian_ingest_silver
              )
              VALUES (
                s.file_nguon, s.nguon_du_lieu, s.source_connector_id, s.source_connector_name,
                s.source_file_name, s.source_upload_id, s.source_table,
                s.ma_chi_tieu, s.nhom_don_vi, s.quy_danh_gia, s.noi_dung_muc_tieu,
                s.dinh_ky_thu_thap, s.muc_dang_ky, s.muc_dang_ky_numeric, s.muc_dat, s.muc_dat_numeric,
                s.ket_qua_he_thong, s.nguyen_nhan, s.hanh_dong_khac_phuc, s.minh_chung_type, s.minh_chung_path, s.checksum_sha256,
                s.thoi_gian_ingest_silver
              )
        """)
        print(f"✅ Đã ghi/cập nhật dữ liệu vào bảng Iceberg trên branch '{branch_name}'.")

        check_quality_silver(spark, SILVER_TABLE)
        merge_branch_to_main(spark, branch_name)
        use_main(spark)

        archive_processed_bronze_files(s3_client, parquet_contents)

        print("\n📊 CHI TIẾT DỮ LIỆU TRONG BẢNG ICEBERG SILVER (main):")
        spark.sql(f"""
            SELECT nguon_du_lieu, source_connector_id, source_connector_name,
                   source_file_name, source_upload_id, source_table,
                   ma_chi_tieu, nhom_don_vi, quy_danh_gia, dinh_ky_thu_thap,
                   muc_dang_ky, muc_dat, ket_qua_he_thong
            FROM {SILVER_TABLE}
            ORDER BY nhom_don_vi, ma_chi_tieu
        """).show(20, truncate=False)

        try:
            om_client = get_client()
            bronze_fqn = ensure_bronze_table(om_client)
            silver_fqn = "lakehouse-trino.lakehouse.silver.kpi_cusc_master"
            push_lineage_safe(
                om_client, bronze_fqn, silver_fqn,
                sql_query=(
                    "MERGE INTO lakehouse.silver.kpi_cusc_master t "
                    "USING bronze_staging_view s ON t.ma_chi_tieu = s.ma_chi_tieu AND t.quy_danh_gia = s.quy_danh_gia AND t.nhom_don_vi = s.nhom_don_vi "
                    "WHEN MATCHED THEN UPDATE * WHEN NOT MATCHED THEN INSERT *"
                ),
                description="Nạp dữ liệu KPI đa nguồn (tài liệu/MySQL) từ Bronze vào Iceberg Silver",
            )
        except Exception as e:
            print(f"⚠️ Không đẩy được lineage lên OpenMetadata (bỏ qua): {e}")

        return True

    except DataQualityError as dqe:
        use_main(spark)
        err_msg = f"Kiểm tra chất lượng thất bại: {dqe}"
        print(f"❌ DỮ LIỆU KHÔNG ĐẠT CHẤT LƯỢNG: {dqe}")
        update_pipeline_error(run_id, err_msg)
        raise dqe

    except Exception as e:
        use_main(spark)
        print(f"❌ Lỗi xử lý đường ống Silver: {str(e)}")
        raise e


def main():
    parser = argparse.ArgumentParser(description="Bronze to Silver")
    parser.add_argument("--run_id", type=str, help="Airflow DAG Run ID", default="")
    args = parser.parse_args()

    sys.stdout.reconfigure(encoding='utf-8')
    spark = get_spark_session()
    try:
        run_bronze_to_silver(spark, args.run_id)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
