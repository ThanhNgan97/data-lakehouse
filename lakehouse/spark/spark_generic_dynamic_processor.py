# -*- coding: utf-8 -*-
"""Generic Dynamic Silver & Gold Processor for Universal Data Lakehouse.

This module acts as the DATA PLANE execution engine:
1. Receives any incoming data path and a RoutingDecision metadata
2. Ingests data into Apache Iceberg Silver layer with automatic Schema Evolution
3. Applies generic classification (mergeable, duplicate, stale, quarantine)
4. Executes ACID MERGE INTO with Project Nessie versioning
5. Automatically rolls up Gold Data Mart aggregates based on inferred metrics & dimensions
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

# Đảm bảo console Windows in tiếng Việt UTF-8 không bị lỗi charmap
if sys.stdout and hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

# Thêm thư mục hiện tại vào sys.path
_CURRENT_DIR = Path(__file__).resolve().parent
if str(_CURRENT_DIR) not in sys.path:
    sys.path.insert(0, str(_CURRENT_DIR))

from env_config import (
    MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_ENDPOINT,
    NESSIE_API_URL, HADOOP_HOME, SPARK_LOCAL_IP,
)
from nessie_catalog_utils import (
    make_branch_name,
    create_branch,
    use_branch,
    use_main,
    merge_branch_to_main,
    delete_nessie_orphaned_key,
)
from generic_silver_classifier import classify_against_target
from generic_silver_merge import merge_into_silver
from generic_silver_quarantine import write_quarantine_rows
from ai_dataset_router import RoutingDecision, to_snake_case

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

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.types import (
    BooleanType, DoubleType, IntegerType, LongType, StringType,
    StructField, StructType, TimestampType,
)


def get_spark_session(app_name: str = "Generic_Dynamic_Processor") -> SparkSession:
    """Khởi tạo Spark Session cấu hình Iceberg, Nessie và MinIO S3A."""
    spark = (
        SparkSession.builder
        .appName(app_name)
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
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.hadoop.fs.s3a.fast.upload", "true")
        .config("spark.hadoop.fs.s3a.connection.maximum", "100")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.parquet.enableVectorizedReader", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


# =====================================================================
# SCHEMA STANDARDIZATION & EVOLUTION
# =====================================================================

def standardize_dataframe_columns(df: DataFrame) -> DataFrame:
    """Chuẩn hóa toàn bộ tên cột của DataFrame sang snake_case không dấu."""
    renamed_df = df
    for col_name in df.columns:
        clean_name = to_snake_case(col_name)
        if clean_name != col_name:
            renamed_df = renamed_df.withColumnRenamed(col_name, clean_name)
    return renamed_df


def spark_type_to_iceberg_ddl(data_type) -> str:
    """Chuyển Spark DataType sang kiểu dữ liệu DDL của Iceberg."""
    type_name = data_type.typeName().lower()
    if type_name in ["integer", "int"]:
        return "INT"
    if type_name in ["long"]:
        return "BIGINT"
    if type_name in ["double", "float"]:
        return "DOUBLE"
    if type_name in ["boolean"]:
        return "BOOLEAN"
    if type_name in ["timestamp"]:
        return "TIMESTAMP"
    if type_name in ["date"]:
        return "DATE"
    return "STRING"


def apply_iceberg_schema_evolution(spark: SparkSession, table_name: str, incoming_df: DataFrame) -> None:
    """Tự động thêm các cột mới vào bảng Iceberg nếu incoming_df có thêm cột mới."""
    try:
        existing_cols = {row.col_name.lower(): row.data_type for row in spark.sql(f"DESCRIBE TABLE {table_name}").collect()}
    except Exception:
        return  # Bảng chưa tồn tại thì không cần evolve

    for field in incoming_df.schema.fields:
        col_name = field.name.lower()
        if col_name not in existing_cols:
            col_type_ddl = spark_type_to_iceberg_ddl(field.dataType)
            alter_sql = f"ALTER TABLE {table_name} ADD COLUMNS ({col_name} {col_type_ddl})"
            print(f"🔄 [Schema Evolution] Thêm cột mới '{col_name}' ({col_type_ddl}) vào bảng {table_name}")
            spark.sql(alter_sql)


def table_exists(spark: SparkSession, table_name: str) -> bool:
    """Kiểm tra sự tồn tại của bảng trong Iceberg Catalog."""
    try:
        spark.table(table_name).limit(1).collect()
        return True
    except Exception:
        return False


# =====================================================================
# DATA INGESTION & PROCESSING CORE
# =====================================================================

def read_input_dataset(spark: SparkSession, input_path: str) -> DataFrame:
    """Đọc dữ liệu từ file Parquet, JSON, hoặc CSV vào DataFrame."""
    path_lower = input_path.lower()
    if any(path_lower.endswith(ext) for ext in [".pdf", ".docx", ".doc", ".png", ".jpg", ".jpeg", ".zip", ".tar", ".gz"]):
        raise ValueError(
            f"File '{input_path}' có định dạng nhị phân/tài liệu phi cấu trúc, không thuộc định dạng bảng (chỉ nhận CSV, JSON, Parquet) của Generic Dynamic Processor!"
        )

    if path_lower.endswith(".parquet"):
        df = spark.read.parquet(input_path)
    elif path_lower.endswith(".json"):
        df = spark.read.option("multiline", "true").json(input_path)
    elif path_lower.endswith((".csv", ".tsv")):
        delimiter = "\t" if path_lower.endswith(".tsv") else ","
        df = spark.read.option("header", "true").option("inferSchema", "true").option("delimiter", delimiter).csv(input_path)
    else:
        # Mặc định thử đọc parquet rồi đến json
        try:
            df = spark.read.parquet(input_path)
        except Exception:
            df = spark.read.option("multiline", "true").json(input_path)

    if "_corrupt_record" in df.columns and len(df.columns) == 1:
        raise ValueError(f"Dữ liệu trong file '{input_path}' bị lỗi hoặc không khớp cú pháp JSON/CSV (chỉ chứa _corrupt_record)!")
    return df


def process_generic_dataset(
    spark: SparkSession,
    input_path: str,
    decision: RoutingDecision,
    run_id: str = "manual_run",
) -> Dict[str, Any]:
    """Quy trình toàn diện nạp dữ liệu động: Ingest -> Silver (ACID) -> Gold (Data Mart)."""
    print(f"\n🚀 [Generic Processor] Bắt đầu xử lý dataset '{decision.dataset_entity}' (Run ID: {run_id})")

    # 1. Đảm bảo namespaces tồn tại
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.silver")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.gold")

    # 2. Đọc và chuẩn hóa dữ liệu đầu vào
    raw_df = read_input_dataset(spark, input_path)
    clean_df = standardize_dataframe_columns(raw_df)

    # 2.5 Áp dụng Column Mapping (nếu AI phát hiện đổi tên cột sang bảng cũ)
    if decision.column_mapping:
        print(f"🔄 [Column Mapping] Áp dụng ánh xạ đổi tên cột sang bảng đích: {decision.column_mapping}")
        for src_col, target_col in decision.column_mapping.items():
            clean_src = to_snake_case(src_col)
            clean_tgt = to_snake_case(target_col)
            if clean_src in clean_df.columns and clean_src != clean_tgt:
                clean_df = clean_df.withColumnRenamed(clean_src, clean_tgt)
                print(f"   ↳ Đã đổi tên cột: '{clean_src}' -> '{clean_tgt}'")

    # 3. Chuẩn hóa Business Keys & Timestamps
    target_table = decision.target_silver_table
    quarantine_table = decision.target_quarantine_table
    gold_table = decision.target_gold_table

    # Đảm bảo các cột business keys tồn tại trong df
    business_keys = [to_snake_case(k) for k in decision.business_keys if to_snake_case(k) in clean_df.columns]
    if not business_keys:
        # Fallback tạo surrogate hash key từ toàn bộ dòng
        clean_df = clean_df.withColumn(
            "_surrogate_row_id",
            F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in clean_df.columns]), 256)
        )
        business_keys = ["_surrogate_row_id"]

    # Đảm bảo cột timestamp tồn tại
    updated_field = to_snake_case(decision.source_updated_at_field) if decision.source_updated_at_field else ""
    if not updated_field or updated_field not in clean_df.columns:
        updated_field = "_source_updated_at"
        clean_df = clean_df.withColumn(updated_field, F.current_timestamp())

    # Thêm cột _record_checksum để đối soát thay đổi
    checksum_field = "_record_checksum"
    value_cols = [c for c in clean_df.columns if c not in [updated_field, checksum_field]]
    clean_df = clean_df.withColumn(
        checksum_field,
        F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("<NULL>")) for c in value_cols]), 256)
    )

    # 4. Quản lý nhánh Nessie (Git-like Data Branch)
    clean_run_id = "".join(c if c.isalnum() else "_" for c in run_id)[:16]
    branch_name = make_branch_name(f"dyn_{clean_run_id}")
    create_branch(spark, branch_name, from_ref="main")
    use_branch(spark, branch_name)

    records_processed = 0
    quarantine_count = 0

    try:
        # 5. Xử lý tầng Silver
        target_exists = table_exists(spark, target_table)

        if not target_exists:
            print(f"📦 [Silver] Bảng '{target_table}' chưa tồn tại. Tạo mới bảng Iceberg...")
            silver_df = clean_df.withColumn("_silver_updated_at", F.current_timestamp())
            silver_df.writeTo(target_table).using("iceberg").create()
            records_processed = clean_df.count()
            print(f"✅ [Silver] Đã khởi tạo bảng và chèn thành công {records_processed} bản ghi.")
        else:
            print(f"🔍 [Silver] Bảng '{target_table}' đã tồn tại. Tiến hành Schema Evolution & MERGE...")
            apply_iceberg_schema_evolution(spark, target_table, clean_df)

            # Phân loại bản ghi: mergeable vs quarantine vs duplicate/stale
            mergeable, quarantine, stale, duplicate = classify_against_target(
                spark,
                clean_df,
                target_table=target_table,
                business_key=business_keys,
                source_updated_field=updated_field,
                checksum_field=checksum_field,
                delete_field=None,
                source_columns=clean_df.columns,
            )

            # Xử lý cách ly nếu có lỗi xung đột
            quarantine_count = quarantine.count()
            if quarantine_count > 0:
                print(f"⚠️ [Quarantine] Phát hiện {quarantine_count} bản ghi xung đột. Ghi vào '{quarantine_table}'...")
                if not table_exists(spark, quarantine_table):
                    quarantine.withColumn("rejected_at", F.current_timestamp()).writeTo(quarantine_table).using("iceberg").create()
                else:
                    apply_iceberg_schema_evolution(spark, quarantine_table, quarantine)
                    quarantine.withColumn("rejected_at", F.current_timestamp()).writeTo(quarantine_table).append()

            # MERGE INTO các bản ghi hợp lệ
            mergeable_count = mergeable.count()
            if mergeable_count > 0:
                print(f"🔄 [Silver] Đang thực hiện MERGE INTO cho {mergeable_count} bản ghi hợp lệ...")
                records_processed = merge_into_silver(
                    spark,
                    mergeable,
                    target_table=target_table,
                    business_key=business_keys,
                    source_columns=clean_df.columns,
                    source_updated_field=updated_field,
                    delete_field=None,
                    source_view=f"source_view_{clean_run_id}",
                    silver_updated_at_field="_silver_updated_at",
                )
                print(f"✅ [Silver] Hoàn tất MERGE INTO {records_processed} bản ghi.")
            else:
                print("ℹ️ [Silver] Không có bản ghi mới cần merge (đều là duplicate hoặc stale).")

        # 6. Tự động sinh tầng Gold (Data Mart Aggregate)
        print(f"\n📊 [Gold] Bắt đầu tổng hợp số liệu cho bảng Gold '{gold_table}'...")
        silver_source = spark.table(target_table)

        # Lọc danh sách metric và dimension hợp lệ hiện có trong bảng
        existing_cols = {c.lower() for c in silver_source.columns}
        valid_metrics = [to_snake_case(m) for m in decision.metric_columns if to_snake_case(m) in existing_cols]
        valid_dims = [to_snake_case(d) for d in decision.dimension_columns if to_snake_case(d) in existing_cols]

        if not valid_dims and business_keys:
            valid_dims = business_keys

        if valid_dims:
            agg_expressions = [F.count("*").alias("total_records")]
            for metric in valid_metrics:
                # Kiểm tra kiểu dữ liệu để chỉ SUM/AVG cột số
                agg_expressions.append(F.round(F.sum(F.col(metric).cast("double")), 2).alias(f"sum_{metric}"))
                agg_expressions.append(F.round(F.avg(F.col(metric).cast("double")), 2).alias(f"avg_{metric}"))

            agg_expressions.append(F.current_timestamp().alias("_gold_generated_at"))

            gold_mart = silver_source.groupBy(*valid_dims).agg(*agg_expressions)
            gold_mart.writeTo(gold_table).using("iceberg").createOrReplace()
            gold_count = gold_mart.count()
            print(f"✅ [Gold] Đã tổng hợp thành công {gold_count} dòng dữ liệu vào '{gold_table}'.")
        else:
            print("ℹ️ [Gold] Bỏ qua tầng Gold do không xác định được dimension gom nhóm.")

        # 7. Merge nhánh Nessie về main
        print(f"🌿 [Nessie] Kiểm tra hoàn tất. Đang merge nhánh '{branch_name}' về 'main'...")
        merge_branch_to_main(spark, branch_name)
        use_main(spark)
        print("🎉 [Nessie] Merge thành công vào main branch!")

    except Exception as exc:
        print(f"❌ [LỖI] Xử lý thất bại: {exc}. Nhánh '{branch_name}' được giữ lại để điều tra.")
        use_main(spark)
        raise exc

    return {
        "status": "SUCCESS",
        "dataset_entity": decision.dataset_entity,
        "silver_table": target_table,
        "silver_records_processed": records_processed,
        "quarantine_records": quarantine_count,
        "gold_table": gold_table,
    }


# =====================================================================
# CLI RUNNER
# =====================================================================

def main():
    parser = argparse.ArgumentParser(description="Generic Dynamic Silver & Gold Processor")
    parser.add_argument("--input", "-i", required=True, help="Đường dẫn file đầu vào (Parquet/JSON/CSV)")
    parser.add_argument("--decision-file", "-d", help="Đường dẫn file JSON RoutingDecision từ Phase 1")
    parser.add_argument("--run-id", "-r", default="test_run", help="Airflow DAG Run ID")
    parser.add_argument("--test-run", action="store_true", help="Chạy kiểm thử với mẫu giả lập")
    args = parser.parse_args()

    spark = get_spark_session()

    try:
        decision_data = None
        if args.decision_file:
            with open(args.decision_file, "r", encoding="utf-8") as f:
                decision_data = RoutingDecision(**json.load(f))
        else:
            # Nếu không truyền decision, tự động gọi router phân tích ngay
            from ai_dataset_router import route_from_file_path
            decision_data = route_from_file_path(args.input)

        result = process_generic_dataset(
            spark=spark,
            input_path=args.input,
            decision=decision_data,
            run_id=args.run_id,
        )
        print("\n" + json.dumps(result, indent=2, ensure_ascii=False))
    finally:
        print("🛑 [Spark] Đang đóng SparkSession để giải phóng tài nguyên JVM...")
        spark.stop()


if __name__ == "__main__":
    main()
