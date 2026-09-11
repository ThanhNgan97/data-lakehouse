# -*- coding: utf-8 -*-
"""Safe MySQL dump -> canonical Bronze adapter.

Supported inputs:
- local .sql path, for development/regression;
- exact MinIO staging object key.

SQL text is parsed only. It is never executed.
"""

from __future__ import annotations

import argparse
import io
from datetime import datetime
from pathlib import Path

import boto3
from botocore.exceptions import ClientError
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp
from pyspark.sql.types import StringType, StructField, StructType

from env_config import (
    MINIO_ACCESS_KEY,
    MINIO_BUCKET_NAME,
    MINIO_ENDPOINT,
    MINIO_SECRET_KEY,
)
from kpi_bronze_mapping import build_kpi_bronze_dataframe
from sql_dump_parser import (
    parse_mysql_dump_file,
    parse_mysql_dump_text,
)


SOURCE_NAME = "MYSQL_DUMP"
SOURCE_PREFIX = "staging/"
BRONZE_PREFIX = "bronze/"
MYSQL_DUMP_BRONZE_PREFIX = f"{BRONZE_PREFIX}data_mysql_dump_extracted_"
MAX_DUMP_BYTES = 50 * 1024 * 1024


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        verify=False,
    )


def get_spark_session():
    spark = (
        SparkSession.builder
        .appName("MySQL_Dump_To_Bronze")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def parsed_table_to_dataframe(spark, parsed_result, table_name):
    table = parsed_result["tables"][table_name]
    columns = table["columns"]
    rows = table["rows"]

    if not columns:
        raise ValueError(
            f"SQL dump does not define schema for table '{table_name}'."
        )

    if not rows:
        raise ValueError(
            f"SQL dump contains no data for table '{table_name}'."
        )

    schema = StructType(
        [
            StructField(column_name, StringType(), True)
            for column_name in columns
        ]
    )

    normalized_rows = [
        tuple(
            None if row.get(column_name) is None
            else str(row.get(column_name))
            for column_name in columns
        )
        for row in rows
    ]

    return spark.createDataFrame(normalized_rows, schema=schema)


def build_dump_bronze_from_parsed(
    spark,
    parsed,
    *,
    run_id="",
    source_uri,
    source_identity,
    source_file_name=None,
    source_upload_id=None,
    source_table="ket_qua_danh_gia",
):
    df_dv = parsed_table_to_dataframe(
        spark,
        parsed,
        "don_vi",
    )
    df_mt = parsed_table_to_dataframe(
        spark,
        parsed,
        "muc_tieu_kpi",
    )
    df_kq = parsed_table_to_dataframe(
        spark,
        parsed,
        "ket_qua_danh_gia",
    )

    if "updated_at" in df_kq.columns:
        df_kq = df_kq.withColumn(
            "updated_at",
            to_timestamp("updated_at"),
        )

    return build_kpi_bronze_dataframe(
        df_kq,
        df_mt,
        df_dv,
        run_id=run_id,
        source_name=SOURCE_NAME,
        source_uri=source_uri,
        evidence_type="mysql_dump",
        source_identity=source_identity,
        source_connector_id=None,
        source_connector_name=None,
        source_file_name=source_file_name,
        source_upload_id=source_upload_id,
        source_table=source_table,
    )


def build_dump_bronze_dataframe(
    spark,
    dump_path,
    *,
    run_id="",
    source_uri=None,
    source_identity=None,
):
    dump_path = Path(dump_path)
    parsed = parse_mysql_dump_file(dump_path)

    return build_dump_bronze_from_parsed(
        spark,
        parsed,
        run_id=run_id,
        source_uri=(
            source_uri
            or f"file://{dump_path.resolve()}"
        ),
        source_identity=(
            source_identity
            or dump_path.name
        ),
        source_file_name=dump_path.name,
        source_upload_id=None,
    )


def read_dump_from_minio(object_key):
    object_key = object_key.strip()

    if not object_key.startswith(SOURCE_PREFIX):
        raise ValueError(
            f"object_key must be inside '{SOURCE_PREFIX}': {object_key}"
        )

    if Path(object_key).suffix.lower() != ".sql":
        raise ValueError(
            f"Only .sql staging objects are supported: {object_key}"
        )

    s3_client = get_s3_client()

    try:
        response = s3_client.get_object(
            Bucket=MINIO_BUCKET_NAME,
            Key=object_key,
        )
    except ClientError as exc:
        error = exc.response.get("Error", {})
        code = str(error.get("Code", ""))

        if code in {"404", "NoSuchKey", "NotFound"}:
            raise FileNotFoundError(
                f"Staging SQL object not found: {object_key}"
            ) from exc

        raise

    raw = response["Body"].read()

    if len(raw) > MAX_DUMP_BYTES:
        raise ValueError(
            f"SQL dump exceeds {MAX_DUMP_BYTES} bytes: {object_key}"
        )

    try:
        text = raw.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise ValueError(
            "SQL dump must use UTF-8 or UTF-8 BOM encoding."
        ) from exc

    return parse_mysql_dump_text(text)


def build_dump_bronze_from_object(
    spark,
    object_key,
    *,
    run_id="",
    upload_id=None,
):
    parsed = read_dump_from_minio(object_key)

    source_identity = (
        f"upload:{upload_id}"
        if upload_id is not None
        else object_key
    )

    return build_dump_bronze_from_parsed(
        spark,
        parsed,
        run_id=run_id,
        source_uri=(
            f"s3://{MINIO_BUCKET_NAME}/{object_key}"
        ),
        source_identity=source_identity,
        source_file_name=Path(object_key).name,
        source_upload_id=upload_id,
    )


def write_single_parquet_to_minio(
    df,
    *,
    upload_id,
):
    """Write one canonical MySQL-dump Bronze Parquet object to MinIO."""

    if upload_id is None or int(upload_id) <= 0:
        raise ValueError(
            "A positive upload_id is required for MySQL dump Bronze output."
        )

    row_count = df.count()

    if row_count == 0:
        print("MySQL dump contains no canonical KPI rows to ingest.")
        return None, 0

    # Microseconds + upload id avoid collisions between concurrent uploads.
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    output_key = (
        f"{MYSQL_DUMP_BRONZE_PREFIX}"
        f"upload_{int(upload_id)}_{timestamp}.parquet"
    )

    pandas_df = df.toPandas()

    parquet_buffer = io.BytesIO()
    pandas_df.to_parquet(
        parquet_buffer,
        index=False,
        engine="pyarrow",
        coerce_timestamps="us",
        allow_truncated_timestamps=True,
    )

    payload = parquet_buffer.getvalue()

    get_s3_client().put_object(
        Bucket=MINIO_BUCKET_NAME,
        Key=output_key,
        Body=payload,
        ContentType="application/octet-stream",
    )

    print(
        f"MYSQL_DUMP -> Bronze: {output_key} "
        f"({row_count} rows, {len(df.columns)} columns)"
    )

    return output_key, row_count


def main():
    parser = argparse.ArgumentParser(
        description="Safe MySQL dump -> canonical Bronze preview"
    )

    parser.add_argument(
        "path",
        nargs="?",
        help="Optional local .sql path",
    )
    parser.add_argument(
        "--object_key",
        default="",
        help="Exact MinIO staging .sql object key",
    )
    parser.add_argument(
        "--upload_id",
        type=int,
        default=None,
        help="UploadHistory ID used for source identity",
    )
    parser.add_argument(
        "--run_id",
        default="",
        help="Airflow DAG Run ID",
    )
    parser.add_argument(
        "--write_bronze",
        action="store_true",
        help="Write canonical Bronze Parquet to MinIO.",
    )

    args = parser.parse_args()

    if bool(args.path) == bool(args.object_key):
        parser.error(
            "Provide exactly one input: local path or --object_key."
        )

    if args.write_bronze and not args.object_key:
        parser.error(
            "--write_bronze requires --object_key."
        )

    if args.write_bronze and (
        args.upload_id is None
        or args.upload_id <= 0
    ):
        parser.error(
            "--write_bronze requires a positive --upload_id."
        )

    spark = get_spark_session()

    try:
        if args.object_key:
            df = build_dump_bronze_from_object(
                spark,
                args.object_key,
                run_id=args.run_id,
                upload_id=args.upload_id,
            )
        else:
            df = build_dump_bronze_dataframe(
                spark,
                args.path,
                run_id=args.run_id,
            )

        print("=== MYSQL DUMP CANONICAL BRONZE PREVIEW ===")
        print("ROW_COUNT:", df.count())
        print("COLUMN_COUNT:", len(df.columns))

        df.select(
            "ma_chi_tieu",
            "nhom_don_vi",
            "quy_danh_gia",
            "muc_dang_ky",
            "muc_dat",
            "muc_dat_numeric",
            "ket_qua_he_thong",
            "nguon_du_lieu",
            "source_connector_id",
            "source_connector_name",
            "source_file_name",
            "source_upload_id",
            "source_table",
            "minh_chung_type",
            "minh_chung_path",
            "run_id",
        ).show(10, truncate=False)

        if args.write_bronze:
            output_key, row_count = write_single_parquet_to_minio(
                df,
                upload_id=args.upload_id,
            )

            if output_key:
                print(
                    "BRONZE_OUTPUT = "
                    f"s3://{MINIO_BUCKET_NAME}/{output_key}"
                )
                print("BRONZE_ROW_COUNT =", row_count)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
