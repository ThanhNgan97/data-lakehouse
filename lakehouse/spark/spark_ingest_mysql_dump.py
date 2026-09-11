# -*- coding: utf-8 -*-
"""Safe MySQL dump -> canonical Bronze adapter.

Supported inputs:
- local .sql path, for development/regression;
- exact MinIO staging object key.

SQL text is parsed only. It is never executed.
"""

from __future__ import annotations

import argparse
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
    )


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

    args = parser.parse_args()

    if bool(args.path) == bool(args.object_key):
        parser.error(
            "Provide exactly one input: local path or --object_key."
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
            "minh_chung_type",
            "minh_chung_path",
            "run_id",
        ).show(10, truncate=False)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
