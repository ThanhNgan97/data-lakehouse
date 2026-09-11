# -*- coding: utf-8 -*-
"""MySQL dump -> canonical Bronze adapter.

Phase 1:
- read a .sql dump from a local path;
- parse it without executing SQL;
- convert the three KPI tables to Spark DataFrames;
- reuse the shared KPI Bronze mapping.

This module does not write to MinIO yet.
"""

from __future__ import annotations

import argparse
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp
from pyspark.sql.types import StringType, StructField, StructType

from kpi_bronze_mapping import build_kpi_bronze_dataframe
from sql_dump_parser import parse_mysql_dump_file


SOURCE_NAME = "MYSQL_DUMP"


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
    """Convert one parsed SQL table to a string-based Spark DataFrame."""
    table = parsed_result["tables"][table_name]
    columns = table["columns"]
    rows = table["rows"]

    if not columns:
        raise ValueError(
            f"SQL dump không xác định được schema của bảng '{table_name}'."
        )

    if not rows:
        raise ValueError(
            f"SQL dump không có dữ liệu cho bảng '{table_name}'."
        )

    schema = StructType(
        [
            StructField(column_name, StringType(), True)
            for column_name in columns
        ]
    )

    normalized_rows = [
        tuple(
            None if row.get(column_name) is None else str(row.get(column_name))
            for column_name in columns
        )
        for row in rows
    ]

    return spark.createDataFrame(normalized_rows, schema=schema)


def build_dump_bronze_dataframe(
    spark,
    dump_path,
    *,
    run_id="",
    source_uri=None,
    source_identity=None,
):
    """Parse a MySQL dump and map its KPI tables to canonical Bronze."""
    dump_path = Path(dump_path)
    parsed = parse_mysql_dump_file(dump_path)

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

    resolved_source_uri = source_uri or f"file://{dump_path.resolve()}"
    resolved_identity = source_identity or dump_path.name

    return build_kpi_bronze_dataframe(
        df_kq,
        df_mt,
        df_dv,
        run_id=run_id,
        source_name=SOURCE_NAME,
        source_uri=resolved_source_uri,
        evidence_type="mysql_dump",
        source_identity=resolved_identity,
        source_connector_id=None,
        source_connector_name=None,
    )


def main():
    parser = argparse.ArgumentParser(
        description="Safe MySQL dump -> canonical Bronze preview"
    )
    parser.add_argument(
        "path",
        help="Đường dẫn local tới file .sql",
    )
    parser.add_argument(
        "--run_id",
        default="",
        help="Run ID dùng cho lineage",
    )
    args = parser.parse_args()

    spark = get_spark_session()

    try:
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
            "run_id",
        ).show(10, truncate=False)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
