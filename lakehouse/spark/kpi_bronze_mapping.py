# -*- coding: utf-8 -*-
"""Shared KPI relational tables -> canonical Bronze mapping.

This module contains only source-agnostic KPI transformation logic.

It does NOT:
- connect to MySQL;
- read connector credentials;
- access MinIO;
- execute SQL dumps.

Both live MySQL ingestion and MySQL dump ingestion can provide the same
three logical KPI tables and reuse this mapping.
"""

from pyspark.sql.functions import (
    col,
    coalesce,
    concat_ws,
    current_timestamp,
    lit,
    regexp_replace,
    sha2,
    trim,
    upper,
    when,
)


BRONZE_COLUMNS = [
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
    "source_updated_at",
    "thoi_gian_ingest_bronze",
    "run_id",
]


def parse_simple_numeric(column_name):
    """Parse only simple numeric values such as 100%, 3,69, or 9.5."""
    cleaned = regexp_replace(trim(col(column_name)), ",", ".")
    cleaned = regexp_replace(cleaned, "%", "")

    return when(
        cleaned.rlike(r"^-?[0-9]+(?:\.[0-9]+)?$"),
        cleaned.cast("double"),
    ).otherwise(lit(None).cast("double"))


def normalize_status(column_name):
    """Normalize the KPI status vocabulary while preserving unknown values."""
    raw = upper(trim(col(column_name)))

    return (
        when(raw.contains("CHƯA ĐẾN KỲ"), lit("CHƯA ĐẾN KỲ ĐÁNH GIÁ"))
        .when(raw.contains("KHÔNG ĐẠT"), lit("KHÔNG ĐẠT"))
        .when(raw.contains("ĐẠT"), lit("ĐẠT"))
        .otherwise(raw)
    )


def build_kpi_bronze_dataframe(
    df_kq,
    df_mt,
    df_dv,
    *,
    run_id="",
    source_name,
    source_uri,
    evidence_type,
    source_identity,
    source_connector_id=None,
    source_connector_name=None,
    source_file_name=None,
    source_upload_id=None,
    source_table="ket_qua_danh_gia",
):
    """Join the three KPI tables and produce the canonical Bronze schema.

    Expected logical tables:
    - ket_qua_danh_gia
    - muc_tieu_kpi
    - don_vi

    ``source_identity`` participates in the checksum only for provenance.
    Silver performs business-key deduplication separately.
    """

    kq = df_kq.alias("kq")
    mt = df_mt.alias("mt")
    dv = df_dv.alias("dv")

    joined = (
        kq
        .join(mt, col("kq.ma_chi_tieu") == col("mt.ma_chi_tieu"), "left")
        .join(dv, col("kq.ma_don_vi") == col("dv.ma_don_vi"), "left")
    )

    if "updated_at" in df_kq.columns:
        source_updated_at = col("kq.updated_at")
    else:
        source_updated_at = lit(None).cast("timestamp")

    df = joined.select(
        upper(trim(col("kq.ma_chi_tieu"))).alias("ma_chi_tieu"),
        upper(trim(col("kq.ma_don_vi"))).alias("nhom_don_vi"),
        upper(trim(col("kq.quy_danh_gia"))).alias("quy_danh_gia"),
        trim(col("mt.noi_dung")).alias("noi_dung_muc_tieu"),
        trim(col("mt.dinh_ky")).alias("dinh_ky_thu_thap"),
        trim(col("kq.muc_dang_ky")).alias("muc_dang_ky"),
        trim(col("kq.muc_dat")).alias("muc_dat"),
        normalize_status("kq.ket_qua_he_thong").alias("ket_qua_he_thong"),
        coalesce(trim(col("kq.nguyen_nhan")), lit("")).alias("nguyen_nhan"),
        source_updated_at.alias("source_updated_at"),
    )

    connector_id_expr = (
        lit(int(source_connector_id)).cast("long")
        if source_connector_id is not None
        else lit(None).cast("long")
    )

    connector_name_expr = (
        lit(str(source_connector_name))
        if source_connector_name is not None
        else lit(None).cast("string")
    )

    source_file_name_expr = (
        lit(str(source_file_name))
        if source_file_name is not None
        else lit(None).cast("string")
    )

    source_upload_id_expr = (
        lit(int(source_upload_id)).cast("long")
        if source_upload_id is not None
        else lit(None).cast("long")
    )

    source_table_expr = (
        lit(str(source_table))
        if source_table is not None
        else lit(None).cast("string")
    )

    df = (
        df
        .withColumn("muc_dang_ky_numeric", parse_simple_numeric("muc_dang_ky"))
        .withColumn("muc_dat_numeric", parse_simple_numeric("muc_dat"))
        .withColumn("hanh_dong_khac_phuc", lit(""))
        .withColumn("file_nguon", lit(source_uri))
        .withColumn("minh_chung_type", lit(evidence_type))
        .withColumn("minh_chung_path", lit(source_uri))
        .withColumn("nguon_du_lieu", lit(source_name))
        .withColumn("source_connector_id", connector_id_expr)
        .withColumn("source_connector_name", connector_name_expr)
        .withColumn("source_file_name", source_file_name_expr)
        .withColumn("source_upload_id", source_upload_id_expr)
        .withColumn("source_table", source_table_expr)
        .withColumn("thoi_gian_ingest_bronze", current_timestamp())
        .withColumn("run_id", lit(run_id or ""))
    )

    df = df.withColumn(
        "checksum_sha256",
        sha2(
            concat_ws(
                "||",
                lit(source_name),
                lit(str(source_identity or "")),
                col("ma_chi_tieu"),
                col("nhom_don_vi"),
                col("quy_danh_gia"),
                coalesce(col("muc_dang_ky"), lit("")),
                coalesce(col("muc_dat"), lit("")),
                coalesce(col("ket_qua_he_thong"), lit("")),
                coalesce(col("nguyen_nhan"), lit("")),
            ),
            256,
        ),
    )

    return df.select(*BRONZE_COLUMNS)
