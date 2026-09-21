# -*- coding: utf-8 -*-
"""Dataset configs for generic API -> Bronze ingestion.

Business schemas live in configs; the ingestion engine does not branch with
`if dataset == ...` logic.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


@dataclass(frozen=True)
class ApiDatasetConfig:
    dataset: str
    schema_version: str
    json_schema_path: Path
    source_fields: tuple[str, ...]
    spark_source_schema: StructType
    bronze_prefix: str
    record_id_field: str
    source_updated_at_field: str
    sample_validation_fields: tuple[str, ...]


_BASE_DIR = Path(__file__).resolve().parent

LEARNING_OUTCOMES_FIELDS = (
    "ma_ban_ghi",
    "ma_chuong_trinh",
    "ten_chuong_trinh",
    "nam_hoc",
    "hoc_ky",
    "so_sinh_vien",
    "so_luot_hoc_phan_dat",
    "tong_luot_hoc_phan",
    "tong_diem_gpa",
    "so_sinh_vien_tinh_gpa",
    "so_sinh_vien_canh_bao",
    "so_sinh_vien_nguy_co_nghi_hoc",
    "so_sinh_vien_dung_tien_do",
    "so_sinh_vien_danh_gia_tien_do",
    "thoi_gian_cap_nhat_nguon",
    "da_xoa",
)

LEARNING_OUTCOMES_SCHEMA = StructType([
    StructField("ma_ban_ghi", StringType(), False),
    StructField("ma_chuong_trinh", StringType(), False),
    StructField("ten_chuong_trinh", StringType(), False),
    StructField("nam_hoc", StringType(), False),
    StructField("hoc_ky", IntegerType(), False),
    StructField("so_sinh_vien", LongType(), False),
    StructField("so_luot_hoc_phan_dat", LongType(), False),
    StructField("tong_luot_hoc_phan", LongType(), False),
    StructField("tong_diem_gpa", DoubleType(), False),
    StructField("so_sinh_vien_tinh_gpa", LongType(), False),
    StructField("so_sinh_vien_canh_bao", LongType(), False),
    StructField("so_sinh_vien_nguy_co_nghi_hoc", LongType(), False),
    StructField("so_sinh_vien_dung_tien_do", LongType(), False),
    StructField("so_sinh_vien_danh_gia_tien_do", LongType(), False),
    StructField("thoi_gian_cap_nhat_nguon", TimestampType(), False),
    StructField("da_xoa", BooleanType(), False),
])

DATASETS = {
    "education.learning_outcomes": ApiDatasetConfig(
        dataset="education.learning_outcomes",
        schema_version="1.0",
        json_schema_path=_BASE_DIR / "contracts" / "learning_outcomes_v0_1.schema.json",
        source_fields=LEARNING_OUTCOMES_FIELDS,
        spark_source_schema=LEARNING_OUTCOMES_SCHEMA,
        bronze_prefix="bronze/api/ctu_ioc/education/learning_outcomes/",
        record_id_field="ma_ban_ghi",
        source_updated_at_field="thoi_gian_cap_nhat_nguon",
        sample_validation_fields=(
            "ma_chuong_trinh",
            "nam_hoc",
            "hoc_ky",
            "so_sinh_vien",
            "tong_diem_gpa",
            "da_xoa",
        ),
    ),
}


def get_dataset_config(dataset: str) -> ApiDatasetConfig:
    try:
        return DATASETS[dataset]
    except KeyError as exc:
        supported = ", ".join(sorted(DATASETS))
        raise ValueError(f"Unsupported API dataset '{dataset}'. Supported: {supported}") from exc
