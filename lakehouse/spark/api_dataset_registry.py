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
    source_delete_field: str
    sample_validation_fields: tuple[str, ...]
    source_to_canonical: dict[str, str]
    business_key: tuple[str, ...]
    silver_table: str
    quarantine_table: str

    def __post_init__(self) -> None:
        """Fail fast when declarative dataset configuration is inconsistent."""
        if not self.dataset:
            raise ValueError("dataset must not be empty")

        if not self.schema_version:
            raise ValueError("schema_version must not be empty")

        if not self.source_fields:
            raise ValueError("source_fields must not be empty")

        if len(set(self.source_fields)) != len(self.source_fields):
            raise ValueError("source_fields contains duplicates")

        spark_field_names = tuple(
            field.name
            for field in self.spark_source_schema.fields
        )
        if spark_field_names != self.source_fields:
            raise ValueError(
                "spark_source_schema field order must exactly match source_fields"
            )

        mapping_keys = set(self.source_to_canonical)
        source_field_set = set(self.source_fields)

        missing_mapping = source_field_set - mapping_keys
        extra_mapping = mapping_keys - source_field_set
        if missing_mapping or extra_mapping:
            raise ValueError(
                "source_to_canonical must map exactly source_fields; "
                f"missing={sorted(missing_mapping)}, "
                f"extra={sorted(extra_mapping)}"
            )

        canonical_fields = self.canonical_fields
        if len(set(canonical_fields)) != len(canonical_fields):
            raise ValueError(
                "source_to_canonical contains duplicate canonical fields"
            )

        required_source_fields = (
            self.record_id_field,
            self.source_updated_at_field,
            self.source_delete_field,
        )
        missing_required_source_fields = [
            field_name
            for field_name in required_source_fields
            if field_name not in source_field_set
        ]
        if missing_required_source_fields:
            raise ValueError(
                "configured source control field(s) missing from source_fields: "
                f"{missing_required_source_fields}"
            )

        invalid_sample_fields = [
            field_name
            for field_name in self.sample_validation_fields
            if field_name not in source_field_set
        ]
        if invalid_sample_fields:
            raise ValueError(
                "sample_validation_fields contains unknown source field(s): "
                f"{invalid_sample_fields}"
            )

        canonical_field_set = set(canonical_fields)
        invalid_business_key = [
            field_name
            for field_name in self.business_key
            if field_name not in canonical_field_set
        ]
        if not self.business_key or invalid_business_key:
            raise ValueError(
                "business_key must contain configured canonical field(s); "
                f"invalid={invalid_business_key}"
            )

        if not self.silver_table:
            raise ValueError("silver_table must not be empty")

        if not self.quarantine_table:
            raise ValueError("quarantine_table must not be empty")

    @property
    def canonical_fields(self) -> tuple[str, ...]:
        """Return canonical fields in configured source-field order."""
        return tuple(
            self.source_to_canonical[source_name]
            for source_name in self.source_fields
        )

    def canonical_field(self, source_field: str) -> str:
        """Resolve one configured source field to its canonical name."""
        try:
            return self.source_to_canonical[source_field]
        except KeyError as exc:
            raise ValueError(
                f"Source field '{source_field}' has no canonical mapping"
            ) from exc


_BASE_DIR = Path(__file__).resolve().parent

LEARNING_OUTCOMES_DATASET = "education.learning_outcomes"

LEARNING_OUTCOMES_FIELDS = (
    "record_id",
    "program_code",
    "program_name",
    "academic_year",
    "semester",
    "student_count",
    "passed_course_count",
    "attempted_course_count",
    "gpa_point_sum",
    "gpa_student_count",
    "warning_student_count",
    "dropout_risk_student_count",
    "on_track_student_count",
    "progress_evaluated_student_count",
    "updated_at",
    "is_deleted",
)

LEARNING_OUTCOMES_SCHEMA = StructType([
    StructField("record_id", StringType(), False),
    StructField("program_code", StringType(), False),
    StructField("program_name", StringType(), False),
    StructField("academic_year", StringType(), False),
    StructField("semester", IntegerType(), False),
    StructField("student_count", LongType(), False),
    StructField("passed_course_count", LongType(), False),
    StructField("attempted_course_count", LongType(), False),
    StructField("gpa_point_sum", DoubleType(), False),
    StructField("gpa_student_count", LongType(), False),
    StructField("warning_student_count", LongType(), False),
    StructField("dropout_risk_student_count", LongType(), False),
    StructField("on_track_student_count", LongType(), False),
    StructField("progress_evaluated_student_count", LongType(), False),
    StructField("updated_at", TimestampType(), False),
    StructField("is_deleted", BooleanType(), False),
])

LEARNING_OUTCOMES_SOURCE_TO_CANONICAL = {
    "record_id": "ma_ban_ghi",
    "program_code": "ma_chuong_trinh",
    "program_name": "ten_chuong_trinh",
    "academic_year": "nam_hoc",
    "semester": "hoc_ky",
    "student_count": "so_sinh_vien",
    "passed_course_count": "so_luot_hoc_phan_dat",
    "attempted_course_count": "tong_luot_hoc_phan",
    "gpa_point_sum": "tong_diem_gpa",
    "gpa_student_count": "so_sinh_vien_tinh_gpa",
    "warning_student_count": "so_sinh_vien_canh_bao",
    "dropout_risk_student_count": "so_sinh_vien_nguy_co_nghi_hoc",
    "on_track_student_count": "so_sinh_vien_dung_tien_do",
    "progress_evaluated_student_count": "so_sinh_vien_danh_gia_tien_do",
    "updated_at": "thoi_gian_cap_nhat_nguon",
    "is_deleted": "da_xoa",
}

DATASETS = {
    LEARNING_OUTCOMES_DATASET: ApiDatasetConfig(
        dataset=LEARNING_OUTCOMES_DATASET,
        schema_version="2.0",
        json_schema_path=_BASE_DIR / "contracts" / "learning_outcomes.schema.json",
        source_fields=LEARNING_OUTCOMES_FIELDS,
        spark_source_schema=LEARNING_OUTCOMES_SCHEMA,
        bronze_prefix="bronze/api/ctu_ioc/education/learning_outcomes/",
        record_id_field="record_id",
        source_updated_at_field="updated_at",
        source_delete_field="is_deleted",
        sample_validation_fields=(
            "program_code",
            "academic_year",
            "semester",
            "student_count",
            "gpa_point_sum",
            "is_deleted",
        ),
        source_to_canonical=LEARNING_OUTCOMES_SOURCE_TO_CANONICAL,
        business_key=(
            "ma_chuong_trinh",
            "nam_hoc",
            "hoc_ky",
        ),
        silver_table="lakehouse.silver.learning_outcomes",
        quarantine_table="lakehouse.silver.learning_outcomes_quarantine",
    ),
}


def get_dataset_config(dataset: str) -> ApiDatasetConfig:
    try:
        return DATASETS[dataset]
    except KeyError as exc:
        supported = ", ".join(sorted(DATASETS))
        raise ValueError(f"Unsupported API dataset '{dataset}'. Supported: {supported}") from exc
