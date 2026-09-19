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


_BASE_DIR = Path(__file__).resolve().parent

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

DATASETS = {
    "education.learning_outcomes": ApiDatasetConfig(
        dataset="education.learning_outcomes",
        schema_version="0.1-demo",
        json_schema_path=_BASE_DIR / "contracts" / "learning_outcomes_v0_1.schema.json",
        source_fields=LEARNING_OUTCOMES_FIELDS,
        spark_source_schema=LEARNING_OUTCOMES_SCHEMA,
        bronze_prefix="bronze/api/ctu_ioc/education/learning_outcomes/",
    ),
}


def get_dataset_config(dataset: str) -> ApiDatasetConfig:
    try:
        return DATASETS[dataset]
    except KeyError as exc:
        supported = ", ".join(sorted(DATASETS))
        raise ValueError(f"Unsupported API dataset '{dataset}'. Supported: {supported}") from exc
