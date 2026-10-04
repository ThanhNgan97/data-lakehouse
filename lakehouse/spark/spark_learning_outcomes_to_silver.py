# -*- coding: utf-8 -*-
"""CTU IOC learning_outcomes Bronze -> Silver.

This module is intentionally isolated from the existing KPI Silver pipeline.
It reuses the project's Spark/Iceberg/Nessie runtime and keeps CTU IOC
dataset-specific correctness rules here.

No Gold metrics belong in this layer.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    LongType,
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from api_dataset_registry import (
    LEARNING_OUTCOMES_DATASET,
    get_dataset_config,
)
from env_config import MINIO_BUCKET_NAME
from spark_bronze_to_silver import get_spark_session
from nessie_catalog_utils import (
    create_branch,
    make_branch_name,
    merge_branch_to_main,
    use_branch,
    use_main,
)
from generic_silver_quality import (
    add_dq_reasons as add_configured_dq_reasons,
    split_dq as split_configured_dq,
)
from generic_silver_quarantine import (
    add_quarantine_metadata as add_configured_quarantine_metadata,
    write_quarantine_rows,
)
from generic_silver_dedup import (
    deterministic_deduplicate as deterministic_deduplicate_configured,
)
from generic_silver_conflict import (
    split_equal_timestamp_conflicts as split_configured_equal_timestamp_conflicts,
)
from generic_silver_classifier import (
    classify_against_target as classify_configured_against_target,
)
from generic_silver_merge import (
    merge_into_silver as merge_configured_into_silver,
)


DATASET = LEARNING_OUTCOMES_DATASET
DATASET_CONFIG = get_dataset_config(DATASET)

SILVER_TABLE = DATASET_CONFIG.silver_table
QUARANTINE_TABLE = DATASET_CONFIG.quarantine_table
BUSINESS_KEY = DATASET_CONFIG.business_key

CANONICAL_RECORD_ID_FIELD = DATASET_CONFIG.canonical_field(
    DATASET_CONFIG.record_id_field
)
CANONICAL_UPDATED_AT_FIELD = DATASET_CONFIG.canonical_field(
    DATASET_CONFIG.source_updated_at_field
)
CANONICAL_DELETE_FIELD = DATASET_CONFIG.canonical_field(
    DATASET_CONFIG.source_delete_field
)

BRONZE_METADATA_FIELDS = (
    "_source_system",
    "_source_type",
    "_dataset",
    "_schema_version",
    "_ingestion_mode",
    "_batch_id",
    "_source_updated_at",
    "_ingested_at",
    "_record_checksum",
)


def build_silver_input_schema() -> StructType:
    """Return the explicit nullable schema for known source-native Bronze fields.

    Bronze's normal API contract already requires the source fields.
    Silver intentionally reads them as nullable so corrupted/historical
    Bronze observations can still reach the dataset mapping/DQ boundary
    instead of failing during Parquet decoding.
    """
    bronze_config = get_dataset_config(DATASET)

    source_fields = [
        StructField(field.name, field.dataType, True)
        for field in bronze_config.spark_source_schema.fields
    ]

    metadata_fields = [
        StructField("_source_system", StringType(), True),
        StructField("_source_type", StringType(), True),
        StructField("_dataset", StringType(), True),
        StructField("_schema_version", StringType(), True),
        StructField("_ingestion_mode", StringType(), True),
        StructField("_batch_id", StringType(), True),
        StructField("_source_updated_at", TimestampType(), True),
        StructField("_ingested_at", TimestampType(), True),
        StructField("_record_checksum", StringType(), True),
    ]

    return StructType(source_fields + metadata_fields)


def canonical_business_fields() -> tuple[str, ...]:
    """Return validated canonical Silver fields in source-field order."""
    return DATASET_CONFIG.canonical_fields


def map_source_to_canonical(df: DataFrame) -> DataFrame:
    """Map known source-native Bronze fields to the canonical Silver contract.

    Only configured source fields and the nine Bronze metadata fields cross
    this boundary. Harmless extra source fields remain preserved in Bronze but
    are not promoted automatically into Silver.
    """
    config = DATASET_CONFIG

    missing_source_fields = [
        source_name
        for source_name in config.source_fields
        if source_name not in df.columns
    ]
    if missing_source_fields:
        raise ValueError(
            "Bronze batch is missing required source field(s): "
            f"{missing_source_fields}"
        )

    missing_metadata = [
        field_name
        for field_name in BRONZE_METADATA_FIELDS
        if field_name not in df.columns
    ]
    if missing_metadata:
        raise ValueError(
            "Bronze batch is missing required metadata field(s): "
            f"{missing_metadata}"
        )

    canonical_fields = canonical_business_fields()

    mapped_business_columns = [
        F.col(source_name).alias(canonical_name)
        for source_name, canonical_name
        in zip(config.source_fields, canonical_fields)
    ]

    return df.select(
        *mapped_business_columns,
        *[F.col(field_name) for field_name in BRONZE_METADATA_FIELDS],
    )


# Minimum demo DQ rules required before a row can compete for Silver.
# These are demo-v0.1 assumptions, not confirmed final CTU IOC rules.
DQ_RULES = (
    ("PROGRAM_CODE_NULL", "ma_chuong_trinh IS NULL"),
    ("ACADEMIC_YEAR_NULL", "nam_hoc IS NULL"),
    ("SEMESTER_NULL", "hoc_ky IS NULL"),
    ("SEMESTER_NON_POSITIVE", "hoc_ky <= 0"),
    ("STUDENT_COUNT_NEGATIVE", "so_sinh_vien IS NULL OR so_sinh_vien < 0"),
    ("PASSED_COURSE_COUNT_NEGATIVE", "so_luot_hoc_phan_dat IS NULL OR so_luot_hoc_phan_dat < 0"),
    ("ATTEMPTED_COURSE_COUNT_NEGATIVE", "tong_luot_hoc_phan IS NULL OR tong_luot_hoc_phan < 0"),
    (
        "PASSED_EXCEEDS_ATTEMPTED",
        "so_luot_hoc_phan_dat > tong_luot_hoc_phan",
    ),
    ("GPA_STUDENT_COUNT_NEGATIVE", "so_sinh_vien_tinh_gpa IS NULL OR so_sinh_vien_tinh_gpa < 0"),
    (
        "GPA_STUDENT_COUNT_EXCEEDS_STUDENT_COUNT",
        "so_sinh_vien_tinh_gpa > so_sinh_vien",
    ),
    ("WARNING_STUDENT_COUNT_NEGATIVE", "so_sinh_vien_canh_bao IS NULL OR so_sinh_vien_canh_bao < 0"),
    (
        "WARNING_STUDENT_COUNT_EXCEEDS_STUDENT_COUNT",
        "so_sinh_vien_canh_bao > so_sinh_vien",
    ),
    (
        "DROPOUT_RISK_STUDENT_COUNT_NEGATIVE",
        "so_sinh_vien_nguy_co_nghi_hoc IS NULL OR so_sinh_vien_nguy_co_nghi_hoc < 0",
    ),
    (
        "DROPOUT_RISK_EXCEEDS_STUDENT_COUNT",
        "so_sinh_vien_nguy_co_nghi_hoc > so_sinh_vien",
    ),
    ("ON_TRACK_STUDENT_COUNT_NEGATIVE", "so_sinh_vien_dung_tien_do IS NULL OR so_sinh_vien_dung_tien_do < 0"),
    (
        "PROGRESS_EVALUATED_STUDENT_COUNT_NEGATIVE",
        "so_sinh_vien_danh_gia_tien_do IS NULL OR so_sinh_vien_danh_gia_tien_do < 0",
    ),
    (
        "ON_TRACK_EXCEEDS_PROGRESS_EVALUATED",
        "so_sinh_vien_dung_tien_do > so_sinh_vien_danh_gia_tien_do",
    ),
)


def init_silver_tables_if_needed(spark: SparkSession) -> None:
    """Create CTU IOC Silver and quarantine Iceberg tables if absent.

    This intentionally does not auto-drop or auto-repair Nessie keys.
    Metadata/catalog failures must remain visible instead of being
    destructively repaired.
    """
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.silver")

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {SILVER_TABLE} (
            ma_ban_ghi STRING,
            ma_chuong_trinh STRING,
            ten_chuong_trinh STRING,
            nam_hoc STRING,
            hoc_ky INT,
            so_sinh_vien BIGINT,
            so_luot_hoc_phan_dat BIGINT,
            tong_luot_hoc_phan BIGINT,
            tong_diem_gpa DOUBLE,
            so_sinh_vien_tinh_gpa BIGINT,
            so_sinh_vien_canh_bao BIGINT,
            so_sinh_vien_nguy_co_nghi_hoc BIGINT,
            so_sinh_vien_dung_tien_do BIGINT,
            so_sinh_vien_danh_gia_tien_do BIGINT,
            thoi_gian_cap_nhat_nguon TIMESTAMP,
            da_xoa BOOLEAN,

            _source_system STRING,
            _source_type STRING,
            _dataset STRING,
            _schema_version STRING,
            _ingestion_mode STRING,
            _batch_id STRING,
            _source_updated_at TIMESTAMP,
            _ingested_at TIMESTAMP,
            _record_checksum STRING,

            _silver_updated_at TIMESTAMP
        )
        USING iceberg
    """)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {QUARANTINE_TABLE} (
            ma_ban_ghi STRING,
            ma_chuong_trinh STRING,
            ten_chuong_trinh STRING,
            nam_hoc STRING,
            hoc_ky INT,
            so_sinh_vien BIGINT,
            so_luot_hoc_phan_dat BIGINT,
            tong_luot_hoc_phan BIGINT,
            tong_diem_gpa DOUBLE,
            so_sinh_vien_tinh_gpa BIGINT,
            so_sinh_vien_canh_bao BIGINT,
            so_sinh_vien_nguy_co_nghi_hoc BIGINT,
            so_sinh_vien_dung_tien_do BIGINT,
            so_sinh_vien_danh_gia_tien_do BIGINT,
            thoi_gian_cap_nhat_nguon TIMESTAMP,
            da_xoa BOOLEAN,

            _source_system STRING,
            _source_type STRING,
            _dataset STRING,
            _schema_version STRING,
            _ingestion_mode STRING,
            _batch_id STRING,
            _source_updated_at TIMESTAMP,
            _ingested_at TIMESTAMP,
            _record_checksum STRING,

            _business_key STRING,
            rejection_reason STRING,
            rejected_at TIMESTAMP
        )
        USING iceberg
    """)


def bronze_batch_path(batch_id: str) -> str:
    """Return the exact Bronze Parquet path for one API ingestion batch."""
    config = get_dataset_config(DATASET)
    prefix = config.bronze_prefix.strip("/")
    return (
        f"s3a://{MINIO_BUCKET_NAME}/"
        f"{prefix}/batch_id={batch_id}/data.parquet"
    )


def read_bronze_batch(
    spark: SparkSession,
    batch_id: str,
) -> DataFrame:
    """Read one real Bronze API batch with an explicit nullable schema."""
    return (
        spark.read
        .schema(build_silver_input_schema())
        .parquet(bronze_batch_path(batch_id))
    )


def add_dq_reasons(df: DataFrame) -> DataFrame:
    """Execute the current Learning Outcomes DQ rules via generic mechanics."""
    return add_configured_dq_reasons(
        df,
        DQ_RULES,
    )


def split_dq(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Split rows with generic mechanics and dataset-specific DQ rules."""
    return split_configured_dq(
        df,
        DQ_RULES,
    )


def split_equal_timestamp_conflicts(
    df: DataFrame,
) -> tuple[DataFrame, DataFrame]:
    """Apply the frozen source-conflict policy via generic mechanics."""
    return split_configured_equal_timestamp_conflicts(
        df,
        business_key=BUSINESS_KEY,
        source_updated_field=CANONICAL_UPDATED_AT_FIELD,
        checksum_field="_record_checksum",
    )


def deterministic_deduplicate(df: DataFrame) -> DataFrame:
    """Apply the frozen Learning Outcomes dedup policy via generic mechanics."""
    return deterministic_deduplicate_configured(
        df,
        business_key=BUSINESS_KEY,
        source_updated_field=CANONICAL_UPDATED_AT_FIELD,
        ingested_at_field="_ingested_at",
        batch_id_field="_batch_id",
        checksum_field="_record_checksum",
        record_id_field=CANONICAL_RECORD_ID_FIELD,
    )


def add_quarantine_metadata(df: DataFrame) -> DataFrame:
    """Attach quarantine metadata via reusable mechanics."""
    return add_configured_quarantine_metadata(
        df,
        BUSINESS_KEY,
    )


def write_quarantine(df: DataFrame) -> int:
    """Append rejected rows via reusable quarantine routing mechanics."""
    columns = (
        list(canonical_business_fields())
        + list(BRONZE_METADATA_FIELDS)
        + [
            "_business_key",
            "rejection_reason",
            "rejected_at",
        ]
    )

    return write_quarantine_rows(
        df,
        target_table=QUARANTINE_TABLE,
        business_key=BUSINESS_KEY,
        output_columns=columns,
    )


def classify_against_target(
    spark: SparkSession,
    df: DataFrame,
) -> tuple[DataFrame, DataFrame, DataFrame, DataFrame]:
    """Apply the frozen target-state classifier via generic mechanics."""
    source_columns = (
        list(canonical_business_fields())
        + list(BRONZE_METADATA_FIELDS)
    )

    return classify_configured_against_target(
        spark,
        df,
        target_table=SILVER_TABLE,
        business_key=BUSINESS_KEY,
        source_updated_field=CANONICAL_UPDATED_AT_FIELD,
        checksum_field="_record_checksum",
        delete_field=CANONICAL_DELETE_FIELD,
        source_columns=source_columns,
    )


def merge_into_silver(
    spark: SparkSession,
    df: DataFrame,
) -> int:
    """MERGE via reusable mechanics while preserving the frozen guards.

    Compatibility contract verified by the Day 4 freeze suite:
      s.{CANONICAL_UPDATED_AT_FIELD} > t.{CANONICAL_UPDATED_AT_FIELD}
      WHEN NOT MATCHED
      AND s.{CANONICAL_DELETE_FIELD} = false
    """
    source_columns = (
        list(canonical_business_fields())
        + list(BRONZE_METADATA_FIELDS)
    )

    return merge_configured_into_silver(
        spark,
        df,
        target_table=SILVER_TABLE,
        business_key=BUSINESS_KEY,
        source_columns=source_columns,
        source_updated_field=CANONICAL_UPDATED_AT_FIELD,
        delete_field=CANONICAL_DELETE_FIELD,
        source_view="learning_outcomes_merge_source",
    )


def business_key_sql(
    source_alias: str = "s",
    target_alias: str = "t",
) -> str:
    """Return the exact logical business-key condition."""
    return " AND ".join(
        f"{target_alias}.{column} = {source_alias}.{column}"
        for column in BUSINESS_KEY
    )


if __name__ == "__main__":
    print("CTU IOC Silver foundation loaded.")
    print(f"Dataset: {DATASET}")
    print(f"Silver table: {SILVER_TABLE}")
    print(f"Quarantine table: {QUARANTINE_TABLE}")
    print(f"Business key: {BUSINESS_KEY}")
    print(f"DQ rules: {len(DQ_RULES)}")

def process_learning_outcomes_batch(
    spark: SparkSession,
    batch_id: str,
    merge_to_main: bool = True,
):
    """Process one Bronze CTU IOC batch through Silver on a Nessie branch."""

    branch_name = None

    try:
        use_main(spark)
        init_silver_tables_if_needed(spark)

        main_silver_before = spark.table(
            SILVER_TABLE
        ).count()

        main_quarantine_before = spark.table(
            QUARANTINE_TABLE
        ).count()

        branch_name = make_branch_name(
            "ctu_ioc_learning_outcomes"
        )

        print("=== SILVER CTU IOC BATCH START ===")
        print(f"BATCH_ID={batch_id}")
        print(f"BRANCH_NAME={branch_name}")
        print(
            f"MAIN_SILVER_BEFORE="
            f"{main_silver_before}"
        )
        print(
            f"MAIN_QUARANTINE_BEFORE="
            f"{main_quarantine_before}"
        )

        create_branch(
            spark,
            branch_name,
            from_ref="main",
        )

        use_branch(
            spark,
            branch_name,
        )

        quarantine_before = spark.table(
            QUARANTINE_TABLE
        ).count()

        bronze_source_native = read_bronze_batch(
            spark,
            batch_id,
        )

        bronze = map_source_to_canonical(
            bronze_source_native
        ).localCheckpoint(
            eager=True
        )

        bronze_count = bronze.count()

        valid, dq_invalid = split_dq(
            bronze
        )

        dq_invalid = dq_invalid.localCheckpoint(
            eager=True
        )

        (
            source_mergeable,
            source_conflicts,
        ) = split_equal_timestamp_conflicts(
            valid
        )

        source_conflicts = (
            source_conflicts.localCheckpoint(
                eager=True
            )
        )

        dedup = deterministic_deduplicate(
            source_mergeable
        ).localCheckpoint(
            eager=True
        )

        (
            mergeable,
            target_quarantine,
            stale,
            duplicate,
        ) = classify_against_target(
            spark,
            dedup,
        )

        mergeable = mergeable.localCheckpoint(
            eager=True
        )

        target_quarantine = (
            target_quarantine.localCheckpoint(
                eager=True
            )
        )

        stale = stale.localCheckpoint(
            eager=True
        )

        duplicate = duplicate.localCheckpoint(
            eager=True
        )

        dq_invalid_count = dq_invalid.count()
        source_conflict_count = (
            source_conflicts.count()
        )
        dedup_count = dedup.count()
        mergeable_count = mergeable.count()
        target_quarantine_count = (
            target_quarantine.count()
        )
        stale_count = stale.count()
        duplicate_count = duplicate.count()

        all_quarantine = (
            dq_invalid
            .unionByName(
                source_conflicts,
                allowMissingColumns=True,
            )
            .unionByName(
                target_quarantine,
                allowMissingColumns=True,
            )
            .localCheckpoint(
                eager=True
            )
        )

        quarantine_input_count = (
            all_quarantine.count()
        )

        print("=== SILVER CLASSIFICATION ===")
        print(f"BRONZE_COUNT={bronze_count}")
        print(
            f"DQ_INVALID="
            f"{dq_invalid_count}"
        )
        print(
            f"SOURCE_CONFLICTS="
            f"{source_conflict_count}"
        )
        print(f"DEDUP={dedup_count}")
        print(
            f"MERGEABLE="
            f"{mergeable_count}"
        )
        print(
            f"TARGET_QUARANTINE="
            f"{target_quarantine_count}"
        )
        print(f"STALE={stale_count}")
        print(
            f"DUPLICATE="
            f"{duplicate_count}"
        )
        print(
            f"QUARANTINE_INPUT="
            f"{quarantine_input_count}"
        )

        quarantined_count = write_quarantine(
            all_quarantine
        )

        merge_input_count = merge_into_silver(
            spark,
            mergeable,
        )

        spark.catalog.clearCache()

        silver_branch = (
            spark.table(SILVER_TABLE)
            .localCheckpoint(eager=True)
        )

        branch_silver_count = (
            silver_branch.count()
        )

        duplicate_key_count = (
            silver_branch
            .groupBy(*BUSINESS_KEY)
            .count()
            .filter(F.col("count") > 1)
            .count()
        )

        _, branch_dq_invalid = split_dq(
            silver_branch
        )

        branch_dq_invalid_count = (
            branch_dq_invalid.count()
        )

        branch_quarantine_count = (
            spark.table(
                QUARANTINE_TABLE
            ).count()
        )

        quarantine_delta = (
            branch_quarantine_count
            - quarantine_before
        )

        print("=== BRANCH QUALITY GATE ===")
        print(
            f"BRANCH_SILVER_COUNT="
            f"{branch_silver_count}"
        )
        print(
            f"DUPLICATE_KEY_COUNT="
            f"{duplicate_key_count}"
        )
        print(
            f"SILVER_DQ_INVALID="
            f"{branch_dq_invalid_count}"
        )
        print(
            f"BRANCH_QUARANTINE_COUNT="
            f"{branch_quarantine_count}"
        )
        print(
            f"QUARANTINE_DELTA="
            f"{quarantine_delta}"
        )
        print(
            f"MERGE_INPUT_COUNT="
            f"{merge_input_count}"
        )
        print(
            f"QUARANTINED_COUNT="
            f"{quarantined_count}"
        )

        if duplicate_key_count != 0:
            raise RuntimeError(
                "SILVER QUALITY FAILED: "
                "duplicate business keys"
            )

        if branch_dq_invalid_count != 0:
            raise RuntimeError(
                "SILVER QUALITY FAILED: "
                "invalid rows reached Silver"
            )

        if quarantine_delta != quarantined_count:
            raise RuntimeError(
                "SILVER QUALITY FAILED: "
                "quarantine count mismatch"
            )

        if merge_to_main:
            use_main(spark)

            merge_branch_to_main(
                spark,
                branch_name,
            )

            use_main(spark)
            spark.catalog.clearCache()

            main_silver_after = (
                spark.table(
                    SILVER_TABLE
                ).count()
            )

            main_quarantine_after = (
                spark.table(
                    QUARANTINE_TABLE
                ).count()
            )

            print("=== MAIN AFTER BRANCH MERGE ===")
            print(
                f"MAIN_SILVER_AFTER="
                f"{main_silver_after}"
            )
            print(
                f"MAIN_QUARANTINE_AFTER="
                f"{main_quarantine_after}"
            )
        else:
            main_silver_after = (
                main_silver_before
            )
            main_quarantine_after = (
                main_quarantine_before
            )

        result = {
            "batch_id": batch_id,
            "branch_name": branch_name,
            "bronze_count": bronze_count,
            "dq_invalid": dq_invalid_count,
            "source_conflicts": source_conflict_count,
            "dedup": dedup_count,
            "mergeable": mergeable_count,
            "target_quarantine": target_quarantine_count,
            "stale": stale_count,
            "duplicate": duplicate_count,
            "quarantined": quarantined_count,
            "merge_input": merge_input_count,
            "branch_silver_count": branch_silver_count,
            "duplicate_key_count": duplicate_key_count,
            "silver_dq_invalid": branch_dq_invalid_count,
            "main_silver_after": main_silver_after,
            "main_quarantine_after": main_quarantine_after,
        }

        print("SILVER_BATCH_PROCESS_PASS")

        return result

    finally:
        try:
            use_main(spark)
        except Exception:
            pass

        if branch_name:
            print(
                f"BRANCH_LEFT_FOR_AUDIT="
                f"{branch_name}"
            )
