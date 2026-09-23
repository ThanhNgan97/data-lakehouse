# -*- coding: utf-8 -*-
"""Teaching Progress Bronze -> Silver demo pipeline.

IMPORTANT:
- ``education.teaching_progress`` is a DEMO / ASSUMED source contract.
- It is NOT a verified CTU IOC production contract.
- Dataset-specific schema/DQ policy stays here.
- Reusable DQ, quarantine, dedup, conflict, classifier, and MERGE mechanics
  stay in the generic Silver modules.
- No Gold or Superset logic belongs in this layer.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from api_dataset_registry import (
    TEACHING_PROGRESS_DATASET,
    get_dataset_config,
)
from env_config import MINIO_BUCKET_NAME
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


DATASET = TEACHING_PROGRESS_DATASET
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
CANONICAL_DELETE_FIELD = None

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
    """Return nullable known-source + metadata schema for Silver reads."""
    source_fields = [
        StructField(field.name, field.dataType, True)
        for field in DATASET_CONFIG.spark_source_schema.fields
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

    return StructType(
        source_fields + metadata_fields
    )


def canonical_business_fields() -> tuple[str, ...]:
    """Canonical Teaching fields in configured source-field order."""
    return DATASET_CONFIG.canonical_fields


def map_source_to_canonical(df: DataFrame) -> DataFrame:
    """Map configured fields after Bronze; do not promote source extras."""
    missing_source_fields = [
        source_name
        for source_name in DATASET_CONFIG.source_fields
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

    mapped_business_columns = [
        F.col(source_name).alias(canonical_name)
        for source_name, canonical_name
        in DATASET_CONFIG.source_to_canonical.items()
    ]

    return df.select(
        *mapped_business_columns,
        *[
            F.col(field_name)
            for field_name in BRONZE_METADATA_FIELDS
        ],
    )


# DEMO rules only. They are not verified CTU IOC production policy.
DQ_RULES = (
    (
        "MISSING_RECORD_ID",
        "ma_ban_ghi IS NULL OR trim(ma_ban_ghi) = ''",
    ),
    (
        "MISSING_UNIT_CODE",
        "ma_don_vi IS NULL OR trim(ma_don_vi) = ''",
    ),
    (
        "MISSING_COURSE_SECTION_CODE",
        "ma_lop_hoc_phan IS NULL OR trim(ma_lop_hoc_phan) = ''",
    ),
    (
        "MISSING_ACADEMIC_YEAR",
        "nam_hoc IS NULL OR trim(nam_hoc) = ''",
    ),
    (
        "INVALID_SEMESTER",
        "hoc_ky IS NULL OR hoc_ky <= 0",
    ),
    (
        "INVALID_TEACHING_PROGRESS_PERCENT",
        (
            "ty_le_tien_do_giang_day IS NULL "
            "OR ty_le_tien_do_giang_day < 0 "
            "OR ty_le_tien_do_giang_day > 100"
        ),
    ),
    (
        "MISSING_SOURCE_UPDATED_AT",
        "thoi_gian_cap_nhat_nguon IS NULL",
    ),
)


def init_silver_tables_if_needed(
    spark: SparkSession,
) -> None:
    """Create Teaching demo Silver/quarantine Iceberg tables if absent."""
    spark.sql(
        "CREATE NAMESPACE IF NOT EXISTS lakehouse.silver"
    )

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {SILVER_TABLE} (
            ma_ban_ghi STRING,
            ma_don_vi STRING,
            ten_don_vi STRING,
            ma_lop_hoc_phan STRING,
            nam_hoc STRING,
            hoc_ky INT,
            ty_le_tien_do_giang_day DOUBLE,
            thoi_gian_cap_nhat_nguon TIMESTAMP,

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
            ma_don_vi STRING,
            ten_don_vi STRING,
            ma_lop_hoc_phan STRING,
            nam_hoc STRING,
            hoc_ky INT,
            ty_le_tien_do_giang_day DOUBLE,
            thoi_gian_cap_nhat_nguon TIMESTAMP,

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
    """Return one Teaching Bronze Parquet path."""
    prefix = DATASET_CONFIG.bronze_prefix.strip("/")
    return (
        f"s3a://{MINIO_BUCKET_NAME}/"
        f"{prefix}/batch_id={batch_id}/data.parquet"
    )


def read_bronze_batch(
    spark: SparkSession,
    batch_id: str,
) -> DataFrame:
    """Read one Teaching Bronze batch with the known-source nullable schema."""
    return (
        spark.read
        .schema(build_silver_input_schema())
        .parquet(bronze_batch_path(batch_id))
    )


def add_dq_reasons(df: DataFrame) -> DataFrame:
    """Execute Teaching demo DQ rules via generic mechanics."""
    return add_configured_dq_reasons(
        df,
        DQ_RULES,
    )


def split_dq(
    df: DataFrame,
) -> tuple[DataFrame, DataFrame]:
    """Split valid/invalid Teaching rows via generic mechanics."""
    return split_configured_dq(
        df,
        DQ_RULES,
    )


def split_equal_timestamp_conflicts(
    df: DataFrame,
) -> tuple[DataFrame, DataFrame]:
    """Apply the shared equal-timestamp source-conflict mechanics."""
    return split_configured_equal_timestamp_conflicts(
        df,
        business_key=BUSINESS_KEY,
        source_updated_field=CANONICAL_UPDATED_AT_FIELD,
        checksum_field="_record_checksum",
    )


def deterministic_deduplicate(
    df: DataFrame,
) -> DataFrame:
    """Apply shared deterministic dedup using Teaching configuration."""
    return deterministic_deduplicate_configured(
        df,
        business_key=BUSINESS_KEY,
        source_updated_field=CANONICAL_UPDATED_AT_FIELD,
        ingested_at_field="_ingested_at",
        batch_id_field="_batch_id",
        checksum_field="_record_checksum",
        record_id_field=CANONICAL_RECORD_ID_FIELD,
    )


def add_quarantine_metadata(
    df: DataFrame,
) -> DataFrame:
    """Attach generic quarantine metadata using the Teaching key."""
    return add_configured_quarantine_metadata(
        df,
        BUSINESS_KEY,
    )


def write_quarantine(df: DataFrame) -> int:
    """Append rejected rows using shared quarantine mechanics."""
    output_columns = (
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
        output_columns=output_columns,
    )


def classify_against_target(
    spark: SparkSession,
    df: DataFrame,
) -> tuple[DataFrame, DataFrame, DataFrame, DataFrame]:
    """Classify Teaching rows without inventing delete semantics."""
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
        delete_field=None,
        source_columns=source_columns,
    )


def merge_into_silver(
    spark: SparkSession,
    df: DataFrame,
) -> int:
    """Use shared MERGE mechanics with no synthetic delete field."""
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
        delete_field=None,
        source_view="teaching_progress_merge_source",
    )


def process_teaching_progress_batch(
    spark: SparkSession,
    batch_id: str,
    merge_to_main: bool = True,
):
    """Process one Teaching demo Bronze batch on an isolated Nessie branch."""
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
            "ctu_ioc_teaching_progress"
        )

        print("=== TEACHING SILVER BATCH START ===")
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

        print("=== TEACHING SILVER CLASSIFICATION ===")
        print(f"BRONZE_COUNT={bronze_count}")
        print(f"DQ_INVALID={dq_invalid_count}")
        print(
            f"SOURCE_CONFLICTS="
            f"{source_conflict_count}"
        )
        print(f"DEDUP={dedup_count}")
        print(f"MERGEABLE={mergeable_count}")
        print(
            f"TARGET_QUARANTINE="
            f"{target_quarantine_count}"
        )
        print(f"STALE={stale_count}")
        print(f"DUPLICATE={duplicate_count}")
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

        print("=== TEACHING BRANCH QUALITY GATE ===")
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
                "TEACHING SILVER QUALITY FAILED: "
                "duplicate business keys"
            )

        if branch_dq_invalid_count != 0:
            raise RuntimeError(
                "TEACHING SILVER QUALITY FAILED: "
                "invalid rows reached Silver"
            )

        if quarantine_delta != quarantined_count:
            raise RuntimeError(
                "TEACHING SILVER QUALITY FAILED: "
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

            print(
                "=== TEACHING MAIN AFTER BRANCH MERGE ==="
            )
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

        print("TEACHING_SILVER_BATCH_PROCESS_PASS")
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


if __name__ == "__main__":
    print("Teaching Progress Silver demo pipeline loaded.")
    print("Contract status: DEMO / ASSUMED")
    print(f"Dataset: {DATASET}")
    print(f"Silver table: {SILVER_TABLE}")
    print(f"Quarantine table: {QUARANTINE_TABLE}")
    print(f"Business key: {BUSINESS_KEY}")
    print(
        f"Delete supported: "
        f"{DATASET_CONFIG.delete_supported}"
    )
    print(f"DQ rules: {len(DQ_RULES)}")
