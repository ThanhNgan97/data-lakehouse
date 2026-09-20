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

from api_dataset_registry import get_dataset_config
from env_config import MINIO_BUCKET_NAME
from spark_bronze_to_silver import get_spark_session
from nessie_catalog_utils import (
    create_branch,
    make_branch_name,
    merge_branch_to_main,
    use_branch,
    use_main,
)


DATASET = "education.learning_outcomes"

SILVER_TABLE = "lakehouse.silver.learning_outcomes"
QUARANTINE_TABLE = "lakehouse.silver.learning_outcomes_quarantine"

BUSINESS_KEY = (
    "program_code",
    "academic_year",
    "semester",
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
    """Return explicit nullable input schema for Silver validation.

    Bronze's normal API contract already requires the business fields.
    Silver intentionally reads them as nullable so corrupted/historical
    Bronze observations can be caught by the DQ gate and quarantined
    instead of failing before DQ evaluation.
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


# Minimum demo DQ rules required before a row can compete for Silver.
# These are demo-v0.1 assumptions, not confirmed final CTU IOC rules.
DQ_RULES = (
    ("PROGRAM_CODE_NULL", "program_code IS NULL"),
    ("ACADEMIC_YEAR_NULL", "academic_year IS NULL"),
    ("SEMESTER_NULL", "semester IS NULL"),
    ("SEMESTER_NON_POSITIVE", "semester <= 0"),
    ("STUDENT_COUNT_NEGATIVE", "student_count IS NULL OR student_count < 0"),
    ("PASSED_COURSE_COUNT_NEGATIVE", "passed_course_count IS NULL OR passed_course_count < 0"),
    ("ATTEMPTED_COURSE_COUNT_NEGATIVE", "attempted_course_count IS NULL OR attempted_course_count < 0"),
    (
        "PASSED_EXCEEDS_ATTEMPTED",
        "passed_course_count > attempted_course_count",
    ),
    ("GPA_STUDENT_COUNT_NEGATIVE", "gpa_student_count IS NULL OR gpa_student_count < 0"),
    (
        "GPA_STUDENT_COUNT_EXCEEDS_STUDENT_COUNT",
        "gpa_student_count > student_count",
    ),
    ("WARNING_STUDENT_COUNT_NEGATIVE", "warning_student_count IS NULL OR warning_student_count < 0"),
    (
        "WARNING_STUDENT_COUNT_EXCEEDS_STUDENT_COUNT",
        "warning_student_count > student_count",
    ),
    (
        "DROPOUT_RISK_STUDENT_COUNT_NEGATIVE",
        "dropout_risk_student_count IS NULL OR dropout_risk_student_count < 0",
    ),
    (
        "DROPOUT_RISK_EXCEEDS_STUDENT_COUNT",
        "dropout_risk_student_count > student_count",
    ),
    ("ON_TRACK_STUDENT_COUNT_NEGATIVE", "on_track_student_count IS NULL OR on_track_student_count < 0"),
    (
        "PROGRESS_EVALUATED_STUDENT_COUNT_NEGATIVE",
        "progress_evaluated_student_count IS NULL OR progress_evaluated_student_count < 0",
    ),
    (
        "ON_TRACK_EXCEEDS_PROGRESS_EVALUATED",
        "on_track_student_count > progress_evaluated_student_count",
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
            record_id STRING,
            program_code STRING,
            program_name STRING,
            academic_year STRING,
            semester INT,
            student_count BIGINT,
            passed_course_count BIGINT,
            attempted_course_count BIGINT,
            gpa_point_sum DOUBLE,
            gpa_student_count BIGINT,
            warning_student_count BIGINT,
            dropout_risk_student_count BIGINT,
            on_track_student_count BIGINT,
            progress_evaluated_student_count BIGINT,
            updated_at TIMESTAMP,
            is_deleted BOOLEAN,

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
            record_id STRING,
            program_code STRING,
            program_name STRING,
            academic_year STRING,
            semester INT,
            student_count BIGINT,
            passed_course_count BIGINT,
            attempted_course_count BIGINT,
            gpa_point_sum DOUBLE,
            gpa_student_count BIGINT,
            warning_student_count BIGINT,
            dropout_risk_student_count BIGINT,
            on_track_student_count BIGINT,
            progress_evaluated_student_count BIGINT,
            updated_at TIMESTAMP,
            is_deleted BOOLEAN,

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
    """Attach deterministic reason codes for every failed demo DQ rule."""
    reason_columns = [
        F.when(F.expr(invalid_expression), F.lit(reason_code))
        for reason_code, invalid_expression in DQ_RULES
    ]

    return df.withColumn(
        "_dq_reasons",
        F.array_compact(F.array(*reason_columns)),
    )


def split_dq(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Split Bronze observations into DQ-valid and quarantinable rows."""
    evaluated = add_dq_reasons(df)

    valid = (
        evaluated
        .filter(F.size(F.col("_dq_reasons")) == 0)
        .drop("_dq_reasons")
    )

    invalid = (
        evaluated
        .filter(F.size(F.col("_dq_reasons")) > 0)
        .withColumn(
            "rejection_reason",
            F.concat_ws("|", F.col("_dq_reasons")),
        )
        .drop("_dq_reasons")
    )

    return valid, invalid


def split_equal_timestamp_conflicts(
    df: DataFrame,
) -> tuple[DataFrame, DataFrame]:
    """Block keys that contain equal-timestamp competing source content.

    If the same logical key and updated_at carry different checksums,
    Silver must not silently choose a winner. The entire key is blocked
    from this merge attempt and retained for conflict quarantine.
    """
    checksum_value = F.coalesce(
        F.col("_record_checksum"),
        F.lit("__NULL_CHECKSUM__"),
    )

    conflict_groups = (
        df.groupBy(*BUSINESS_KEY, "updated_at")
        .agg(
            F.countDistinct(checksum_value).alias("_checksum_variants")
        )
        .filter(F.col("_checksum_variants") > 1)
        .select(*BUSINESS_KEY, "updated_at")
    )

    conflict_keys = conflict_groups.select(*BUSINESS_KEY).distinct()

    conflict_rows = (
        df.join(
            conflict_keys,
            on=list(BUSINESS_KEY),
            how="inner",
        )
        .withColumn(
            "rejection_reason",
            F.lit("EQUAL_TIMESTAMP_DIFFERENT_CHECKSUM"),
        )
    )

    mergeable = df.join(
        conflict_keys,
        on=list(BUSINESS_KEY),
        how="left_anti",
    )

    return mergeable, conflict_rows


def deterministic_deduplicate(df: DataFrame) -> DataFrame:
    """Return one deterministic source row per logical business key.

    Priority:
      1. newest updated_at
      2. newest _ingested_at
      3. deterministic batch/checksum/record tie-breakers
    """
    ordering = Window.partitionBy(*BUSINESS_KEY).orderBy(
        F.col("updated_at").desc_nulls_last(),
        F.col("_ingested_at").desc_nulls_last(),
        F.col("_batch_id").desc_nulls_last(),
        F.col("_record_checksum").asc_nulls_last(),
        F.col("record_id").asc_nulls_last(),
    )

    return (
        df.withColumn("_silver_row_number", F.row_number().over(ordering))
        .filter(F.col("_silver_row_number") == 1)
        .drop("_silver_row_number")
    )


def add_quarantine_metadata(df: DataFrame) -> DataFrame:
    """Attach deterministic business-key text and quarantine timestamp."""
    business_key = F.concat_ws(
        "|",
        F.coalesce(F.col("program_code"), F.lit("<NULL>")),
        F.coalesce(F.col("academic_year"), F.lit("<NULL>")),
        F.coalesce(
            F.col("semester").cast("string"),
            F.lit("<NULL>"),
        ),
    )

    return (
        df.withColumn("_business_key", business_key)
        .withColumn("rejected_at", F.current_timestamp())
    )


def write_quarantine(df: DataFrame) -> int:
    """Append rejected source observations to the Iceberg quarantine table."""
    columns = (
        list(get_dataset_config(DATASET).source_fields)
        + list(BRONZE_METADATA_FIELDS)
        + [
            "_business_key",
            "rejection_reason",
            "rejected_at",
        ]
    )

    prepared = add_quarantine_metadata(df).select(*columns)
    row_count = prepared.count()

    if row_count == 0:
        return 0

    prepared.writeTo(QUARANTINE_TABLE).append()
    return row_count


def classify_against_target(
    spark: SparkSession,
    df: DataFrame,
) -> tuple[DataFrame, DataFrame, DataFrame, DataFrame]:
    """Compare deduplicated source rows with current Silver state.

    Returns:
        mergeable,
        quarantine,
        stale,
        duplicate
    """
    target = spark.table(SILVER_TABLE).select(
        F.col("program_code").alias("_target_program_code"),
        F.col("academic_year").alias("_target_academic_year"),
        F.col("semester").alias("_target_semester"),
        F.col("updated_at").alias("_target_updated_at"),
        F.col("_record_checksum").alias("_target_record_checksum"),
    )

    source = df.alias("s")
    target = target.alias("t")

    join_condition = (
        (F.col("s.program_code") == F.col("t._target_program_code"))
        & (F.col("s.academic_year") == F.col("t._target_academic_year"))
        & (F.col("s.semester") == F.col("t._target_semester"))
    )

    joined = source.join(
        target,
        join_condition,
        "left",
    )

    # Spark 3.5 + Iceberg/DataSource V2 can hit an optimizer assertion
    # when downstream classification filters are pushed through this join.
    # Materializing locally cuts the V2 scan lineage before those filters.
    joined = joined.localCheckpoint(eager=True)

    source_columns = (
        list(get_dataset_config(DATASET).source_fields)
        + list(BRONZE_METADATA_FIELDS)
    )

    target_exists = F.col("_target_program_code").isNotNull()

    source_updated_at = F.col("updated_at")
    target_updated_at = F.col("_target_updated_at")

    source_checksum = F.coalesce(
        F.col("_record_checksum"),
        F.lit("__NULL_CHECKSUM__"),
    )
    target_checksum = F.coalesce(
        F.col("_target_record_checksum"),
        F.lit("__NULL_CHECKSUM__"),
    )

    same_checksum = source_checksum == target_checksum
    different_checksum = source_checksum != target_checksum

    source_is_deleted = F.coalesce(
        F.col("is_deleted"),
        F.lit(False),
    )

    delete_without_target_condition = (
        (~target_exists)
        & source_is_deleted
    )

    equal_timestamp_conflict_condition = (
        target_exists
        & (source_updated_at == target_updated_at)
        & different_checksum
    )

    stale_condition = (
        target_exists
        & (source_updated_at < target_updated_at)
    )

    duplicate_condition = (
        target_exists
        & (source_updated_at == target_updated_at)
        & same_checksum
    )

    mergeable_condition = (
        (
            (~target_exists)
            & (~source_is_deleted)
        )
        |
        (
            target_exists
            & (source_updated_at > target_updated_at)
        )
    )

    mergeable = (
        joined
        .filter(mergeable_condition)
        .select(*source_columns)
    )

    delete_without_target = (
        joined
        .filter(delete_without_target_condition)
        .select(*source_columns)
        .withColumn(
            "rejection_reason",
            F.lit("DELETE_WITHOUT_EXISTING_TARGET"),
        )
    )

    equal_timestamp_conflicts = (
        joined
        .filter(equal_timestamp_conflict_condition)
        .select(*source_columns)
        .withColumn(
            "rejection_reason",
            F.lit("EQUAL_TIMESTAMP_DIFFERENT_CHECKSUM"),
        )
    )

    quarantine = delete_without_target.unionByName(
        equal_timestamp_conflicts
    )

    stale = (
        joined
        .filter(stale_condition)
        .select(*source_columns)
    )

    duplicate = (
        joined
        .filter(duplicate_condition)
        .select(*source_columns)
    )

    return mergeable, quarantine, stale, duplicate


def merge_into_silver(
    spark: SparkSession,
    df: DataFrame,
) -> int:
    """MERGE only insertable/newer observations into Silver Iceberg."""
    row_count = df.count()

    if row_count == 0:
        return 0

    source_columns = (
        list(get_dataset_config(DATASET).source_fields)
        + list(BRONZE_METADATA_FIELDS)
    )

    merge_source = (
        df.withColumn(
            "_silver_updated_at",
            F.current_timestamp(),
        )
        .select(
            *source_columns,
            "_silver_updated_at",
        )
    )

    view_name = "learning_outcomes_merge_source"
    merge_source.createOrReplaceTempView(view_name)

    update_assignments = ",\n".join(
        f"t.{column} = s.{column}"
        for column in source_columns + ["_silver_updated_at"]
    )

    insert_columns = source_columns + ["_silver_updated_at"]

    insert_column_sql = ", ".join(insert_columns)
    insert_value_sql = ", ".join(
        f"s.{column}"
        for column in insert_columns
    )

    spark.sql(f"""
        MERGE INTO {SILVER_TABLE} t
        USING {view_name} s
        ON {business_key_sql("s", "t")}

        WHEN MATCHED
          AND s.updated_at > t.updated_at
        THEN UPDATE SET
          {update_assignments}

        WHEN NOT MATCHED
          AND s.is_deleted = false
        THEN INSERT (
          {insert_column_sql}
        )
        VALUES (
          {insert_value_sql}
        )
    """)

    return row_count


def business_key_sql(
    source_alias: str = "s",
    target_alias: str = "t",
) -> str:
    """Return the exact logical business-key condition."""
    return " AND ".join(
        f"{target_alias}.{column} = {source_alias}.{column}"
        for column in BUSINESS_KEY
    )
    """Return the exact logical-key SQL condition for MERGE."""
    return " AND ".join(
        f"{alias}.{column} = target.{column}"
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

        bronze = read_bronze_batch(
            spark,
            batch_id,
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
