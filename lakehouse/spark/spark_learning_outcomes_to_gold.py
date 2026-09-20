# -*- coding: utf-8 -*-
"""
Gold transformation for Mock CTU IOC education.learning_outcomes.

Strategy:
    FULL RECOMPUTE from Nessie main Silver
    -> isolated Nessie branch
    -> Iceberg Gold table
    -> quality gate
    -> merge branch to main.

This module is intentionally isolated from the existing PDF/DOCX KPI Gold
pipeline and never performs destructive Nessie metadata repair.
"""

from typing import Dict

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from spark_bronze_to_silver import get_spark_session
from nessie_catalog_utils import (
    make_branch_name,
    create_branch,
    use_branch,
    use_main,
    merge_branch_to_main,
)


SILVER_TABLE = "lakehouse.silver.learning_outcomes"
GOLD_TABLE = "lakehouse.gold.learning_outcomes_metrics"

BUSINESS_KEY = (
    "program_code",
    "academic_year",
    "semester",
)

GOLD_COLUMNS = [
    "program_code",
    "program_name",
    "academic_year",
    "academic_start_year",
    "semester",

    "student_count",

    "passed_course_count",
    "attempted_course_count",
    "course_pass_rate",

    "gpa_point_sum",
    "gpa_student_count",
    "average_gpa",

    "warning_student_count",
    "dropout_risk_student_count",

    "on_track_student_count",
    "progress_evaluated_student_count",
    "on_track_rate",

    "previous_academic_year",
    "previous_semester",

    "previous_course_pass_rate",
    "previous_average_gpa",
    "previous_on_track_rate",

    "course_pass_rate_change_pp",
    "average_gpa_change",
    "on_track_rate_change_pp",

    "source_record_id",
    "source_updated_at",
    "source_batch_id",
    "source_record_checksum",

    "_gold_updated_at",
]


def read_active_silver_main(
    spark: SparkSession,
) -> DataFrame:
    """Read only active CTU IOC Silver records from the current main ref."""
    return (
        spark.table(SILVER_TABLE)
        .filter(F.col("is_deleted") == F.lit(False))
        .localCheckpoint(eager=True)
    )


def transform_learning_outcomes_gold(
    silver_active: DataFrame,
) -> DataFrame:
    """Derive Gold metrics at program/year/semester grain."""

    base = (
        silver_active
        .withColumn(
            "academic_start_year",
            F.regexp_extract(
                F.col("academic_year"),
                r"^(\d{4})-",
                1,
            ).cast("int"),
        )
        .withColumn(
            "course_pass_rate",
            F.when(
                F.col("attempted_course_count") > 0,
                (
                    F.col("passed_course_count").cast("double")
                    * F.lit(100.0)
                    / F.col("attempted_course_count").cast("double")
                ),
            ).otherwise(
                F.lit(None).cast("double")
            ),
        )
        .withColumn(
            "average_gpa",
            F.when(
                F.col("gpa_student_count") > 0,
                (
                    F.col("gpa_point_sum").cast("double")
                    / F.col("gpa_student_count").cast("double")
                ),
            ).otherwise(
                F.lit(None).cast("double")
            ),
        )
        .withColumn(
            "on_track_rate",
            F.when(
                F.col("progress_evaluated_student_count") > 0,
                (
                    F.col("on_track_student_count").cast("double")
                    * F.lit(100.0)
                    / F.col(
                        "progress_evaluated_student_count"
                    ).cast("double")
                ),
            ).otherwise(
                F.lit(None).cast("double")
            ),
        )
    )

    trend_window = (
        Window
        .partitionBy("program_code")
        .orderBy(
            F.col("academic_start_year").asc(),
            F.col("semester").asc(),
        )
    )

    with_previous = (
        base
        .withColumn(
            "previous_academic_year",
            F.lag("academic_year").over(trend_window),
        )
        .withColumn(
            "previous_semester",
            F.lag("semester").over(trend_window),
        )
        .withColumn(
            "previous_course_pass_rate",
            F.lag("course_pass_rate").over(trend_window),
        )
        .withColumn(
            "previous_average_gpa",
            F.lag("average_gpa").over(trend_window),
        )
        .withColumn(
            "previous_on_track_rate",
            F.lag("on_track_rate").over(trend_window),
        )
    )

    result = (
        with_previous
        .withColumn(
            "course_pass_rate_change_pp",
            F.when(
                F.col("course_pass_rate").isNotNull()
                & F.col("previous_course_pass_rate").isNotNull(),
                (
                    F.col("course_pass_rate")
                    - F.col("previous_course_pass_rate")
                ),
            ).otherwise(
                F.lit(None).cast("double")
            ),
        )
        .withColumn(
            "average_gpa_change",
            F.when(
                F.col("average_gpa").isNotNull()
                & F.col("previous_average_gpa").isNotNull(),
                (
                    F.col("average_gpa")
                    - F.col("previous_average_gpa")
                ),
            ).otherwise(
                F.lit(None).cast("double")
            ),
        )
        .withColumn(
            "on_track_rate_change_pp",
            F.when(
                F.col("on_track_rate").isNotNull()
                & F.col("previous_on_track_rate").isNotNull(),
                (
                    F.col("on_track_rate")
                    - F.col("previous_on_track_rate")
                ),
            ).otherwise(
                F.lit(None).cast("double")
            ),
        )
        .withColumn(
            "source_record_id",
            F.col("record_id"),
        )
        .withColumn(
            "source_updated_at",
            F.col("updated_at"),
        )
        .withColumn(
            "source_batch_id",
            F.col("_batch_id"),
        )
        .withColumn(
            "source_record_checksum",
            F.col("_record_checksum"),
        )
        .withColumn(
            "_gold_updated_at",
            F.current_timestamp(),
        )
        .select(*GOLD_COLUMNS)
    )

    return result


def validate_zero_denominator_behavior(
    silver_active: DataFrame,
) -> None:
    """Prove denominator=0 produces NULL rather than zero/NaN/Infinity."""

    probe_source = (
        silver_active
        .limit(1)
        .withColumn(
            "attempted_course_count",
            F.lit(0).cast("long"),
        )
        .withColumn(
            "gpa_student_count",
            F.lit(0).cast("long"),
        )
        .withColumn(
            "progress_evaluated_student_count",
            F.lit(0).cast("long"),
        )
        .localCheckpoint(eager=True)
    )

    probe = (
        transform_learning_outcomes_gold(
            probe_source
        )
        .select(
            "course_pass_rate",
            "average_gpa",
            "on_track_rate",
        )
        .collect()
    )

    if len(probe) != 1:
        raise RuntimeError(
            "ZERO DENOMINATOR TEST FAILED: expected one probe row"
        )

    row = probe[0]

    if row["course_pass_rate"] is not None:
        raise RuntimeError(
            "ZERO DENOMINATOR TEST FAILED: "
            "course_pass_rate must be NULL"
        )

    if row["average_gpa"] is not None:
        raise RuntimeError(
            "ZERO DENOMINATOR TEST FAILED: "
            "average_gpa must be NULL"
        )

    if row["on_track_rate"] is not None:
        raise RuntimeError(
            "ZERO DENOMINATOR TEST FAILED: "
            "on_track_rate must be NULL"
        )

    print("ZERO_DENOMINATOR_PASS")


def write_gold_full_recompute(
    gold_df: DataFrame,
) -> None:
    """Replace the CTU IOC Gold table on the current Nessie branch."""
    (
        gold_df
        .select(*GOLD_COLUMNS)
        .writeTo(GOLD_TABLE)
        .createOrReplace()
    )


def check_gold_quality(
    spark: SparkSession,
    expected_active_rows: int,
) -> Dict[str, int]:
    """Validate derived Gold data on the current Nessie branch."""

    gold = (
        spark.table(GOLD_TABLE)
        .localCheckpoint(eager=True)
    )

    gold_count = gold.count()

    duplicate_key_count = (
        gold
        .groupBy(*BUSINESS_KEY)
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    null_key_count = (
        gold
        .filter(
            F.col("program_code").isNull()
            | F.col("academic_year").isNull()
            | F.col("semester").isNull()
        )
        .count()
    )

    invalid_start_year_count = (
        gold
        .filter(
            F.col("academic_start_year").isNull()
        )
        .count()
    )

    invalid_course_pass_rate_count = (
        gold
        .filter(
            F.col("course_pass_rate").isNotNull()
            & (
                (F.col("course_pass_rate") < 0)
                | (F.col("course_pass_rate") > 100)
                | F.isnan("course_pass_rate")
            )
        )
        .count()
    )

    invalid_on_track_rate_count = (
        gold
        .filter(
            F.col("on_track_rate").isNotNull()
            & (
                (F.col("on_track_rate") < 0)
                | (F.col("on_track_rate") > 100)
                | F.isnan("on_track_rate")
            )
        )
        .count()
    )

    invalid_average_gpa_count = (
        gold
        .filter(
            F.col("average_gpa").isNotNull()
            & (
                (F.col("average_gpa") < 0)
                | F.isnan("average_gpa")
            )
        )
        .count()
    )

    ordering_window = (
        Window
        .partitionBy("program_code")
        .orderBy(
            F.col("academic_start_year").asc(),
            F.col("semester").asc(),
        )
    )

    ordering_check = (
        gold
        .withColumn(
            "_period_row_number",
            F.row_number().over(ordering_window),
        )
        .localCheckpoint(eager=True)
    )

    first_period_invalid = (
        ordering_check
        .filter(
            (F.col("_period_row_number") == 1)
            & (
                F.col("previous_academic_year").isNotNull()
                | F.col("previous_semester").isNotNull()
            )
        )
        .count()
    )

    later_period_missing_previous = (
        ordering_check
        .filter(
            (F.col("_period_row_number") > 1)
            & (
                F.col("previous_academic_year").isNull()
                | F.col("previous_semester").isNull()
            )
        )
        .count()
    )

    print("=== GOLD QUALITY GATE ===")
    print(f"GOLD_ROWS={gold_count}")
    print(
        f"EXPECTED_ACTIVE_SILVER_ROWS="
        f"{expected_active_rows}"
    )
    print(
        f"DUPLICATE_BUSINESS_KEYS="
        f"{duplicate_key_count}"
    )
    print(f"NULL_BUSINESS_KEYS={null_key_count}")
    print(
        f"INVALID_ACADEMIC_START_YEAR="
        f"{invalid_start_year_count}"
    )
    print(
        f"INVALID_COURSE_PASS_RATE="
        f"{invalid_course_pass_rate_count}"
    )
    print(
        f"INVALID_ON_TRACK_RATE="
        f"{invalid_on_track_rate_count}"
    )
    print(
        f"INVALID_AVERAGE_GPA="
        f"{invalid_average_gpa_count}"
    )
    print(
        f"FIRST_PERIOD_INVALID_PREVIOUS="
        f"{first_period_invalid}"
    )
    print(
        f"LATER_PERIOD_MISSING_PREVIOUS="
        f"{later_period_missing_previous}"
    )

    if gold_count != expected_active_rows:
        raise RuntimeError(
            "GOLD QUALITY FAILED: "
            "Gold row count differs from active Silver"
        )

    if duplicate_key_count != 0:
        raise RuntimeError(
            "GOLD QUALITY FAILED: duplicate business key"
        )

    if null_key_count != 0:
        raise RuntimeError(
            "GOLD QUALITY FAILED: null business key"
        )

    if invalid_start_year_count != 0:
        raise RuntimeError(
            "GOLD QUALITY FAILED: "
            "academic_year cannot be parsed"
        )

    if invalid_course_pass_rate_count != 0:
        raise RuntimeError(
            "GOLD QUALITY FAILED: invalid course_pass_rate"
        )

    if invalid_on_track_rate_count != 0:
        raise RuntimeError(
            "GOLD QUALITY FAILED: invalid on_track_rate"
        )

    if invalid_average_gpa_count != 0:
        raise RuntimeError(
            "GOLD QUALITY FAILED: invalid average_gpa"
        )

    if first_period_invalid != 0:
        raise RuntimeError(
            "GOLD QUALITY FAILED: "
            "first period has previous-period key"
        )

    if later_period_missing_previous != 0:
        raise RuntimeError(
            "GOLD QUALITY FAILED: "
            "later period lacks previous-period key"
        )

    print("GOLD_QUALITY_GATE_PASS")

    return {
        "gold_rows": gold_count,
        "duplicate_business_keys": duplicate_key_count,
        "null_business_keys": null_key_count,
        "invalid_academic_start_year": invalid_start_year_count,
        "invalid_course_pass_rate": invalid_course_pass_rate_count,
        "invalid_on_track_rate": invalid_on_track_rate_count,
        "invalid_average_gpa": invalid_average_gpa_count,
        "first_period_invalid_previous": first_period_invalid,
        "later_period_missing_previous": later_period_missing_previous,
    }


def run_learning_outcomes_gold(
    spark: SparkSession,
    merge_to_main: bool = True,
) -> Dict[str, object]:
    """Full-recompute CTU IOC Gold from active Silver main."""

    branch_name = None

    try:
        # Source-of-truth requirement: always snapshot Silver from main.
        use_main(spark)

        silver_all = (
            spark.table(SILVER_TABLE)
            .localCheckpoint(eager=True)
        )

        silver_rows = silver_all.count()

        silver_active = (
            silver_all
            .filter(F.col("is_deleted") == F.lit(False))
            .localCheckpoint(eager=True)
        )

        active_rows = silver_active.count()

        deleted_rows = (
            silver_all
            .filter(F.col("is_deleted") == F.lit(True))
            .count()
        )

        silver_duplicate_keys = (
            silver_active
            .groupBy(*BUSINESS_KEY)
            .count()
            .filter(F.col("count") > 1)
            .count()
        )

        print("=== GOLD SOURCE ===")
        print("NESSIE_REFERENCE=main")
        print(f"SILVER_ROWS={silver_rows}")
        print(f"ACTIVE_SILVER_ROWS={active_rows}")
        print(f"DELETED_SILVER_ROWS={deleted_rows}")
        print(
            f"SILVER_DUPLICATE_BUSINESS_KEYS="
            f"{silver_duplicate_keys}"
        )

        if silver_duplicate_keys != 0:
            raise RuntimeError(
                "REFUSE GOLD: active Silver has duplicate keys"
            )

        if active_rows == 0:
            raise RuntimeError(
                "REFUSE GOLD: no active Silver records"
            )

        validate_zero_denominator_behavior(
            silver_active
        )

        gold_df = (
            transform_learning_outcomes_gold(
                silver_active
            )
            .localCheckpoint(eager=True)
        )

        transformed_rows = gold_df.count()

        print("=== GOLD TRANSFORMATION ===")
        print(
            f"TRANSFORMED_ROWS="
            f"{transformed_rows}"
        )

        if transformed_rows != active_rows:
            raise RuntimeError(
                "GOLD TRANSFORMATION FAILED: "
                "row count changed"
            )

        branch_name = make_branch_name(
            "ctu_ioc_gold_learning_outcomes"
        )

        print(f"BRANCH_NAME={branch_name}")

        create_branch(
            spark,
            branch_name,
            from_ref="main",
        )

        use_branch(
            spark,
            branch_name,
        )

        print("=== WRITE GOLD ON BRANCH ===")

        write_gold_full_recompute(
            gold_df
        )

        spark.catalog.clearCache()

        quality = check_gold_quality(
            spark,
            active_rows,
        )

        print("=== GOLD BRANCH SAMPLE ===")

        (
            spark.table(GOLD_TABLE)
            .select(
                "program_code",
                "program_name",
                "academic_year",
                "semester",
                "student_count",
                "course_pass_rate",
                "average_gpa",
                "on_track_rate",
                "course_pass_rate_change_pp",
                "average_gpa_change",
                "on_track_rate_change_pp",
            )
            .orderBy(
                "program_code",
                "academic_start_year",
                "semester",
            )
            .show(
                5,
                truncate=False,
            )
        )

        print("=== THREE-PERIOD TREND SAMPLE ===")

        (
            spark.table(GOLD_TABLE)
            .filter(
                F.col("program_code")
                == F.lit("DEMO-P001")
            )
            .select(
                "program_code",
                "academic_year",
                "semester",
                "previous_academic_year",
                "previous_semester",
                "course_pass_rate",
                "previous_course_pass_rate",
                "course_pass_rate_change_pp",
                "average_gpa",
                "previous_average_gpa",
                "average_gpa_change",
                "on_track_rate",
                "previous_on_track_rate",
                "on_track_rate_change_pp",
            )
            .orderBy(
                "academic_start_year",
                "semester",
            )
            .show(
                truncate=False,
            )
        )

        if merge_to_main:
            use_main(spark)

            print("=== MERGE GOLD BRANCH TO MAIN ===")

            merge_branch_to_main(
                spark,
                branch_name,
            )

            use_main(spark)
            spark.catalog.clearCache()

            main_gold = spark.table(
                GOLD_TABLE
            )

            main_gold_rows = main_gold.count()

            print("=== GOLD MAIN READ-BACK ===")
            print(
                f"MAIN_GOLD_ROWS="
                f"{main_gold_rows}"
            )

            if main_gold_rows != active_rows:
                raise RuntimeError(
                    "GOLD MAIN READ-BACK FAILED"
                )

            print("GOLD_MAIN_READBACK_PASS")

        else:
            main_gold_rows = None

        result = {
            "strategy": "FULL_RECOMPUTE",
            "silver_rows": silver_rows,
            "active_silver_rows": active_rows,
            "deleted_silver_rows": deleted_rows,
            "transformed_rows": transformed_rows,
            "gold_rows": quality["gold_rows"],
            "duplicate_business_keys":
                quality["duplicate_business_keys"],
            "branch_name": branch_name,
            "main_gold_rows": main_gold_rows,
            "gold_table": GOLD_TABLE,
        }

        print("GOLD_CTU_IOC_PROCESS_PASS")

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


def main():
    spark = get_spark_session()

    try:
        run_learning_outcomes_gold(
            spark,
            merge_to_main=True,
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
