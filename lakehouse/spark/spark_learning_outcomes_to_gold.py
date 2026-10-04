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
    "ma_chuong_trinh",
    "nam_hoc",
    "hoc_ky",
)

GOLD_COLUMNS = [
    "ma_chuong_trinh",
    "ten_chuong_trinh",
    "nam_hoc",
    "nam_bat_dau",
    "hoc_ky",

    "so_sinh_vien",

    "so_luot_hoc_phan_dat",
    "tong_luot_hoc_phan",
    "ty_le_qua_hoc_phan",

    "tong_diem_gpa",
    "so_sinh_vien_tinh_gpa",
    "gpa_trung_binh",

    "so_sinh_vien_canh_bao",
    "so_sinh_vien_nguy_co_nghi_hoc",

    "so_sinh_vien_dung_tien_do",
    "so_sinh_vien_danh_gia_tien_do",
    "ty_le_dung_tien_do",

    "nam_hoc_truoc",
    "hoc_ky_truoc",

    "ty_le_qua_hoc_phan_truoc",
    "gpa_trung_binh_truoc",
    "ty_le_dung_tien_do_truoc",

    "chenh_lech_ty_le_qua_hoc_phan_pp",
    "chenh_lech_gpa_trung_binh",
    "chenh_lech_ty_le_dung_tien_do_pp",

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
        .filter(F.col("da_xoa") == F.lit(False))
        .localCheckpoint(eager=True)
    )


def transform_learning_outcomes_gold(
    silver_active: DataFrame,
) -> DataFrame:
    """Derive Gold metrics at program/year/hoc_ky grain."""

    base = (
        silver_active
        .withColumn(
            "nam_bat_dau",
            F.regexp_extract(
                F.col("nam_hoc"),
                r"^(\d{4})-",
                1,
            ).cast("int"),
        )
        .withColumn(
            "ty_le_qua_hoc_phan",
            F.when(
                F.col("tong_luot_hoc_phan") > 0,
                (
                    F.col("so_luot_hoc_phan_dat").cast("double")
                    * F.lit(100.0)
                    / F.col("tong_luot_hoc_phan").cast("double")
                ),
            ).otherwise(
                F.lit(None).cast("double")
            ),
        )
        .withColumn(
            "gpa_trung_binh",
            F.when(
                F.col("so_sinh_vien_tinh_gpa") > 0,
                (
                    F.col("tong_diem_gpa").cast("double")
                    / F.col("so_sinh_vien_tinh_gpa").cast("double")
                ),
            ).otherwise(
                F.lit(None).cast("double")
            ),
        )
        .withColumn(
            "ty_le_dung_tien_do",
            F.when(
                F.col("so_sinh_vien_danh_gia_tien_do") > 0,
                (
                    F.col("so_sinh_vien_dung_tien_do").cast("double")
                    * F.lit(100.0)
                    / F.col(
                        "so_sinh_vien_danh_gia_tien_do"
                    ).cast("double")
                ),
            ).otherwise(
                F.lit(None).cast("double")
            ),
        )
    )

    trend_window = (
        Window
        .partitionBy("ma_chuong_trinh")
        .orderBy(
            F.col("nam_bat_dau").asc(),
            F.col("hoc_ky").asc(),
        )
    )

    with_previous = (
        base
        .withColumn(
            "nam_hoc_truoc",
            F.lag("nam_hoc").over(trend_window),
        )
        .withColumn(
            "hoc_ky_truoc",
            F.lag("hoc_ky").over(trend_window),
        )
        .withColumn(
            "ty_le_qua_hoc_phan_truoc",
            F.lag("ty_le_qua_hoc_phan").over(trend_window),
        )
        .withColumn(
            "gpa_trung_binh_truoc",
            F.lag("gpa_trung_binh").over(trend_window),
        )
        .withColumn(
            "ty_le_dung_tien_do_truoc",
            F.lag("ty_le_dung_tien_do").over(trend_window),
        )
    )

    result = (
        with_previous
        .withColumn(
            "chenh_lech_ty_le_qua_hoc_phan_pp",
            F.when(
                F.col("ty_le_qua_hoc_phan").isNotNull()
                & F.col("ty_le_qua_hoc_phan_truoc").isNotNull(),
                (
                    F.col("ty_le_qua_hoc_phan")
                    - F.col("ty_le_qua_hoc_phan_truoc")
                ),
            ).otherwise(
                F.lit(None).cast("double")
            ),
        )
        .withColumn(
            "chenh_lech_gpa_trung_binh",
            F.when(
                F.col("gpa_trung_binh").isNotNull()
                & F.col("gpa_trung_binh_truoc").isNotNull(),
                (
                    F.col("gpa_trung_binh")
                    - F.col("gpa_trung_binh_truoc")
                ),
            ).otherwise(
                F.lit(None).cast("double")
            ),
        )
        .withColumn(
            "chenh_lech_ty_le_dung_tien_do_pp",
            F.when(
                F.col("ty_le_dung_tien_do").isNotNull()
                & F.col("ty_le_dung_tien_do_truoc").isNotNull(),
                (
                    F.col("ty_le_dung_tien_do")
                    - F.col("ty_le_dung_tien_do_truoc")
                ),
            ).otherwise(
                F.lit(None).cast("double")
            ),
        )
        .withColumn(
            "source_record_id",
            F.col("ma_ban_ghi"),
        )
        .withColumn(
            "source_updated_at",
            F.col("thoi_gian_cap_nhat_nguon"),
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
            "tong_luot_hoc_phan",
            F.lit(0).cast("long"),
        )
        .withColumn(
            "so_sinh_vien_tinh_gpa",
            F.lit(0).cast("long"),
        )
        .withColumn(
            "so_sinh_vien_danh_gia_tien_do",
            F.lit(0).cast("long"),
        )
        .localCheckpoint(eager=True)
    )

    probe = (
        transform_learning_outcomes_gold(
            probe_source
        )
        .select(
            "ty_le_qua_hoc_phan",
            "gpa_trung_binh",
            "ty_le_dung_tien_do",
        )
        .collect()
    )

    if len(probe) != 1:
        raise RuntimeError(
            "ZERO DENOMINATOR TEST FAILED: expected one probe row"
        )

    row = probe[0]

    if row["ty_le_qua_hoc_phan"] is not None:
        raise RuntimeError(
            "ZERO DENOMINATOR TEST FAILED: "
            "ty_le_qua_hoc_phan must be NULL"
        )

    if row["gpa_trung_binh"] is not None:
        raise RuntimeError(
            "ZERO DENOMINATOR TEST FAILED: "
            "gpa_trung_binh must be NULL"
        )

    if row["ty_le_dung_tien_do"] is not None:
        raise RuntimeError(
            "ZERO DENOMINATOR TEST FAILED: "
            "ty_le_dung_tien_do must be NULL"
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
            F.col("ma_chuong_trinh").isNull()
            | F.col("nam_hoc").isNull()
            | F.col("hoc_ky").isNull()
        )
        .count()
    )

    invalid_start_year_count = (
        gold
        .filter(
            F.col("nam_bat_dau").isNull()
        )
        .count()
    )

    invalid_course_pass_rate_count = (
        gold
        .filter(
            F.col("ty_le_qua_hoc_phan").isNotNull()
            & (
                (F.col("ty_le_qua_hoc_phan") < 0)
                | (F.col("ty_le_qua_hoc_phan") > 100)
                | F.isnan("ty_le_qua_hoc_phan")
            )
        )
        .count()
    )

    invalid_on_track_rate_count = (
        gold
        .filter(
            F.col("ty_le_dung_tien_do").isNotNull()
            & (
                (F.col("ty_le_dung_tien_do") < 0)
                | (F.col("ty_le_dung_tien_do") > 100)
                | F.isnan("ty_le_dung_tien_do")
            )
        )
        .count()
    )

    invalid_average_gpa_count = (
        gold
        .filter(
            F.col("gpa_trung_binh").isNotNull()
            & (
                (F.col("gpa_trung_binh") < 0)
                | F.isnan("gpa_trung_binh")
            )
        )
        .count()
    )

    ordering_window = (
        Window
        .partitionBy("ma_chuong_trinh")
        .orderBy(
            F.col("nam_bat_dau").asc(),
            F.col("hoc_ky").asc(),
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
                F.col("nam_hoc_truoc").isNotNull()
                | F.col("hoc_ky_truoc").isNotNull()
            )
        )
        .count()
    )

    later_period_missing_previous = (
        ordering_check
        .filter(
            (F.col("_period_row_number") > 1)
            & (
                F.col("nam_hoc_truoc").isNull()
                | F.col("hoc_ky_truoc").isNull()
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
            "nam_hoc cannot be parsed"
        )

    if invalid_course_pass_rate_count != 0:
        raise RuntimeError(
            "GOLD QUALITY FAILED: invalid ty_le_qua_hoc_phan"
        )

    if invalid_on_track_rate_count != 0:
        raise RuntimeError(
            "GOLD QUALITY FAILED: invalid ty_le_dung_tien_do"
        )

    if invalid_average_gpa_count != 0:
        raise RuntimeError(
            "GOLD QUALITY FAILED: invalid gpa_trung_binh"
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
            .filter(F.col("da_xoa") == F.lit(False))
            .localCheckpoint(eager=True)
        )

        active_rows = silver_active.count()

        deleted_rows = (
            silver_all
            .filter(F.col("da_xoa") == F.lit(True))
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

        # A fresh Nessie catalog has no Gold namespace yet.
        # Create it on the isolated dataset branch before the first Gold table write.
        spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.gold")
        print("GOLD_NAMESPACE_READY=lakehouse.gold")

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
                "ma_chuong_trinh",
                "ten_chuong_trinh",
                "nam_hoc",
                "hoc_ky",
                "so_sinh_vien",
                "ty_le_qua_hoc_phan",
                "gpa_trung_binh",
                "ty_le_dung_tien_do",
                "chenh_lech_ty_le_qua_hoc_phan_pp",
                "chenh_lech_gpa_trung_binh",
                "chenh_lech_ty_le_dung_tien_do_pp",
            )
            .orderBy(
                "ma_chuong_trinh",
                "nam_bat_dau",
                "hoc_ky",
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
                F.col("ma_chuong_trinh")
                == F.lit("DEMO-P001")
            )
            .select(
                "ma_chuong_trinh",
                "nam_hoc",
                "hoc_ky",
                "nam_hoc_truoc",
                "hoc_ky_truoc",
                "ty_le_qua_hoc_phan",
                "ty_le_qua_hoc_phan_truoc",
                "chenh_lech_ty_le_qua_hoc_phan_pp",
                "gpa_trung_binh",
                "gpa_trung_binh_truoc",
                "chenh_lech_gpa_trung_binh",
                "ty_le_dung_tien_do",
                "ty_le_dung_tien_do_truoc",
                "chenh_lech_ty_le_dung_tien_do_pp",
            )
            .orderBy(
                "nam_bat_dau",
                "hoc_ky",
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
