# -*- coding: utf-8 -*-
"""Teaching Progress business Gold for the Day 7 demo contract.

IMPORTANT:
- ``education.teaching_progress`` is still a DEMO / ASSUMED source contract.
- This module must not be interpreted as a verified CTU IOC production model.
- Gold remains at one course section x academic year x semester.
- ``ty_le_tien_do_giang_day`` is SOURCE-PROVIDED; Gold does not recalculate it.
- No on-schedule/behind-schedule status, weighted progress, completion counts,
  Learning Outcomes KPI, or period-over-period trend is implemented.
"""

from __future__ import annotations

from typing import Dict

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from spark_bronze_to_silver import get_spark_session
from nessie_catalog_utils import (
    create_branch,
    make_branch_name,
    merge_branch_to_main,
    use_branch,
    use_main,
)


SILVER_TABLE = "lakehouse.silver.teaching_progress"
GOLD_TABLE = "lakehouse.gold.teaching_progress_metrics"

BUSINESS_KEY = (
    "ma_lop_hoc_phan",
    "nam_hoc",
    "hoc_ky",
)

GOLD_COLUMNS = [
    "ma_don_vi",
    "ten_don_vi",
    "ma_lop_hoc_phan",
    "nam_hoc",
    "hoc_ky",
    "ty_le_tien_do_giang_day",
    "so_ban_ghi_theo_doi",
    "source_record_id",
    "source_updated_at",
    "source_batch_id",
    "source_record_checksum",
    "_gold_updated_at",
]


def read_teaching_silver_main(
    spark: SparkSession,
) -> DataFrame:
    """Read the canonical Teaching Silver source of truth from Nessie main."""
    use_main(spark)
    return (
        spark.table(SILVER_TABLE)
        .localCheckpoint(eager=True)
    )


def transform_teaching_progress_gold(
    silver: DataFrame,
) -> DataFrame:
    """Build row-preserving Teaching Gold at the frozen Day 7 grain.

    ``ty_le_tien_do_giang_day`` is carried directly from Silver because the
    source already provides this aggregate percentage.

    ``so_ban_ghi_theo_doi`` is the only Gold-derived business measure:
    literal 1 per valid Gold-grain row, so serving can count monitored
    course-section-period records without inventing a hidden formula.
    """
    return (
        silver
        .select(
            F.col("ma_don_vi"),
            F.col("ten_don_vi"),
            F.col("ma_lop_hoc_phan"),
            F.col("nam_hoc"),
            F.col("hoc_ky"),
            F.col("ty_le_tien_do_giang_day"),
            F.lit(1).cast("long").alias(
                "so_ban_ghi_theo_doi"
            ),
            F.col("ma_ban_ghi").alias(
                "source_record_id"
            ),
            F.col(
                "thoi_gian_cap_nhat_nguon"
            ).alias(
                "source_updated_at"
            ),
            F.col("_batch_id").alias(
                "source_batch_id"
            ),
            F.col("_record_checksum").alias(
                "source_record_checksum"
            ),
            F.current_timestamp().alias(
                "_gold_updated_at"
            ),
        )
    )


def write_gold_full_recompute(
    gold_df: DataFrame,
) -> None:
    """Replace Teaching Gold on the current Nessie branch."""
    (
        gold_df
        .select(*GOLD_COLUMNS)
        .writeTo(GOLD_TABLE)
        .createOrReplace()
    )


def _quality_counts(
    spark: SparkSession,
    expected_silver_rows: int,
) -> Dict[str, int]:
    """Collect Teaching-specific Gold reconciliation counters."""
    gold = (
        spark.table(GOLD_TABLE)
        .localCheckpoint(eager=True)
    )

    silver = (
        spark.table(SILVER_TABLE)
        .localCheckpoint(eager=True)
    )

    gold_rows = gold.count()

    distinct_gold_keys = (
        gold
        .select(*BUSINESS_KEY)
        .distinct()
        .count()
    )

    duplicate_gold_keys = (
        gold
        .groupBy(*BUSINESS_KEY)
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    null_keys = (
        gold
        .filter(
            F.col("ma_lop_hoc_phan").isNull()
            | F.col("nam_hoc").isNull()
            | F.col("hoc_ky").isNull()
        )
        .count()
    )

    invalid_progress = (
        gold
        .filter(
            F.col(
                "ty_le_tien_do_giang_day"
            ).isNull()
            | F.isnan(
                "ty_le_tien_do_giang_day"
            )
            | (
                F.col(
                    "ty_le_tien_do_giang_day"
                ) < 0
            )
            | (
                F.col(
                    "ty_le_tien_do_giang_day"
                ) > 100
            )
        )
        .count()
    )

    invalid_monitor_count = (
        gold
        .filter(
            F.col("so_ban_ghi_theo_doi").isNull()
            | (
                F.col(
                    "so_ban_ghi_theo_doi"
                ) != 1
            )
        )
        .count()
    )

    null_lineage = (
        gold
        .filter(
            F.col("source_record_id").isNull()
            | F.col("source_updated_at").isNull()
            | F.col("source_batch_id").isNull()
            | F.col(
                "source_record_checksum"
            ).isNull()
        )
        .count()
    )

    lineage_mismatch = (
        gold.alias("g")
        .join(
            silver.alias("s"),
            F.col("g.source_record_id")
            == F.col("s.ma_ban_ghi"),
            "left",
        )
        .filter(
            F.col("s.ma_ban_ghi").isNull()
            | (
                F.col("g.source_updated_at")
                != F.col(
                    "s.thoi_gian_cap_nhat_nguon"
                )
            )
            | (
                F.col("g.source_batch_id")
                != F.col("s._batch_id")
            )
            | (
                F.col(
                    "g.source_record_checksum"
                )
                != F.col(
                    "s._record_checksum"
                )
            )
        )
        .count()
    )

    return {
        "expected_silver_rows": expected_silver_rows,
        "gold_rows": gold_rows,
        "distinct_gold_keys": distinct_gold_keys,
        "duplicate_gold_keys": duplicate_gold_keys,
        "null_keys": null_keys,
        "invalid_progress": invalid_progress,
        "invalid_monitor_count": invalid_monitor_count,
        "null_lineage": null_lineage,
        "lineage_mismatch": lineage_mismatch,
    }


def check_gold_quality(
    spark: SparkSession,
    expected_silver_rows: int,
) -> Dict[str, int]:
    """Enforce the frozen Teaching-specific Gold quality contract."""
    quality = _quality_counts(
        spark,
        expected_silver_rows,
    )

    print("=== TEACHING GOLD QUALITY ===")
    for key, value in quality.items():
        print(f"{key.upper()}={value}")

    violations = {
        "ROW_RECONCILIATION": (
            quality["gold_rows"]
            != expected_silver_rows
        ),
        "DISTINCT_KEY_RECONCILIATION": (
            quality["distinct_gold_keys"]
            != expected_silver_rows
        ),
        "DUPLICATE_GOLD_KEYS": (
            quality["duplicate_gold_keys"]
            != 0
        ),
        "NULL_KEYS": (
            quality["null_keys"]
            != 0
        ),
        "INVALID_PROGRESS": (
            quality["invalid_progress"]
            != 0
        ),
        "INVALID_MONITOR_COUNT": (
            quality["invalid_monitor_count"]
            != 0
        ),
        "NULL_LINEAGE": (
            quality["null_lineage"]
            != 0
        ),
        "LINEAGE_MISMATCH": (
            quality["lineage_mismatch"]
            != 0
        ),
    }

    failed = [
        name
        for name, is_failed in violations.items()
        if is_failed
    ]

    if failed:
        raise RuntimeError(
            "TEACHING GOLD QUALITY FAILED: "
            + ", ".join(failed)
        )

    print("TEACHING_GOLD_QUALITY_PASS")
    return quality


def run_teaching_progress_gold(
    spark: SparkSession,
    merge_to_main: bool = True,
) -> Dict[str, object]:
    """Full-recompute Teaching Gold using the existing Nessie safe pattern."""
    branch_name = None

    try:
        use_main(spark)

        silver = (
            spark.table(SILVER_TABLE)
            .localCheckpoint(eager=True)
        )

        silver_rows = silver.count()

        silver_duplicate_keys = (
            silver
            .groupBy(*BUSINESS_KEY)
            .count()
            .filter(F.col("count") > 1)
            .count()
        )

        print("=== TEACHING GOLD SOURCE ===")
        print("NESSIE_REFERENCE=main")
        print(f"ACTIVE_SILVER_ROWS={silver_rows}")
        print(
            "SILVER_DUPLICATE_BUSINESS_KEYS="
            f"{silver_duplicate_keys}"
        )

        if silver_rows == 0:
            raise RuntimeError(
                "REFUSE TEACHING GOLD: no Silver records"
            )

        if silver_duplicate_keys != 0:
            raise RuntimeError(
                "REFUSE TEACHING GOLD: "
                "Silver has duplicate business keys"
            )

        gold_df = (
            transform_teaching_progress_gold(
                silver
            )
            .localCheckpoint(eager=True)
        )

        transformed_rows = gold_df.count()

        print("=== TEACHING GOLD TRANSFORMATION ===")
        print(
            f"TRANSFORMED_ROWS={transformed_rows}"
        )

        if transformed_rows != silver_rows:
            raise RuntimeError(
                "TEACHING GOLD TRANSFORMATION FAILED: "
                "row count changed"
            )

        branch_name = make_branch_name(
            "ctu_ioc_gold_teaching_progress"
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

        print("=== WRITE TEACHING GOLD ON BRANCH ===")
        write_gold_full_recompute(
            gold_df
        )

        spark.catalog.clearCache()

        quality = check_gold_quality(
            spark,
            silver_rows,
        )

        branch_gold_rows = quality[
            "gold_rows"
        ]

        if merge_to_main:
            use_main(spark)

            print(
                "=== MERGE TEACHING GOLD BRANCH TO MAIN ==="
            )

            merge_branch_to_main(
                spark,
                branch_name,
            )

            use_main(spark)
            spark.catalog.clearCache()

            main_gold = spark.table(
                GOLD_TABLE
            )

            main_gold_rows = (
                main_gold.count()
            )

            main_distinct_keys = (
                main_gold
                .select(*BUSINESS_KEY)
                .distinct()
                .count()
            )

            print(
                "=== TEACHING GOLD MAIN READ-BACK ==="
            )
            print(
                f"MAIN_GOLD_ROWS="
                f"{main_gold_rows}"
            )
            print(
                f"MAIN_DISTINCT_GOLD_KEYS="
                f"{main_distinct_keys}"
            )

            if (
                main_gold_rows
                != silver_rows
            ):
                raise RuntimeError(
                    "TEACHING GOLD MAIN READ-BACK "
                    "FAILED: row count"
                )

            if (
                main_distinct_keys
                != silver_rows
            ):
                raise RuntimeError(
                    "TEACHING GOLD MAIN READ-BACK "
                    "FAILED: key count"
                )

            print(
                "TEACHING_GOLD_MAIN_READBACK_PASS"
            )
        else:
            main_gold_rows = None
            main_distinct_keys = None

        result = {
            "strategy": "FULL_RECOMPUTE",
            "active_silver_rows": silver_rows,
            "transformed_rows": transformed_rows,
            "gold_rows": branch_gold_rows,
            "distinct_gold_keys": quality[
                "distinct_gold_keys"
            ],
            "duplicate_gold_keys": quality[
                "duplicate_gold_keys"
            ],
            "null_keys": quality[
                "null_keys"
            ],
            "dq_violations": (
                quality["invalid_progress"]
                + quality[
                    "invalid_monitor_count"
                ]
                + quality["null_lineage"]
            ),
            "lineage_mismatch": quality[
                "lineage_mismatch"
            ],
            "branch_name": branch_name,
            "main_gold_rows": main_gold_rows,
            "main_distinct_gold_keys": (
                main_distinct_keys
            ),
            "gold_table": GOLD_TABLE,
            "trend": "NOT IMPLEMENTED",
        }

        print(
            "TEACHING_GOLD_PROCESS_PASS"
        )
        return result

    finally:
        try:
            use_main(spark)
        except Exception:
            pass

        if branch_name:
            print(
                "BRANCH_LEFT_FOR_AUDIT="
                f"{branch_name}"
            )


def main() -> None:
    spark = get_spark_session()

    try:
        run_teaching_progress_gold(
            spark,
            merge_to_main=True,
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
