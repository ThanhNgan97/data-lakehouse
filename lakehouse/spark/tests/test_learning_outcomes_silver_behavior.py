# -*- coding: utf-8 -*-
"""Day 4 behavior-freeze tests for CTU IOC Learning Outcomes Silver.

These tests intentionally freeze CURRENT behavior before Day 5 extracts any
generic Silver mechanics. They do not propose or "improve" semantics.
Run inside the Airflow/Spark runtime where PySpark is already available.
"""

from __future__ import annotations

import inspect
import unittest
from datetime import datetime

from pyspark.sql import SparkSession
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

import spark_learning_outcomes_to_silver as silver


CANONICAL_SCHEMA = StructType([
    StructField("ma_ban_ghi", StringType(), True),
    StructField("ma_chuong_trinh", StringType(), True),
    StructField("ten_chuong_trinh", StringType(), True),
    StructField("nam_hoc", StringType(), True),
    StructField("hoc_ky", IntegerType(), True),
    StructField("so_sinh_vien", LongType(), True),
    StructField("so_luot_hoc_phan_dat", LongType(), True),
    StructField("tong_luot_hoc_phan", LongType(), True),
    StructField("tong_diem_gpa", DoubleType(), True),
    StructField("so_sinh_vien_tinh_gpa", LongType(), True),
    StructField("so_sinh_vien_canh_bao", LongType(), True),
    StructField("so_sinh_vien_nguy_co_nghi_hoc", LongType(), True),
    StructField("so_sinh_vien_dung_tien_do", LongType(), True),
    StructField("so_sinh_vien_danh_gia_tien_do", LongType(), True),
    StructField("thoi_gian_cap_nhat_nguon", TimestampType(), True),
    StructField("da_xoa", BooleanType(), True),
    StructField("_source_system", StringType(), True),
    StructField("_source_type", StringType(), True),
    StructField("_dataset", StringType(), True),
    StructField("_schema_version", StringType(), True),
    StructField("_ingestion_mode", StringType(), True),
    StructField("_batch_id", StringType(), True),
    StructField("_source_updated_at", TimestampType(), True),
    StructField("_ingested_at", TimestampType(), True),
    StructField("_record_checksum", StringType(), True),
])


SOURCE_SCHEMA_WITH_EXTRA = StructType([
    StructField("record_id", StringType(), True),
    StructField("program_code", StringType(), True),
    StructField("program_name", StringType(), True),
    StructField("academic_year", StringType(), True),
    StructField("semester", IntegerType(), True),
    StructField("student_count", LongType(), True),
    StructField("passed_course_count", LongType(), True),
    StructField("attempted_course_count", LongType(), True),
    StructField("gpa_point_sum", DoubleType(), True),
    StructField("gpa_student_count", LongType(), True),
    StructField("warning_student_count", LongType(), True),
    StructField("dropout_risk_student_count", LongType(), True),
    StructField("on_track_student_count", LongType(), True),
    StructField("progress_evaluated_student_count", LongType(), True),
    StructField("updated_at", TimestampType(), True),
    StructField("is_deleted", BooleanType(), True),
    StructField("future_source_note", StringType(), True),
    StructField("_source_system", StringType(), True),
    StructField("_source_type", StringType(), True),
    StructField("_dataset", StringType(), True),
    StructField("_schema_version", StringType(), True),
    StructField("_ingestion_mode", StringType(), True),
    StructField("_batch_id", StringType(), True),
    StructField("_source_updated_at", TimestampType(), True),
    StructField("_ingested_at", TimestampType(), True),
    StructField("_record_checksum", StringType(), True),
])


EXPECTED_DQ_RULES = (
    ("PROGRAM_CODE_NULL", "ma_chuong_trinh IS NULL"),
    ("ACADEMIC_YEAR_NULL", "nam_hoc IS NULL"),
    ("SEMESTER_NULL", "hoc_ky IS NULL"),
    ("SEMESTER_NON_POSITIVE", "hoc_ky <= 0"),
    ("STUDENT_COUNT_NEGATIVE", "so_sinh_vien IS NULL OR so_sinh_vien < 0"),
    (
        "PASSED_COURSE_COUNT_NEGATIVE",
        "so_luot_hoc_phan_dat IS NULL OR so_luot_hoc_phan_dat < 0",
    ),
    (
        "ATTEMPTED_COURSE_COUNT_NEGATIVE",
        "tong_luot_hoc_phan IS NULL OR tong_luot_hoc_phan < 0",
    ),
    (
        "PASSED_EXCEEDS_ATTEMPTED",
        "so_luot_hoc_phan_dat > tong_luot_hoc_phan",
    ),
    (
        "GPA_STUDENT_COUNT_NEGATIVE",
        "so_sinh_vien_tinh_gpa IS NULL OR so_sinh_vien_tinh_gpa < 0",
    ),
    (
        "GPA_STUDENT_COUNT_EXCEEDS_STUDENT_COUNT",
        "so_sinh_vien_tinh_gpa > so_sinh_vien",
    ),
    (
        "WARNING_STUDENT_COUNT_NEGATIVE",
        "so_sinh_vien_canh_bao IS NULL OR so_sinh_vien_canh_bao < 0",
    ),
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
    (
        "ON_TRACK_STUDENT_COUNT_NEGATIVE",
        "so_sinh_vien_dung_tien_do IS NULL OR so_sinh_vien_dung_tien_do < 0",
    ),
    (
        "PROGRESS_EVALUATED_STUDENT_COUNT_NEGATIVE",
        "so_sinh_vien_danh_gia_tien_do IS NULL OR so_sinh_vien_danh_gia_tien_do < 0",
    ),
    (
        "ON_TRACK_EXCEEDS_PROGRESS_EVALUATED",
        "so_sinh_vien_dung_tien_do > so_sinh_vien_danh_gia_tien_do",
    ),
)


class LearningOutcomesSilverBehaviorFreeze(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder
            .master("local[2]")
            .appName("day4-learning-outcomes-silver-behavior-freeze")
            .config("spark.sql.shuffle.partitions", "1")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")
        cls.original_silver_table = silver.SILVER_TABLE
        silver.SILVER_TABLE = "silver_behavior_freeze_target"

    @classmethod
    def tearDownClass(cls):
        silver.SILVER_TABLE = cls.original_silver_table
        cls.spark.stop()

    def canonical_row(self, **overrides):
        row = {
            "ma_ban_ghi": "R1",
            "ma_chuong_trinh": "P1",
            "ten_chuong_trinh": "Program 1",
            "nam_hoc": "2025-2026",
            "hoc_ky": 1,
            "so_sinh_vien": 100,
            "so_luot_hoc_phan_dat": 80,
            "tong_luot_hoc_phan": 100,
            "tong_diem_gpa": 300.0,
            "so_sinh_vien_tinh_gpa": 100,
            "so_sinh_vien_canh_bao": 5,
            "so_sinh_vien_nguy_co_nghi_hoc": 2,
            "so_sinh_vien_dung_tien_do": 90,
            "so_sinh_vien_danh_gia_tien_do": 100,
            "thoi_gian_cap_nhat_nguon": datetime(2026, 1, 1, 8, 0, 0),
            "da_xoa": False,
            "_source_system": "ctu_ioc",
            "_source_type": "API",
            "_dataset": "education.learning_outcomes",
            "_schema_version": "2.0",
            "_ingestion_mode": "FULL",
            "_batch_id": "batch-001",
            "_source_updated_at": datetime(2026, 1, 1, 8, 0, 0),
            "_ingested_at": datetime(2026, 1, 1, 8, 5, 0),
            "_record_checksum": "checksum-a",
        }
        row.update(overrides)
        return row

    def source_row(self, **overrides):
        row = {
            "record_id": "R1",
            "program_code": "P1",
            "program_name": "Program 1",
            "academic_year": "2025-2026",
            "semester": 1,
            "student_count": 100,
            "passed_course_count": 80,
            "attempted_course_count": 100,
            "gpa_point_sum": 300.0,
            "gpa_student_count": 100,
            "warning_student_count": 5,
            "dropout_risk_student_count": 2,
            "on_track_student_count": 90,
            "progress_evaluated_student_count": 100,
            "updated_at": datetime(2026, 1, 1, 8, 0, 0),
            "is_deleted": False,
            "future_source_note": "must-stay-in-bronze",
            "_source_system": "ctu_ioc",
            "_source_type": "API",
            "_dataset": "education.learning_outcomes",
            "_schema_version": "2.0",
            "_ingestion_mode": "FULL",
            "_batch_id": "batch-001",
            "_source_updated_at": datetime(2026, 1, 1, 8, 0, 0),
            "_ingested_at": datetime(2026, 1, 1, 8, 5, 0),
            "_record_checksum": "checksum-a",
        }
        row.update(overrides)
        return row

    def canonical_df(self, *rows):
        return self.spark.createDataFrame(list(rows), CANONICAL_SCHEMA)

    def source_df(self, *rows):
        return self.spark.createDataFrame(list(rows), SOURCE_SCHEMA_WITH_EXTRA)

    def install_target(self, *rows):
        target = self.canonical_df(*rows)
        target.createOrReplaceTempView(silver.SILVER_TABLE)

    def counts(self, result):
        return tuple(df.count() for df in result)

    def test_business_key_contract_is_frozen(self):
        self.assertEqual(
            silver.BUSINESS_KEY,
            ("ma_chuong_trinh", "nam_hoc", "hoc_ky"),
        )
        self.assertEqual(
            silver.business_key_sql("s", "t"),
            (
                "t.ma_chuong_trinh = s.ma_chuong_trinh AND "
                "t.nam_hoc = s.nam_hoc AND "
                "t.hoc_ky = s.hoc_ky"
            ),
        )

    def test_mapping_is_after_bronze_boundary_and_drops_unknown_source_field(self):
        mapped = silver.map_source_to_canonical(
            self.source_df(self.source_row())
        )

        expected_columns = (
            list(silver.canonical_business_fields())
            + list(silver.BRONZE_METADATA_FIELDS)
        )

        self.assertEqual(mapped.columns, expected_columns)
        self.assertNotIn("future_source_note", mapped.columns)

        row = mapped.first()
        self.assertEqual(row["ma_ban_ghi"], "R1")
        self.assertEqual(row["ma_chuong_trinh"], "P1")
        self.assertEqual(row["_record_checksum"], "checksum-a")

    def test_dq_rule_contract_is_exact(self):
        self.assertEqual(silver.DQ_RULES, EXPECTED_DQ_RULES)

    def test_dq_invalid_rows_are_split_with_current_reason_codes(self):
        invalid_row = self.canonical_row(
            ma_chuong_trinh=None,
            so_sinh_vien=-1,
        )

        valid, invalid = silver.split_dq(
            self.canonical_df(invalid_row)
        )

        self.assertEqual(valid.count(), 0)
        self.assertEqual(invalid.count(), 1)

        reasons = set(
            invalid.first()["rejection_reason"].split("|")
        )

        self.assertIn("PROGRAM_CODE_NULL", reasons)
        self.assertIn("STUDENT_COUNT_NEGATIVE", reasons)

    def test_equal_timestamp_different_checksum_blocks_entire_business_key(self):
        ts = datetime(2026, 1, 1, 8, 0, 0)

        rows = [
            self.canonical_row(
                ma_ban_ghi="R1-A",
                thoi_gian_cap_nhat_nguon=ts,
                _record_checksum="checksum-a",
            ),
            self.canonical_row(
                ma_ban_ghi="R1-B",
                thoi_gian_cap_nhat_nguon=ts,
                _record_checksum="checksum-b",
            ),
            self.canonical_row(
                ma_ban_ghi="R1-OLDER",
                thoi_gian_cap_nhat_nguon=datetime(2025, 12, 31, 8, 0, 0),
                _record_checksum="checksum-old",
            ),
        ]

        mergeable, conflicts = silver.split_equal_timestamp_conflicts(
            self.canonical_df(*rows)
        )

        # CURRENT behavior: one equal-timestamp conflict blocks every row for
        # that business key in this batch, including the older observation.
        self.assertEqual(mergeable.count(), 0)
        self.assertEqual(conflicts.count(), 3)
        self.assertEqual(
            {
                row["rejection_reason"]
                for row in conflicts.collect()
            },
            {"EQUAL_TIMESTAMP_DIFFERENT_CHECKSUM"},
        )

    def test_dedup_prefers_newest_source_timestamp(self):
        old = self.canonical_row(
            ma_ban_ghi="R-OLD",
            thoi_gian_cap_nhat_nguon=datetime(2026, 1, 1, 8, 0, 0),
            _record_checksum="checksum-old",
        )
        new = self.canonical_row(
            ma_ban_ghi="R-NEW",
            thoi_gian_cap_nhat_nguon=datetime(2026, 1, 2, 8, 0, 0),
            _record_checksum="checksum-new",
        )

        winner = silver.deterministic_deduplicate(
            self.canonical_df(old, new)
        ).first()

        self.assertEqual(winner["ma_ban_ghi"], "R-NEW")

    def test_dedup_same_source_timestamp_prefers_newest_ingestion(self):
        ts = datetime(2026, 1, 1, 8, 0, 0)

        earlier_ingest = self.canonical_row(
            ma_ban_ghi="R-EARLY",
            thoi_gian_cap_nhat_nguon=ts,
            _ingested_at=datetime(2026, 1, 1, 8, 5, 0),
            _record_checksum="checksum-a",
        )
        later_ingest = self.canonical_row(
            ma_ban_ghi="R-LATE",
            thoi_gian_cap_nhat_nguon=ts,
            _ingested_at=datetime(2026, 1, 1, 8, 10, 0),
            _record_checksum="checksum-a",
        )

        winner = silver.deterministic_deduplicate(
            self.canonical_df(earlier_ingest, later_ingest)
        ).first()

        self.assertEqual(winner["ma_ban_ghi"], "R-LATE")

    def test_classification_new_active_record_is_mergeable(self):
        self.install_target()

        result = silver.classify_against_target(
            self.spark,
            self.canonical_df(self.canonical_row()),
        )

        self.assertEqual(self.counts(result), (1, 0, 0, 0))

    def test_classification_unknown_delete_is_quarantined(self):
        self.install_target()

        result = silver.classify_against_target(
            self.spark,
            self.canonical_df(
                self.canonical_row(da_xoa=True)
            ),
        )

        mergeable, quarantine, stale, duplicate = result

        self.assertEqual(
            (
                mergeable.count(),
                quarantine.count(),
                stale.count(),
                duplicate.count(),
            ),
            (0, 1, 0, 0),
        )
        self.assertEqual(
            quarantine.first()["rejection_reason"],
            "DELETE_WITHOUT_EXISTING_TARGET",
        )

    def test_classification_newer_changed_record_is_mergeable(self):
        target = self.canonical_row(
            thoi_gian_cap_nhat_nguon=datetime(2026, 1, 1, 8, 0, 0),
            _record_checksum="checksum-old",
        )
        source = self.canonical_row(
            thoi_gian_cap_nhat_nguon=datetime(2026, 1, 2, 8, 0, 0),
            _record_checksum="checksum-new",
        )

        self.install_target(target)

        self.assertEqual(
            self.counts(
                silver.classify_against_target(
                    self.spark,
                    self.canonical_df(source),
                )
            ),
            (1, 0, 0, 0),
        )

    def test_classification_older_record_is_stale(self):
        target = self.canonical_row(
            thoi_gian_cap_nhat_nguon=datetime(2026, 1, 2, 8, 0, 0),
            _record_checksum="checksum-target",
        )
        source = self.canonical_row(
            thoi_gian_cap_nhat_nguon=datetime(2026, 1, 1, 8, 0, 0),
            _record_checksum="checksum-source",
        )

        self.install_target(target)

        self.assertEqual(
            self.counts(
                silver.classify_against_target(
                    self.spark,
                    self.canonical_df(source),
                )
            ),
            (0, 0, 1, 0),
        )

    def test_classification_equal_timestamp_same_checksum_is_duplicate(self):
        target = self.canonical_row()
        source = self.canonical_row()

        self.install_target(target)

        self.assertEqual(
            self.counts(
                silver.classify_against_target(
                    self.spark,
                    self.canonical_df(source),
                )
            ),
            (0, 0, 0, 1),
        )

    def test_classification_equal_timestamp_different_checksum_is_quarantine(self):
        target = self.canonical_row(
            _record_checksum="checksum-target",
        )
        source = self.canonical_row(
            _record_checksum="checksum-source",
        )

        self.install_target(target)

        mergeable, quarantine, stale, duplicate = (
            silver.classify_against_target(
                self.spark,
                self.canonical_df(source),
            )
        )

        self.assertEqual(
            (
                mergeable.count(),
                quarantine.count(),
                stale.count(),
                duplicate.count(),
            ),
            (0, 1, 0, 0),
        )
        self.assertEqual(
            quarantine.first()["rejection_reason"],
            "EQUAL_TIMESTAMP_DIFFERENT_CHECKSUM",
        )

    def test_soft_delete_existing_target_is_mergeable_only_when_newer(self):
        target = self.canonical_row(
            thoi_gian_cap_nhat_nguon=datetime(2026, 1, 1, 8, 0, 0),
            _record_checksum="checksum-active",
            da_xoa=False,
        )
        newer_delete = self.canonical_row(
            thoi_gian_cap_nhat_nguon=datetime(2026, 1, 2, 8, 0, 0),
            _record_checksum="checksum-delete",
            da_xoa=True,
        )

        self.install_target(target)

        mergeable, quarantine, stale, duplicate = (
            silver.classify_against_target(
                self.spark,
                self.canonical_df(newer_delete),
            )
        )

        self.assertEqual(
            (
                mergeable.count(),
                quarantine.count(),
                stale.count(),
                duplicate.count(),
            ),
            (1, 0, 0, 0),
        )
        self.assertTrue(mergeable.first()["da_xoa"])

    def test_repeated_soft_delete_is_duplicate(self):
        deleted = self.canonical_row(
            da_xoa=True,
            _record_checksum="checksum-delete",
        )

        self.install_target(deleted)

        self.assertEqual(
            self.counts(
                silver.classify_against_target(
                    self.spark,
                    self.canonical_df(deleted),
                )
            ),
            (0, 0, 0, 1),
        )

    def test_stale_delete_is_stale(self):
        target = self.canonical_row(
            thoi_gian_cap_nhat_nguon=datetime(2026, 1, 2, 8, 0, 0),
            _record_checksum="checksum-target",
            da_xoa=False,
        )
        stale_delete = self.canonical_row(
            thoi_gian_cap_nhat_nguon=datetime(2026, 1, 1, 8, 0, 0),
            _record_checksum="checksum-delete",
            da_xoa=True,
        )

        self.install_target(target)

        self.assertEqual(
            self.counts(
                silver.classify_against_target(
                    self.spark,
                    self.canonical_df(stale_delete),
                )
            ),
            (0, 0, 1, 0),
        )

    def test_merge_sql_guards_are_frozen(self):
        source = inspect.getsource(silver.merge_into_silver)

        self.assertEqual(
            silver.CANONICAL_UPDATED_AT_FIELD,
            "thoi_gian_cap_nhat_nguon",
        )
        self.assertEqual(
            silver.CANONICAL_DELETE_FIELD,
            "da_xoa",
        )
        self.assertIn(
            "s.{CANONICAL_UPDATED_AT_FIELD} > "
            "t.{CANONICAL_UPDATED_AT_FIELD}",
            source,
        )
        self.assertIn(
            "WHEN NOT MATCHED",
            source,
        )
        self.assertIn(
            "AND s.{CANONICAL_DELETE_FIELD} = false",
            source,
        )

    def test_nessie_lifecycle_order_is_frozen(self):
        source = inspect.getsource(
            silver.process_learning_outcomes_batch
        )

        expected_order = [
            "use_main(spark)",
            "create_branch(",
            "use_branch(",
            "read_bronze_batch(",
            "map_source_to_canonical(",
            "split_dq(",
            "split_equal_timestamp_conflicts(",
            "deterministic_deduplicate(",
            "classify_against_target(",
            "write_quarantine(",
            "merge_into_silver(",
            "merge_branch_to_main(",
        ]

        positions = []
        for token in expected_order:
            position = source.find(token)
            self.assertNotEqual(
                position,
                -1,
                msg=f"Missing lifecycle token: {token}",
            )
            positions.append(position)

        self.assertEqual(
            positions,
            sorted(positions),
            msg="Silver/Nessie lifecycle order changed",
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
