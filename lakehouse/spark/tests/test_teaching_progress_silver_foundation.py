# -*- coding: utf-8 -*-
"""Day 6 Teaching Progress Silver-foundation tests."""

from __future__ import annotations

import inspect
import unittest
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

import spark_teaching_progress_to_silver as teaching


class TeachingProgressSilverFoundationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder
            .master("local[2]")
            .appName("day6-teaching-silver-foundation")
            .config("spark.sql.shuffle.partitions", "1")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

        cls.source_schema = StructType([
            StructField("record_id", StringType(), True),
            StructField("unit_code", StringType(), True),
            StructField("unit_name", StringType(), True),
            StructField("course_section_code", StringType(), True),
            StructField("academic_year", StringType(), True),
            StructField("semester", IntegerType(), True),
            StructField("progress_percent", DoubleType(), True),
            StructField("updated_at", TimestampType(), True),
            StructField("future_teaching_source_note", StringType(), True),
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

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def source_row(self, **overrides):
        updated = datetime(2026, 1, 20, 9, 0, 0)
        row = {
            "record_id": "DEMO-TP-U01-C01|2025-2026|S1",
            "unit_code": "DEMO-U01",
            "unit_name": "Don vi 01",
            "course_section_code": "DEMO-TP-U01-C01",
            "academic_year": "2025-2026",
            "semester": 1,
            "progress_percent": 74.0,
            "updated_at": updated,
            "future_teaching_source_note": "bronze-only",
            "_source_system": "ctu_ioc",
            "_source_type": "API",
            "_dataset": "education.teaching_progress",
            "_schema_version": "1.0-demo",
            "_ingestion_mode": "FULL_DEMO",
            "_batch_id": "day6-test",
            "_source_updated_at": updated,
            "_ingested_at": datetime(2026, 9, 23, 8, 0, 0),
            "_record_checksum": "checksum-1",
        }
        row.update(overrides)
        return row

    def frame(self, *rows):
        return self.spark.createDataFrame(
            list(rows),
            self.source_schema,
        )

    def test_registry_drives_business_key_and_no_delete_semantics(self):
        self.assertEqual(
            teaching.BUSINESS_KEY,
            (
                "ma_lop_hoc_phan",
                "nam_hoc",
                "hoc_ky",
            ),
        )
        self.assertFalse(
            teaching.DATASET_CONFIG.delete_supported
        )
        self.assertIsNone(
            teaching.CANONICAL_DELETE_FIELD
        )

    def test_mapping_occurs_after_bronze_and_drops_unknown_source_field(self):
        mapped = teaching.map_source_to_canonical(
            self.frame(self.source_row())
        )

        self.assertEqual(
            tuple(mapped.columns),
            (
                "ma_ban_ghi",
                "ma_don_vi",
                "ten_don_vi",
                "ma_lop_hoc_phan",
                "nam_hoc",
                "hoc_ky",
                "ty_le_tien_do_giang_day",
                "thoi_gian_cap_nhat_nguon",
                "_source_system",
                "_source_type",
                "_dataset",
                "_schema_version",
                "_ingestion_mode",
                "_batch_id",
                "_source_updated_at",
                "_ingested_at",
                "_record_checksum",
            ),
        )
        self.assertNotIn(
            "future_teaching_source_note",
            mapped.columns,
        )
        self.assertEqual(
            mapped.first()["ma_lop_hoc_phan"],
            "DEMO-TP-U01-C01",
        )

    def test_mapping_requires_configured_source_fields(self):
        broken = self.frame(
            self.source_row()
        ).drop("unit_code")

        with self.assertRaisesRegex(
            ValueError,
            "missing required source field",
        ):
            teaching.map_source_to_canonical(
                broken
            )

    def test_mapping_requires_nine_bronze_metadata_fields(self):
        broken = self.frame(
            self.source_row()
        ).drop("_record_checksum")

        with self.assertRaisesRegex(
            ValueError,
            "missing required metadata field",
        ):
            teaching.map_source_to_canonical(
                broken
            )

    def test_valid_demo_row_passes_configured_dq(self):
        mapped = teaching.map_source_to_canonical(
            self.frame(self.source_row())
        )

        valid, invalid = teaching.split_dq(
            mapped
        )

        self.assertEqual(valid.count(), 1)
        self.assertEqual(invalid.count(), 0)

    def test_invalid_demo_values_are_routed_by_generic_dq(self):
        mapped = teaching.map_source_to_canonical(
            self.frame(
                self.source_row(
                    unit_code="",
                    semester=0,
                    progress_percent=120.0,
                )
            )
        )

        valid, invalid = teaching.split_dq(
            mapped
        )

        self.assertEqual(valid.count(), 0)
        self.assertEqual(invalid.count(), 1)

        reasons = invalid.first()[
            "rejection_reason"
        ].split("|")

        self.assertEqual(
            reasons,
            [
                "MISSING_UNIT_CODE",
                "INVALID_SEMESTER",
                "INVALID_TEACHING_PROGRESS_PERCENT",
            ],
        )

    def test_teaching_wrapper_reuses_generic_modules(self):
        source = inspect.getsource(teaching)

        expected_imports = (
            "generic_silver_quality",
            "generic_silver_dedup",
            "generic_silver_conflict",
            "generic_silver_classifier",
            "generic_silver_merge",
        )

        for module_name in expected_imports:
            self.assertIn(
                module_name,
                source,
            )

        self.assertNotIn(
            "WHEN MATCHED",
            source,
        )
        self.assertNotIn(
            "row_number()",
            source,
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
