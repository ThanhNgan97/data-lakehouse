# -*- coding: utf-8 -*-
"""Unit tests for the frozen Day 7 Teaching Progress Gold contract."""

from __future__ import annotations

import inspect
import unittest
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

import spark_teaching_progress_to_gold as gold


class TeachingProgressGoldTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder
            .master("local[2]")
            .appName(
                "day7-teaching-progress-gold"
            )
            .config(
                "spark.sql.shuffle.partitions",
                "1",
            )
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel(
            "ERROR"
        )

        cls.silver_schema = StructType([
            StructField(
                "ma_ban_ghi",
                StringType(),
                True,
            ),
            StructField(
                "ma_don_vi",
                StringType(),
                True,
            ),
            StructField(
                "ten_don_vi",
                StringType(),
                True,
            ),
            StructField(
                "ma_lop_hoc_phan",
                StringType(),
                True,
            ),
            StructField(
                "nam_hoc",
                StringType(),
                True,
            ),
            StructField(
                "hoc_ky",
                IntegerType(),
                True,
            ),
            StructField(
                "ty_le_tien_do_giang_day",
                DoubleType(),
                True,
            ),
            StructField(
                "thoi_gian_cap_nhat_nguon",
                TimestampType(),
                True,
            ),
            StructField(
                "_batch_id",
                StringType(),
                True,
            ),
            StructField(
                "_record_checksum",
                StringType(),
                True,
            ),
        ])

        cls.original_gold_table = (
            gold.GOLD_TABLE
        )
        cls.original_silver_table = (
            gold.SILVER_TABLE
        )

    @classmethod
    def tearDownClass(cls):
        gold.GOLD_TABLE = (
            cls.original_gold_table
        )
        gold.SILVER_TABLE = (
            cls.original_silver_table
        )
        cls.spark.stop()

    def setUp(self):
        gold.GOLD_TABLE = "day7_test_gold"
        gold.SILVER_TABLE = (
            "day7_test_silver"
        )

    def silver_row(self, **overrides):
        row = {
            "ma_ban_ghi": (
                "DEMO-TP-U01-C01|"
                "2025-2026|S1"
            ),
            "ma_don_vi": "DEMO-U01",
            "ten_don_vi": "Don vi 01",
            "ma_lop_hoc_phan": (
                "DEMO-TP-U01-C01"
            ),
            "nam_hoc": "2025-2026",
            "hoc_ky": 1,
            "ty_le_tien_do_giang_day": 74.0,
            "thoi_gian_cap_nhat_nguon": (
                datetime(
                    2026, 9, 23, 9, 30, 0
                )
            ),
            "_batch_id": "day7-test",
            "_record_checksum": (
                "checksum-teaching-1"
            ),
        }
        row.update(overrides)
        return row

    def frame(self, *rows):
        return self.spark.createDataFrame(
            list(rows),
            self.silver_schema,
        )

    def install_quality_views(
        self,
        silver,
        gold_frame,
    ):
        silver.createOrReplaceTempView(
            gold.SILVER_TABLE
        )
        gold_frame.createOrReplaceTempView(
            gold.GOLD_TABLE
        )

    def test_contract_grain_and_columns_are_frozen(self):
        self.assertEqual(
            gold.BUSINESS_KEY,
            (
                "ma_lop_hoc_phan",
                "nam_hoc",
                "hoc_ky",
            ),
        )

        self.assertEqual(
            gold.GOLD_COLUMNS,
            [
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
            ],
        )

    def test_transform_is_row_preserving(self):
        silver = self.frame(
            self.silver_row(),
            self.silver_row(
                ma_ban_ghi=(
                    "DEMO-TP-U01-C02|"
                    "2025-2026|S1"
                ),
                ma_lop_hoc_phan=(
                    "DEMO-TP-U01-C02"
                ),
                ty_le_tien_do_giang_day=81.0,
                _record_checksum=(
                    "checksum-teaching-2"
                ),
            ),
        )

        transformed = (
            gold.transform_teaching_progress_gold(
                silver
            )
        )

        self.assertEqual(
            transformed.count(),
            2,
        )

        self.assertEqual(
            tuple(transformed.columns),
            tuple(gold.GOLD_COLUMNS),
        )

    def test_source_progress_is_not_recalculated(self):
        silver = self.frame(
            self.silver_row(
                ty_le_tien_do_giang_day=63.25,
            )
        )

        row = (
            gold.transform_teaching_progress_gold(
                silver
            )
            .first()
        )

        self.assertAlmostEqual(
            row[
                "ty_le_tien_do_giang_day"
            ],
            63.25,
        )

    def test_monitor_count_is_exactly_one_per_gold_row(self):
        silver = self.frame(
            self.silver_row()
        )

        row = (
            gold.transform_teaching_progress_gold(
                silver
            )
            .select(
                "so_ban_ghi_theo_doi"
            )
            .first()
        )

        self.assertEqual(
            row["so_ban_ghi_theo_doi"],
            1,
        )

    def test_lineage_is_directly_preserved(self):
        source = self.silver_row()
        silver = self.frame(source)

        row = (
            gold.transform_teaching_progress_gold(
                silver
            )
            .first()
        )

        self.assertEqual(
            row["source_record_id"],
            source["ma_ban_ghi"],
        )
        self.assertEqual(
            row["source_updated_at"],
            source[
                "thoi_gian_cap_nhat_nguon"
            ],
        )
        self.assertEqual(
            row["source_batch_id"],
            source["_batch_id"],
        )
        self.assertEqual(
            row[
                "source_record_checksum"
            ],
            source["_record_checksum"],
        )

    def test_quality_gate_passes_valid_gold(self):
        silver = self.frame(
            self.silver_row()
        )

        gold_frame = (
            gold.transform_teaching_progress_gold(
                silver
            )
        )

        self.install_quality_views(
            silver,
            gold_frame,
        )

        result = gold.check_gold_quality(
            self.spark,
            expected_silver_rows=1,
        )

        self.assertEqual(
            result["gold_rows"],
            1,
        )
        self.assertEqual(
            result["distinct_gold_keys"],
            1,
        )
        self.assertEqual(
            result["lineage_mismatch"],
            0,
        )

    def test_quality_gate_rejects_invalid_percentage(self):
        silver = self.frame(
            self.silver_row()
        )

        invalid_gold = (
            gold.transform_teaching_progress_gold(
                silver
            )
            .withColumn(
                "ty_le_tien_do_giang_day",
                gold.F.lit(120.0),
            )
        )

        self.install_quality_views(
            silver,
            invalid_gold,
        )

        with self.assertRaisesRegex(
            RuntimeError,
            "INVALID_PROGRESS",
        ):
            gold.check_gold_quality(
                self.spark,
                expected_silver_rows=1,
            )

    def test_quality_gate_rejects_duplicate_grain(self):
        silver = self.frame(
            self.silver_row()
        )

        valid_gold = (
            gold.transform_teaching_progress_gold(
                silver
            )
        )

        duplicate_gold = (
            valid_gold.unionByName(
                valid_gold
            )
        )

        self.install_quality_views(
            silver,
            duplicate_gold,
        )

        with self.assertRaisesRegex(
            RuntimeError,
            "ROW_RECONCILIATION",
        ):
            gold.check_gold_quality(
                self.spark,
                expected_silver_rows=1,
            )

    def test_quality_gate_rejects_lineage_mismatch(self):
        silver = self.frame(
            self.silver_row()
        )

        invalid_gold = (
            gold.transform_teaching_progress_gold(
                silver
            )
            .withColumn(
                "source_record_checksum",
                gold.F.lit(
                    "different-checksum"
                ),
            )
        )

        self.install_quality_views(
            silver,
            invalid_gold,
        )

        with self.assertRaisesRegex(
            RuntimeError,
            "LINEAGE_MISMATCH",
        ):
            gold.check_gold_quality(
                self.spark,
                expected_silver_rows=1,
            )

    def test_no_learning_kpi_or_trend_logic_is_copied(self):
        source = inspect.getsource(gold)

        forbidden = (
            "ty_le_qua_hoc_phan",
            "gpa_trung_binh",
            "ty_le_dung_tien_do",
            "chenh_lech_",
            "Window",
            "lag(",
            "behind_schedule",
            "on_schedule",
            "weighted_progress",
        )

        for token in forbidden:
            self.assertNotIn(
                token,
                source,
            )

    def test_gold_does_not_introduce_generic_framework(self):
        source = inspect.getsource(gold)

        forbidden = (
            "GenericGoldEngine",
            "GoldMetricDSL",
            "GoldFactory",
            "UniversalMartBuilder",
        )

        for token in forbidden:
            self.assertNotIn(
                token,
                source,
            )


if __name__ == "__main__":
    unittest.main(verbosity=2)
