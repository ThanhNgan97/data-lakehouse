# -*- coding: utf-8 -*-
"""Unit tests for reusable Silver Iceberg MERGE mechanics."""

from __future__ import annotations

import inspect
import unittest

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

import generic_silver_merge as merge


class GenericSilverMergeTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder
            .master("local[2]")
            .appName("day6-generic-silver-merge")
            .config("spark.sql.shuffle.partitions", "1")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")
        cls.schema = StructType([
            StructField("key_a", StringType(), False),
            StructField("key_b", StringType(), False),
            StructField("source_updated", TimestampType(), True),
            StructField("is_deleted", BooleanType(), True),
            StructField("payload", StringType(), True),
        ])

        cls.no_delete_schema = StructType([
            StructField("key_a", StringType(), False),
            StructField("key_b", StringType(), False),
            StructField("source_updated", TimestampType(), True),
            StructField("payload", StringType(), True),
        ])

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def build_sql(self):
        return merge.build_merge_sql(
            target_table="catalog.schema.target_table",
            source_view="configured_merge_source",
            business_key=("key_a", "key_b"),
            source_columns=(
                "key_a",
                "key_b",
                "source_updated",
                "is_deleted",
                "payload",
            ),
            source_updated_field="source_updated",
            delete_field="is_deleted",
        )

    def build_no_delete_sql(self):
        return merge.build_merge_sql(
            target_table="catalog.schema.target_table",
            source_view="configured_merge_source",
            business_key=("key_a", "key_b"),
            source_columns=(
                "key_a",
                "key_b",
                "source_updated",
                "payload",
            ),
            source_updated_field="source_updated",
            delete_field=None,
        )

    def test_business_key_sql_preserves_configured_order(self):
        self.assertEqual(
            merge.business_key_sql(("key_a", "key_b")),
            "t.key_a = s.key_a AND t.key_b = s.key_b",
        )

    def test_business_key_must_not_be_empty(self):
        with self.assertRaises(ValueError):
            merge.business_key_sql(())

    def test_merge_sql_preserves_newer_only_update_guard(self):
        sql = self.build_sql()

        self.assertIn("WHEN MATCHED", sql)
        self.assertIn(
            "AND s.source_updated > t.source_updated",
            sql,
        )

    def test_merge_sql_preserves_unmatched_non_deleted_insert_guard(self):
        sql = self.build_sql()

        self.assertIn("WHEN NOT MATCHED", sql)
        self.assertIn("AND s.is_deleted = false", sql)
        self.assertNotIn("THEN DELETE", sql)

    def test_no_delete_merge_omits_synthetic_delete_guard(self):
        sql = self.build_no_delete_sql()

        self.assertIn("WHEN NOT MATCHED", sql)
        self.assertNotIn("s.is_deleted", sql)
        self.assertNotIn("s.None", sql)
        self.assertNotIn("THEN DELETE", sql)

    def test_merge_sql_updates_and_inserts_configured_columns(self):
        sql = self.build_sql()

        for column in (
            "key_a",
            "key_b",
            "source_updated",
            "is_deleted",
            "payload",
            "_silver_updated_at",
        ):
            self.assertIn(f"t.{column} = s.{column}", sql)
            self.assertIn(column, sql)

    def test_empty_input_returns_zero_without_resolving_target_table(self):
        empty = self.spark.createDataFrame([], self.schema)

        result = merge.merge_into_silver(
            self.spark,
            empty,
            target_table="table_that_must_not_be_resolved",
            business_key=("key_a", "key_b"),
            source_columns=(
                "key_a",
                "key_b",
                "source_updated",
                "is_deleted",
                "payload",
            ),
            source_updated_field="source_updated",
            delete_field="is_deleted",
            source_view="configured_merge_source",
        )

        self.assertEqual(result, 0)

    def test_empty_no_delete_input_returns_zero_without_target_resolution(self):
        empty = self.spark.createDataFrame(
            [],
            self.no_delete_schema,
        )

        result = merge.merge_into_silver(
            self.spark,
            empty,
            target_table="table_that_must_not_be_resolved",
            business_key=("key_a", "key_b"),
            source_columns=(
                "key_a",
                "key_b",
                "source_updated",
                "payload",
            ),
            source_updated_field="source_updated",
            delete_field=None,
            source_view="configured_no_delete_merge_source",
        )

        self.assertEqual(result, 0)

    def test_generic_module_contains_no_dataset_literals(self):
        source = inspect.getsource(merge)

        forbidden = (
            "ma_chuong_trinh",
            "nam_hoc",
            "hoc_ky",
            "learning_outcomes",
            "learning_outcomes_quarantine",
            "gpa_trung_binh",
            "ty_le_qua_hoc_phan",
            "education.teaching_progress",
            "ma_lop_hoc_phan",
            "ty_le_tien_do_giang_day",
        )

        for literal in forbidden:
            self.assertNotIn(literal, source)


if __name__ == "__main__":
    unittest.main(verbosity=2)
