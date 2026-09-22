# -*- coding: utf-8 -*-
"""Unit tests for reusable equal-timestamp Silver conflict mechanics."""

from __future__ import annotations

import inspect
import unittest
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
    TimestampType,
)

import generic_silver_conflict as conflict


class GenericSilverConflictTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder
            .master("local[2]")
            .appName("day5-generic-silver-conflict")
            .config("spark.sql.shuffle.partitions", "1")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")
        cls.schema = StructType([
            StructField("key_a", StringType(), False),
            StructField("source_updated", TimestampType(), True),
            StructField("checksum", StringType(), True),
            StructField("payload", StringType(), True),
        ])

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def frame(self, *rows):
        return self.spark.createDataFrame(list(rows), self.schema)

    def split(self, df):
        return conflict.split_equal_timestamp_conflicts(
            df,
            business_key=("key_a",),
            source_updated_field="source_updated",
            checksum_field="checksum",
        )

    def row(
        self,
        *,
        key_a="K1",
        hour=8,
        checksum="checksum-a",
        payload="row",
    ):
        return {
            "key_a": key_a,
            "source_updated": datetime(2026, 1, 1, hour, 0, 0),
            "checksum": checksum,
            "payload": payload,
        }

    def test_equal_timestamp_different_checksum_blocks_entire_business_key(self):
        mergeable, conflicts = self.split(
            self.frame(
                self.row(hour=9, checksum="checksum-a", payload="conflict-a"),
                self.row(hour=9, checksum="checksum-b", payload="conflict-b"),
                self.row(hour=8, checksum="checksum-old", payload="older-row"),
            )
        )

        self.assertEqual(mergeable.count(), 0)
        self.assertEqual(conflicts.count(), 3)
        self.assertEqual(
            {row["payload"] for row in conflicts.collect()},
            {"conflict-a", "conflict-b", "older-row"},
        )
        self.assertEqual(
            {row["rejection_reason"] for row in conflicts.collect()},
            {"EQUAL_TIMESTAMP_DIFFERENT_CHECKSUM"},
        )

    def test_equal_timestamp_same_checksum_is_not_conflict(self):
        mergeable, conflicts = self.split(
            self.frame(
                self.row(checksum="same", payload="a"),
                self.row(checksum="same", payload="b"),
            )
        )

        self.assertEqual(mergeable.count(), 2)
        self.assertEqual(conflicts.count(), 0)

    def test_conflict_in_one_key_does_not_block_other_key(self):
        mergeable, conflicts = self.split(
            self.frame(
                self.row(key_a="K1", checksum="a", payload="k1-a"),
                self.row(key_a="K1", checksum="b", payload="k1-b"),
                self.row(key_a="K2", checksum="a", payload="k2"),
            )
        )

        self.assertEqual(
            {row["key_a"] for row in conflicts.collect()},
            {"K1"},
        )
        self.assertEqual(
            {row["key_a"] for row in mergeable.collect()},
            {"K2"},
        )

    def test_null_checksum_and_non_null_checksum_conflict(self):
        mergeable, conflicts = self.split(
            self.frame(
                self.row(checksum=None, payload="null"),
                self.row(checksum="checksum-a", payload="non-null"),
            )
        )

        self.assertEqual(mergeable.count(), 0)
        self.assertEqual(conflicts.count(), 2)

    def test_two_null_checksums_are_same_variant(self):
        mergeable, conflicts = self.split(
            self.frame(
                self.row(checksum=None, payload="null-a"),
                self.row(checksum=None, payload="null-b"),
            )
        )

        self.assertEqual(mergeable.count(), 2)
        self.assertEqual(conflicts.count(), 0)

    def test_generic_module_contains_no_learning_outcomes_literals(self):
        source = inspect.getsource(conflict)

        forbidden = (
            "ma_chuong_trinh",
            "nam_hoc",
            "hoc_ky",
            "learning_outcomes",
            "learning_outcomes_quarantine",
            "gpa_trung_binh",
            "ty_le_qua_hoc_phan",
        )

        for literal in forbidden:
            self.assertNotIn(literal, source)


if __name__ == "__main__":
    unittest.main(verbosity=2)
