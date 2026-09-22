# -*- coding: utf-8 -*-
"""Unit tests for reusable Silver target-state classification mechanics."""

from __future__ import annotations

import inspect
import unittest
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

import generic_silver_classifier as classifier


class GenericSilverClassifierTest(unittest.TestCase):
    TARGET = "generic_silver_classifier_target"

    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder
            .master("local[2]")
            .appName("day5-generic-silver-classifier")
            .config("spark.sql.shuffle.partitions", "1")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

        cls.schema = StructType([
            StructField("key_a", StringType(), False),
            StructField("source_updated", TimestampType(), True),
            StructField("checksum", StringType(), True),
            StructField("is_deleted", BooleanType(), True),
            StructField("payload", StringType(), True),
        ])

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def frame(self, *rows):
        return self.spark.createDataFrame(list(rows), self.schema)

    def source_row(
        self,
        *,
        key_a="K1",
        hour=8,
        checksum="checksum-a",
        is_deleted=False,
        payload="source",
    ):
        return {
            "key_a": key_a,
            "source_updated": datetime(2026, 1, 1, hour, 0, 0),
            "checksum": checksum,
            "is_deleted": is_deleted,
            "payload": payload,
        }

    def install_target(self, *rows):
        self.frame(*rows).createOrReplaceTempView(self.TARGET)

    def classify(self, source):
        return classifier.classify_against_target(
            self.spark,
            source,
            target_table=self.TARGET,
            business_key=("key_a",),
            source_updated_field="source_updated",
            checksum_field="checksum",
            delete_field="is_deleted",
            source_columns=(
                "key_a",
                "source_updated",
                "checksum",
                "is_deleted",
                "payload",
            ),
        )

    def counts(self, outputs):
        return tuple(df.count() for df in outputs)

    def test_new_active_record_is_mergeable(self):
        self.install_target()

        outputs = self.classify(
            self.frame(self.source_row())
        )

        self.assertEqual(self.counts(outputs), (1, 0, 0, 0))

    def test_unknown_delete_is_quarantined(self):
        self.install_target()

        mergeable, quarantine, stale, duplicate = self.classify(
            self.frame(self.source_row(is_deleted=True))
        )

        self.assertEqual(
            (mergeable.count(), stale.count(), duplicate.count()),
            (0, 0, 0),
        )
        self.assertEqual(quarantine.count(), 1)
        self.assertEqual(
            quarantine.first()["rejection_reason"],
            "DELETE_WITHOUT_EXISTING_TARGET",
        )

    def test_newer_changed_record_is_mergeable(self):
        self.install_target(
            self.source_row(hour=8, checksum="old"),
        )

        outputs = self.classify(
            self.frame(
                self.source_row(hour=9, checksum="new"),
            )
        )

        self.assertEqual(self.counts(outputs), (1, 0, 0, 0))

    def test_older_record_is_stale(self):
        self.install_target(
            self.source_row(hour=9, checksum="target"),
        )

        outputs = self.classify(
            self.frame(
                self.source_row(hour=8, checksum="source"),
            )
        )

        self.assertEqual(self.counts(outputs), (0, 0, 1, 0))

    def test_equal_timestamp_same_checksum_is_duplicate(self):
        self.install_target(
            self.source_row(hour=8, checksum="same"),
        )

        outputs = self.classify(
            self.frame(
                self.source_row(hour=8, checksum="same"),
            )
        )

        self.assertEqual(self.counts(outputs), (0, 0, 0, 1))

    def test_equal_timestamp_different_checksum_is_quarantine(self):
        self.install_target(
            self.source_row(hour=8, checksum="target"),
        )

        mergeable, quarantine, stale, duplicate = self.classify(
            self.frame(
                self.source_row(hour=8, checksum="source"),
            )
        )

        self.assertEqual(
            (mergeable.count(), stale.count(), duplicate.count()),
            (0, 0, 0),
        )
        self.assertEqual(quarantine.count(), 1)
        self.assertEqual(
            quarantine.first()["rejection_reason"],
            "EQUAL_TIMESTAMP_DIFFERENT_CHECKSUM",
        )

    def test_newer_soft_delete_existing_target_is_mergeable(self):
        self.install_target(
            self.source_row(hour=8, is_deleted=False),
        )

        outputs = self.classify(
            self.frame(
                self.source_row(hour=9, is_deleted=True),
            )
        )

        self.assertEqual(self.counts(outputs), (1, 0, 0, 0))

    def test_repeated_soft_delete_is_duplicate(self):
        self.install_target(
            self.source_row(
                hour=8,
                checksum="same",
                is_deleted=True,
            ),
        )

        outputs = self.classify(
            self.frame(
                self.source_row(
                    hour=8,
                    checksum="same",
                    is_deleted=True,
                ),
            )
        )

        self.assertEqual(self.counts(outputs), (0, 0, 0, 1))

    def test_stale_delete_is_stale(self):
        self.install_target(
            self.source_row(hour=9, is_deleted=False),
        )

        outputs = self.classify(
            self.frame(
                self.source_row(hour=8, is_deleted=True),
            )
        )

        self.assertEqual(self.counts(outputs), (0, 0, 1, 0))

    def test_generic_module_contains_no_learning_outcomes_literals(self):
        source = inspect.getsource(classifier)

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
