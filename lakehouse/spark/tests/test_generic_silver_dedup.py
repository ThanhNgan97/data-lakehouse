# -*- coding: utf-8 -*-
"""Unit tests for reusable deterministic Silver dedup mechanics."""

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

import generic_silver_dedup as dedup
import generic_silver_conflict as conflict


class GenericSilverDedupTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder
            .master("local[2]")
            .appName("day5-generic-silver-dedup")
            .config("spark.sql.shuffle.partitions", "1")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")
        cls.schema = StructType([
            StructField("key_a", StringType(), False),
            StructField("source_updated", TimestampType(), True),
            StructField("ingested_at", TimestampType(), True),
            StructField("batch_id", StringType(), True),
            StructField("checksum", StringType(), True),
            StructField("record_id", StringType(), True),
            StructField("payload", StringType(), True),
        ])

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def frame(self, *rows):
        return self.spark.createDataFrame(list(rows), self.schema)

    def run_dedup(self, df):
        return dedup.deterministic_deduplicate(
            df,
            business_key=("key_a",),
            source_updated_field="source_updated",
            ingested_at_field="ingested_at",
            batch_id_field="batch_id",
            checksum_field="checksum",
            record_id_field="record_id",
        )

    def base_row(self, **overrides):
        row = {
            "key_a": "K1",
            "source_updated": datetime(2026, 1, 1, 8, 0, 0),
            "ingested_at": datetime(2026, 1, 1, 8, 5, 0),
            "batch_id": "batch-001",
            "checksum": "checksum-b",
            "record_id": "R2",
            "payload": "base",
        }
        row.update(overrides)
        return row

    def test_prefers_newest_source_timestamp(self):
        winner = self.run_dedup(
            self.frame(
                self.base_row(
                    payload="old",
                    source_updated=datetime(2026, 1, 1, 7, 0, 0),
                ),
                self.base_row(
                    payload="new",
                    source_updated=datetime(2026, 1, 1, 9, 0, 0),
                ),
            )
        ).first()

        self.assertEqual(winner["payload"], "new")

    def test_same_source_timestamp_prefers_newest_ingestion(self):
        winner = self.run_dedup(
            self.frame(
                self.base_row(
                    payload="older-ingest",
                    ingested_at=datetime(2026, 1, 1, 8, 5, 0),
                ),
                self.base_row(
                    payload="newer-ingest",
                    ingested_at=datetime(2026, 1, 1, 8, 6, 0),
                ),
            )
        ).first()

        self.assertEqual(winner["payload"], "newer-ingest")

    def test_batch_id_desc_is_third_tie_breaker(self):
        winner = self.run_dedup(
            self.frame(
                self.base_row(
                    payload="batch-a",
                    batch_id="batch-a",
                ),
                self.base_row(
                    payload="batch-z",
                    batch_id="batch-z",
                ),
            )
        ).first()

        self.assertEqual(winner["payload"], "batch-z")

    def test_checksum_asc_is_fourth_tie_breaker(self):
        winner = self.run_dedup(
            self.frame(
                self.base_row(
                    payload="checksum-z",
                    checksum="checksum-z",
                ),
                self.base_row(
                    payload="checksum-a",
                    checksum="checksum-a",
                ),
            )
        ).first()

        self.assertEqual(winner["payload"], "checksum-a")

    def test_record_id_asc_is_final_tie_breaker(self):
        winner = self.run_dedup(
            self.frame(
                self.base_row(
                    payload="record-z",
                    record_id="R9",
                ),
                self.base_row(
                    payload="record-a",
                    record_id="R1",
                ),
            )
        ).first()

        self.assertEqual(winner["payload"], "record-a")

    def test_returns_one_winner_per_business_key(self):
        df = self.frame(
            self.base_row(key_a="K1", payload="k1-old"),
            self.base_row(
                key_a="K1",
                payload="k1-new",
                source_updated=datetime(2026, 1, 1, 9, 0, 0),
            ),
            self.base_row(key_a="K2", payload="k2"),
        )

        result = self.run_dedup(df)

        self.assertEqual(result.count(), 2)
        self.assertEqual(
            {row["key_a"] for row in result.collect()},
            {"K1", "K2"},
        )

    def test_conflicts_are_quarantined_before_exact_duplicates_are_collapsed(self):
        source = self.frame(
            self.base_row(key_a="K1", checksum="same", record_id="R1"),
            self.base_row(key_a="K1", checksum="same", record_id="R1"),
            self.base_row(key_a="K2", checksum="a", record_id="R2"),
            self.base_row(key_a="K2", checksum="b", record_id="R3"),
        )

        mergeable, conflicts = conflict.split_equal_timestamp_conflicts(
            source,
            business_key=("key_a",),
            source_updated_field="source_updated",
            checksum_field="checksum",
        )
        result = self.run_dedup(mergeable)

        self.assertEqual(result.count(), 1)
        self.assertEqual(result.first()["key_a"], "K1")
        self.assertEqual(conflicts.count(), 2)
        self.assertEqual(
            {row["key_a"] for row in conflicts.collect()},
            {"K2"},
        )

    def test_generic_module_contains_no_learning_outcomes_literals(self):
        source = inspect.getsource(dedup)

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
