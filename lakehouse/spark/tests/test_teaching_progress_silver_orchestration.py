# -*- coding: utf-8 -*-
"""Static contract tests for Teaching Progress Silver orchestration."""

from __future__ import annotations

import inspect
import unittest

import spark_teaching_progress_to_silver as teaching


class TeachingProgressSilverOrchestrationTest(unittest.TestCase):
    def test_demo_targets_are_registry_owned(self):
        self.assertEqual(
            teaching.SILVER_TABLE,
            "lakehouse.silver.teaching_progress",
        )
        self.assertEqual(
            teaching.QUARANTINE_TABLE,
            "lakehouse.silver.teaching_progress_quarantine",
        )

    def test_no_synthetic_delete_field_exists(self):
        self.assertFalse(
            teaching.DATASET_CONFIG.delete_supported
        )
        self.assertIsNone(
            teaching.CANONICAL_DELETE_FIELD
        )
        self.assertNotIn(
            "da_xoa",
            teaching.canonical_business_fields(),
        )

    def test_wrapper_reuses_generic_quarantine_and_nessie_helpers(self):
        source = inspect.getsource(teaching)

        for expected in (
            "generic_silver_quarantine",
            "create_branch",
            "use_branch",
            "use_main",
            "merge_branch_to_main",
            "process_teaching_progress_batch",
        ):
            self.assertIn(expected, source)

    def test_wrapper_does_not_copy_generic_merge_sql(self):
        source = inspect.getsource(teaching)

        self.assertNotIn(
            "MERGE INTO",
            source,
        )
        self.assertNotIn(
            "WHEN MATCHED",
            source,
        )

    def test_teaching_tables_have_no_delete_column(self):
        source = inspect.getsource(
            teaching.init_silver_tables_if_needed
        )

        self.assertNotIn(
            "da_xoa",
            source,
        )
        self.assertNotIn(
            "is_deleted",
            source,
        )

    def test_demo_dq_rule_count_is_frozen_for_day6(self):
        self.assertEqual(
            len(teaching.DQ_RULES),
            7,
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
