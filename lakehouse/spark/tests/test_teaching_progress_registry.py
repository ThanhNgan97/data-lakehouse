# -*- coding: utf-8 -*-
"""Day 6 registry tests for Teaching Progress demo onboarding."""

from __future__ import annotations

import unittest
from dataclasses import replace

from api_dataset_registry import (
    LEARNING_OUTCOMES_DATASET,
    TEACHING_PROGRESS_DATASET,
    get_dataset_config,
)


class TeachingProgressRegistryTest(unittest.TestCase):
    def setUp(self):
        self.learning = get_dataset_config(
            LEARNING_OUTCOMES_DATASET
        )
        self.teaching = get_dataset_config(
            TEACHING_PROGRESS_DATASET
        )

    def test_both_datasets_are_registered_without_collision(self):
        self.assertNotEqual(
            self.learning.dataset,
            self.teaching.dataset,
        )
        self.assertNotEqual(
            self.learning.bronze_prefix,
            self.teaching.bronze_prefix,
        )
        self.assertNotEqual(
            self.learning.silver_table,
            self.teaching.silver_table,
        )
        self.assertNotEqual(
            self.learning.quarantine_table,
            self.teaching.quarantine_table,
        )

    def test_teaching_identity_and_demo_version_are_explicit(self):
        self.assertEqual(
            self.teaching.dataset,
            "education.teaching_progress",
        )
        self.assertEqual(
            self.teaching.schema_version,
            "1.0-demo",
        )

    def test_teaching_source_fields_match_spark_schema(self):
        spark_fields = tuple(
            field.name
            for field in self.teaching.spark_source_schema.fields
        )
        self.assertEqual(
            spark_fields,
            self.teaching.source_fields,
        )

    def test_teaching_mapping_is_complete_one_to_one(self):
        self.assertEqual(
            set(self.teaching.source_to_canonical),
            set(self.teaching.source_fields),
        )
        self.assertEqual(
            len(set(self.teaching.canonical_fields)),
            len(self.teaching.canonical_fields),
        )

    def test_teaching_business_key_is_not_learning_key(self):
        self.assertEqual(
            self.teaching.business_key,
            (
                "ma_lop_hoc_phan",
                "nam_hoc",
                "hoc_ky",
            ),
        )
        self.assertNotEqual(
            self.teaching.business_key,
            self.learning.business_key,
        )

    def test_teaching_delete_is_explicitly_unsupported(self):
        self.assertIsNone(
            self.teaching.source_delete_field
        )
        self.assertFalse(
            self.teaching.delete_supported
        )
        self.assertTrue(
            self.learning.delete_supported
        )

    def test_none_delete_field_is_valid_registry_config(self):
        replaced = replace(
            self.teaching,
            source_delete_field=None,
        )
        self.assertFalse(
            replaced.delete_supported
        )

    def test_nonexistent_configured_delete_field_still_fails(self):
        with self.assertRaisesRegex(
            ValueError,
            "configured source control field",
        ):
            replace(
                self.teaching,
                source_delete_field="not_a_source_field",
            )


if __name__ == "__main__":
    unittest.main(verbosity=2)
