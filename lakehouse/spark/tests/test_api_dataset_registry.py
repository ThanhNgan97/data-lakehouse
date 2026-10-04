# -*- coding: utf-8 -*-
"""Day 4 regression tests for declarative dataset registry ownership."""

from __future__ import annotations

import unittest
from dataclasses import replace

from api_dataset_registry import (
    LEARNING_OUTCOMES_DATASET,
    get_dataset_config,
)


class ApiDatasetRegistryHardeningTest(unittest.TestCase):
    def setUp(self):
        self.config = get_dataset_config(LEARNING_OUTCOMES_DATASET)

    def test_dataset_identity_and_schema_version(self):
        self.assertEqual(
            self.config.dataset,
            "education.learning_outcomes",
        )
        self.assertEqual(self.config.schema_version, "2.0")

    def test_source_fields_match_spark_schema_exactly(self):
        spark_fields = tuple(
            field.name
            for field in self.config.spark_source_schema.fields
        )
        self.assertEqual(spark_fields, self.config.source_fields)

    def test_source_to_canonical_is_one_to_one_and_complete(self):
        self.assertEqual(
            set(self.config.source_to_canonical),
            set(self.config.source_fields),
        )
        self.assertEqual(
            len(set(self.config.canonical_fields)),
            len(self.config.canonical_fields),
        )
        self.assertEqual(
            len(self.config.canonical_fields),
            len(self.config.source_fields),
        )

    def test_control_fields_resolve_to_current_canonical_names(self):
        self.assertEqual(
            self.config.canonical_field(self.config.record_id_field),
            "ma_ban_ghi",
        )
        self.assertEqual(
            self.config.canonical_field(
                self.config.source_updated_at_field
            ),
            "thoi_gian_cap_nhat_nguon",
        )
        self.assertEqual(
            self.config.canonical_field(self.config.source_delete_field),
            "da_xoa",
        )

    def test_business_key_is_frozen(self):
        self.assertEqual(
            self.config.business_key,
            ("ma_chuong_trinh", "nam_hoc", "hoc_ky"),
        )

    def test_silver_targets_are_owned_by_registry(self):
        self.assertEqual(
            self.config.silver_table,
            "lakehouse.silver.learning_outcomes",
        )
        self.assertEqual(
            self.config.quarantine_table,
            "lakehouse.silver.learning_outcomes_quarantine",
        )

    def test_invalid_mapping_fails_fast(self):
        broken_mapping = dict(self.config.source_to_canonical)
        broken_mapping.pop("program_code")

        with self.assertRaisesRegex(
            ValueError,
            "source_to_canonical must map exactly source_fields",
        ):
            replace(
                self.config,
                source_to_canonical=broken_mapping,
            )

    def test_invalid_business_key_fails_fast(self):
        with self.assertRaisesRegex(
            ValueError,
            "business_key must contain configured canonical field",
        ):
            replace(
                self.config,
                business_key=("not_a_canonical_field",),
            )

    def test_unknown_canonical_resolution_fails_clearly(self):
        with self.assertRaisesRegex(
            ValueError,
            "has no canonical mapping",
        ):
            self.config.canonical_field("not_a_source_field")


if __name__ == "__main__":
    unittest.main(verbosity=2)
