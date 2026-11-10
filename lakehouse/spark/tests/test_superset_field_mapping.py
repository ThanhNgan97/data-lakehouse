from __future__ import annotations

import unittest

from ai_dataset_router import RoutingDecision
from superset_dynamic_provisioner import _gold_field_name


class SupersetFieldMappingTest(unittest.TestCase):
    def test_registered_teaching_fields_map_to_gold_columns(self):
        decision = RoutingDecision(
            dataset_domain="education",
            dataset_entity="education_teaching_progress",
            route_target="registered_api",
            registered_dataset_id="education.teaching_progress",
            target_silver_table="lakehouse.silver.teaching_progress",
            target_quarantine_table="lakehouse.silver.teaching_progress_quarantine",
            target_gold_table="lakehouse.gold.teaching_progress_metrics",
            reasoning="registered teaching-progress contract",
        )
        self.assertEqual(
            _gold_field_name(decision, "progress_percent"),
            "ty_le_tien_do_giang_day",
        )
        self.assertEqual(
            _gold_field_name(decision, "updated_at"),
            "source_updated_at",
        )


if __name__ == "__main__":
    unittest.main()
