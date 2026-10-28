from __future__ import annotations

import unittest

from gx_candidate_binding import CandidateBindingError, dbt_gold_candidate_binding


class GXCandidateBindingTest(unittest.TestCase):
    def test_binding_is_deterministic_and_run_scoped(self):
        first = dbt_gold_candidate_binding(
            "education.teaching_progress",
            "manual__42",
            "lakehouse.gold.teaching_progress_metrics",
        )
        repeated = dbt_gold_candidate_binding(
            "education.teaching_progress",
            "manual__42",
            "lakehouse.gold.teaching_progress_metrics",
        )
        other = dbt_gold_candidate_binding(
            "education.teaching_progress",
            "manual__43",
            "lakehouse.gold.teaching_progress_metrics",
        )

        self.assertEqual(first, repeated)
        self.assertRegex(first.candidate_branch, r"^dbt_gold_[0-9a-f]{24}$")
        self.assertEqual(
            first.physical_asset_ref,
            "lakehouse.gold.teaching_progress_metrics_"
            + first.candidate_branch.removeprefix("dbt_gold_"),
        )
        self.assertNotEqual(first, other)

    def test_only_safe_canonical_gold_assets_are_accepted(self):
        for asset in (
            "lakehouse.silver.teaching_progress",
            "lakehouse.gold.metrics;DROP TABLE x",
            "other.gold.metrics",
        ):
            with self.subTest(asset=asset), self.assertRaises(CandidateBindingError):
                dbt_gold_candidate_binding("education.teaching_progress", "run", asset)


if __name__ == "__main__":
    unittest.main(verbosity=2)
