from __future__ import annotations

import unittest
from pathlib import Path

from airflow.models import DagBag


DAG_FILE = Path("/opt/airflow/dags/file_upload_candidate_pipeline.py")


class FileUploadCandidateDagTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.bag = DagBag(dag_folder=str(DAG_FILE.parent), include_examples=False)
        cls.dag = cls.bag.get_dag("file_upload_candidate_pipeline")

    def test_imports(self):
        self.assertIsNone(self.bag.import_errors.get(str(DAG_FILE)))
        self.assertIsNotNone(self.dag)

    def test_registered_path_is_candidate_first(self):
        expected = (
            "profile_and_resolve_contract",
            "branch_contract_status",
            "prepare_file_silver_candidate",
            "gx_validate_file_candidate",
            "publish_file_silver_candidate",
            "validate_published_silver",
            "prepare_gold_candidate",
            "build_gold_candidate",
            "validate_dbt_candidate",
            "gx_validate_gold_candidate",
            "gx_reconcile_candidate",
            "publish_gold_candidate",
            "trino_smoke_test",
            "provision_superset",
            "archive_source",
        )
        for before, after in zip(expected, expected[1:]):
            self.assertIn(after, self.dag.get_task(before).downstream_task_ids)

    def test_not_configured_has_no_path_to_publishers(self):
        onboarding = self.dag.get_task("onboarding_required")
        self.assertFalse(onboarding.downstream_task_ids)
        self.assertNotIn(
            "onboarding_required",
            self.dag.get_task("publish_file_silver_candidate").upstream_task_ids,
        )


if __name__ == "__main__":
    unittest.main()
