from pathlib import Path
import unittest

from airflow.models import DagBag
from airflow.utils.trigger_rule import TriggerRule


DAG_DIR = Path("/opt/airflow/dags")


class GXUniversalPipelineDagTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.bag = DagBag(dag_folder=str(DAG_DIR), include_examples=False)
        cls.dag = cls.bag.get_dag("universal_lakehouse_pipeline")

    def test_universal_dag_imports_with_candidate_gates(self):
        self.assertIsNotNone(self.dag)
        self.assertFalse(self.bag.import_errors)
        task_ids = {task.task_id for task in self.dag.tasks}
        for source_kind in ("api", "file", "database"):
            self.assertIn(f"{source_kind}_flow.gx_validate_silver", task_ids)
            self.assertIn(f"{source_kind}_flow.publish_silver_candidate", task_ids)
            self.assertIn(f"{source_kind}_gold.gx_validate", task_ids)
            self.assertIn(f"{source_kind}_gold.gx_reconcile", task_ids)
            self.assertIn(f"{source_kind}_gold.publish", task_ids)
        self.assertIn("file_flow.gx_validate_bronze", task_ids)
        self.assertIn("api_flow.gx_validate_bronze", task_ids)
        self.assertIn("onboarding_required", task_ids)

    def test_only_governed_gold_publishers_reach_smoke_test(self):
        join = self.dag.get_task("join_and_smoke_test")
        self.assertEqual(
            join.upstream_task_ids,
            {
                "api_gold.publish",
                "file_gold.publish",
                "database_gold.publish",
            },
        )
        self.assertEqual(join.trigger_rule, TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)

    def test_candidate_must_pass_gx_before_each_publish(self):
        for source_kind in ("api", "file", "database"):
            silver_publish = self.dag.get_task(
                f"{source_kind}_flow.publish_silver_candidate"
            )
            self.assertEqual(
                silver_publish.upstream_task_ids,
                {f"{source_kind}_flow.gx_validate_silver"},
            )
            gold_publish = self.dag.get_task(f"{source_kind}_gold.publish")
            self.assertEqual(
                gold_publish.upstream_task_ids,
                {f"{source_kind}_gold.gx_reconcile"},
            )

    def test_old_direct_publish_processors_are_removed_from_graph(self):
        task_ids = {task.task_id for task in self.dag.tasks}
        self.assertNotIn("generic_flow.process_dynamic_silver_gold", task_ids)
        self.assertNotIn("relational_flow.process_relational_context", task_ids)
        self.assertNotIn("kpi_flow.silver_to_gold", task_ids)


if __name__ == "__main__":
    unittest.main(verbosity=2)
