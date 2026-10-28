from pathlib import Path
import unittest

from airflow.models import DagBag
from airflow.utils.trigger_rule import TriggerRule


DAG_DIR = Path("/opt/airflow/dags")


class GXDatabasePipelineDagTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.bag = DagBag(dag_folder=str(DAG_DIR), include_examples=False)
        cls.dag = cls.bag.get_dag("universal_lakehouse_pipeline")

    def test_legacy_dag_imports_without_gx_tasks(self):
        self.assertIsNotNone(self.dag)
        self.assertFalse(self.bag.import_errors)
        task_ids = {task.task_id for task in self.dag.tasks}
        self.assertNotIn("gx_validate_database", task_ids)
        self.assertNotIn("gx_validate_file_upload", task_ids)

    def test_legacy_branches_join_directly(self):
        processor = self.dag.get_task("relational_flow.process_relational_context")
        join = self.dag.get_task("join_and_smoke_test")

        self.assertIn(processor.task_id, join.upstream_task_ids)
        self.assertIn("generic_flow.process_dynamic_silver_gold", join.upstream_task_ids)
        self.assertIn("api_flow.run_registered_api", join.upstream_task_ids)
        self.assertIn("kpi_flow.predictive_analysis", join.upstream_task_ids)
        self.assertEqual(join.trigger_rule, TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)


if __name__ == "__main__":
    unittest.main(verbosity=2)
