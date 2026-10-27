from pathlib import Path
import unittest

from airflow.models import DagBag
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule


DAG_DIR = Path("/opt/airflow/dags")


class GXDatabasePipelineDagTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.bag = DagBag(dag_folder=str(DAG_DIR), include_examples=False)
        cls.dag = cls.bag.get_dag("universal_lakehouse_pipeline")

    def test_dag_imports_and_database_gate_exists(self):
        self.assertIsNotNone(self.dag)
        self.assertFalse(self.bag.import_errors)
        self.assertIsInstance(
            self.dag.get_task("gx_validate_database"), PythonOperator
        )

    def test_database_branch_has_its_own_gx_boundary(self):
        processor = self.dag.get_task("relational_flow.process_relational_context")
        database_gate = self.dag.get_task("gx_validate_database")
        file_gate = self.dag.get_task("gx_validate_file_upload")
        join = self.dag.get_task("join_and_smoke_test")

        self.assertEqual(
            database_gate.upstream_task_ids,
            {processor.task_id},
        )
        self.assertNotIn(processor.task_id, file_gate.upstream_task_ids)
        self.assertEqual(
            join.upstream_task_ids,
            {database_gate.task_id, file_gate.task_id},
        )
        self.assertEqual(join.trigger_rule, TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)

    def test_database_gate_retries_transient_failure_once(self):
        gate = self.dag.get_task("gx_validate_database")
        self.assertEqual(gate.retries, 1)


if __name__ == "__main__":
    unittest.main(verbosity=2)
