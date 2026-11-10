from pathlib import Path
import unittest
from unittest.mock import Mock, patch

from airflow.models import DagBag
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import (
    SparkKubernetesOperator,
)
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

    @patch("gx_bronze_gate.validate_file_bronze")
    def test_file_bronze_uses_local_copy_for_evidence(self, validate):
        from universal_lakehouse_pipeline import validate_universal_bronze

        values = {
            "registered_dataset_id": "education.learning_outcomes",
            "local_input_path": "/opt/airflow/spark/.staging_downloads/run/input.json",
            "s3_staging_key": "staging/manual/run/input.json",
        }
        task_instance = Mock()
        task_instance.xcom_pull.side_effect = lambda **kwargs: values.get(kwargs["key"])

        validate_universal_bronze(ti=task_instance, run_id="file_upload__run")

        self.assertEqual(
            validate.call_args.kwargs["path"],
            values["local_input_path"],
        )

    def test_file_candidate_runs_on_kubernetes(self):
        task = self.dag.get_task("file_flow.prepare_silver_candidate")
        self.assertIsInstance(task, SparkKubernetesOperator)
        self.assertEqual(task.application_file, "spark-file-candidate-airflow.yaml")


if __name__ == "__main__":
    unittest.main(verbosity=2)
