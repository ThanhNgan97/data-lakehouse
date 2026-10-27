from pathlib import Path
import unittest
import yaml

from airflow.models import DagBag
from airflow.operators.bash import BashOperator
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import (
    SparkKubernetesOperator,
)


DAG_DIR = Path("/opt/airflow/dags")
TEMPLATE_DIR = Path("/opt/airflow/k8s/spark/templates")


class SparkKubernetesDagContractTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.bag = DagBag(dag_folder=str(DAG_DIR), include_examples=False)
        cls.dag = cls.bag.get_dag("universal_lakehouse_pipeline")

    def test_dag_imports_without_errors(self):
        self.assertIsNotNone(self.dag)
        self.assertFalse(self.bag.import_errors)

    def test_only_non_spark_kpi_ingest_stays_local(self):
        self.assertIsInstance(
            self.dag.get_task("kpi_flow.ingest_bronze"), BashOperator
        )

    def test_every_spark_compute_task_uses_kubernetes(self):
        task_ids = (
            "kpi_flow.bronze_to_silver",
            "kpi_flow.silver_to_gold",
            "kpi_flow.predictive_analysis",
            "api_flow.run_registered_api",
            "generic_flow.process_dynamic_silver_gold",
            "relational_flow.process_relational_context",
        )
        for task_id in task_ids:
            task = self.dag.get_task(task_id)
            self.assertIsInstance(task, SparkKubernetesOperator)
            self.assertEqual(task.pool, "spark_k8s")
            self.assertEqual(task.namespace, "lakehouse-spark")
            self.assertEqual(task.kubernetes_conn_id, "kubernetes_default")

    def test_airflow_templates_define_driver_and_executor_labels(self):
        for template_path in TEMPLATE_DIR.glob("*.yaml"):
            template = yaml.safe_load(template_path.read_text(encoding="utf-8"))
            self.assertTrue(template["spec"]["driver"]["labels"], template_path.name)
            self.assertTrue(template["spec"]["executor"]["labels"], template_path.name)


if __name__ == "__main__":
    unittest.main(verbosity=2)
