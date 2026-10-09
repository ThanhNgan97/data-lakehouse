from pathlib import Path
import sys
import unittest

from airflow.models import DagBag
from airflow.operators.bash import BashOperator


DAG_DIR = Path("/opt/airflow/dags")
DAG_FILE = DAG_DIR / "api_dataset_pipeline.py"

SPARK_DIR = "/opt/airflow/spark"

if SPARK_DIR not in sys.path:
    sys.path.insert(0, SPARK_DIR)

from api_dataset_registry import DATASETS


EXPECTED_TASKS = (
    "api_preflight",
    "ingest_bronze",
    "validate_bronze",
    "process_silver",
    "validate_silver",
    "process_gold",
    "validate_gold",
    "trino_smoke_test",
)

EXPECTED_COMMANDS = (
    "api-preflight",
    "ingest-bronze",
    "validate-bronze",
    "process-silver",
    "validate-silver",
    "process-gold",
    "validate-gold",
    "trino-smoke-test",
)


class ApiDatasetPipelineDagTest(
    unittest.TestCase
):
    @classmethod
    def setUpClass(cls):
        cls.bag = DagBag(
            dag_folder=str(DAG_DIR),
            include_examples=False,
        )
        cls.dag = cls.bag.get_dag(
            "api_dataset_pipeline"
        )

    def test_no_import_error_for_new_dag(self):
        error = self.bag.import_errors.get(
            str(DAG_FILE)
        )
        self.assertIsNone(error)

    def test_new_and_legacy_dags_both_exist(self):
        self.assertIsNotNone(self.dag)
        self.assertIsNotNone(
            self.bag.get_dag(
                "lakehouse_pipeline"
            )
        )

    def test_exact_eight_visible_tasks(self):
        self.assertEqual(
            tuple(
                task.task_id
                for task in self.dag.tasks
            ),
            EXPECTED_TASKS,
        )

    def test_task_graph_is_linear(self):
        for index, task_id in enumerate(
            EXPECTED_TASKS
        ):
            task = self.dag.get_task(task_id)

            expected_upstream = (
                set()
                if index == 0
                else {
                    EXPECTED_TASKS[
                        index - 1
                    ]
                }
            )
            expected_downstream = (
                set()
                if index
                == len(EXPECTED_TASKS) - 1
                else {
                    EXPECTED_TASKS[
                        index + 1
                    ]
                }
            )

            self.assertEqual(
                task.upstream_task_ids,
                expected_upstream,
            )
            self.assertEqual(
                task.downstream_task_ids,
                expected_downstream,
            )

    def test_all_tasks_use_bash_operator(self):
        self.assertTrue(
            all(
                isinstance(
                    task,
                    BashOperator,
                )
                for task in self.dag.tasks
            )
        )

    def test_ingest_bronze_uses_no_change_skip_code(
        self,
    ):
        task = self.dag.get_task(
            "ingest_bronze"
        )

        skip_codes = task.skip_on_exit_code

        if isinstance(skip_codes, int):
            self.assertEqual(
                skip_codes,
                99,
            )
        else:
            self.assertIn(
                99,
                skip_codes,
            )

    def test_each_task_invokes_generic_runner(
        self,
    ):
        for task, command in zip(
            self.dag.tasks,
            EXPECTED_COMMANDS,
        ):
            self.assertIn(
                "api_dataset_orchestration.py",
                task.bash_command,
            )
            self.assertIn(
                command,
                task.bash_command,
            )
            self.assertIn(
                '--dataset-id "$DATASET_ID"',
                task.bash_command,
            )
            self.assertIn(
                '--run-id "$AIRFLOW_RUN_ID"',
                task.bash_command,
            )

    def test_manual_config_parameter_is_templated(
        self,
    ):
        for task in self.dag.tasks:
            self.assertIn(
                "dag_run.conf.get('dataset_id'",
                task.env["DATASET_ID"],
            )

    def test_source_url_and_key_are_templated_from_run_config(self):
        for task in self.dag.tasks:
            self.assertIn(
                "dag_run.conf.get('source_api_url'",
                task.env["SOURCE_API_URL"],
            )
            self.assertIn(
                "dag_run.conf.get('source_api_key'",
                task.env["SOURCE_API_KEY"],
            )

    def test_checkpoint_updates_are_serialized(self):
        self.assertEqual(self.dag.max_active_runs, 1)

    def test_trigger_ui_param_is_registry_driven(
        self,
    ):
        param = self.dag.params.get_param(
            "dataset_id"
        )

        self.assertEqual(
            param.schema.get("type"),
            "string",
        )
        self.assertEqual(
            param.schema.get("enum"),
            sorted(DATASETS),
        )
        self.assertFalse(
            param.has_value,
        )

    def test_dag_has_no_business_field_leakage(
        self,
    ):
        source = DAG_FILE.read_text(
            encoding="utf-8"
        )

        forbidden = (
            "ma_chuong_trinh",
            "gpa_trung_binh",
            "ty_le_qua_hoc_phan",
            "ma_lop_hoc_phan",
            "ty_le_tien_do_giang_day",
            "progress_percent",
            "course_section_code",
            "student_count",
            "GenericGoldEngine",
            "GoldMetricDSL",
            "GoldFactory",
            "UniversalMartBuilder",
        )

        for token in forbidden:
            self.assertNotIn(
                token,
                source,
            )

    def test_dag_does_not_duplicate_dataset_routes(
        self,
    ):
        source = DAG_FILE.read_text(
            encoding="utf-8"
        )

        self.assertNotIn(
            "education.learning_outcomes",
            source,
        )
        self.assertNotIn(
            "education.teaching_progress",
            source,
        )

    def test_no_superset_processing_task(self):
        self.assertNotIn(
            "superset",
            {
                task.task_id
                for task in self.dag.tasks
            },
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
