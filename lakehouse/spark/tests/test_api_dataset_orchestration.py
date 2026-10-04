import hashlib
import unittest

from api_dataset_registry import (
    DATASETS,
    LEARNING_OUTCOMES_DATASET,
    TEACHING_PROGRESS_DATASET,
    get_dataset_config,
)
from api_dataset_orchestration import (
    COMMANDS,
    deterministic_batch_id,
    require_orchestration_config,
    source_api_url,
)


class ApiDatasetOrchestrationTest(
    unittest.TestCase
):
    def test_both_registered_datasets_are_orchestration_ready(
        self,
    ):
        self.assertEqual(
            set(DATASETS),
            {
                LEARNING_OUTCOMES_DATASET,
                TEACHING_PROGRESS_DATASET,
            },
        )

        for dataset_id in DATASETS:
            config = require_orchestration_config(
                dataset_id
            )

            self.assertTrue(
                config.orchestration_ready
            )
            self.assertTrue(
                config.source_api_path.startswith("/")
            )
            self.assertIn(
                ":",
                config.silver_processor,
            )
            self.assertIn(
                ":",
                config.gold_processor,
            )
            self.assertTrue(
                config.gold_table.startswith(
                    "lakehouse.gold."
                )
            )

    def test_learning_route(self):
        config = get_dataset_config(
            LEARNING_OUTCOMES_DATASET
        )

        self.assertEqual(
            config.source_api_path,
            "/api/v1/education/learning-outcomes",
        )
        self.assertEqual(
            config.silver_processor,
            (
                "spark_learning_outcomes_to_silver:"
                "process_learning_outcomes_batch"
            ),
        )
        self.assertEqual(
            config.gold_processor,
            (
                "spark_learning_outcomes_to_gold:"
                "run_learning_outcomes_gold"
            ),
        )
        self.assertEqual(
            config.gold_table,
            (
                "lakehouse.gold."
                "learning_outcomes_metrics"
            ),
        )

    def test_teaching_route(self):
        config = get_dataset_config(
            TEACHING_PROGRESS_DATASET
        )

        self.assertEqual(
            config.source_api_path,
            "/api/v1/education/teaching-progress",
        )
        self.assertEqual(
            config.silver_processor,
            (
                "spark_teaching_progress_to_silver:"
                "process_teaching_progress_batch"
            ),
        )
        self.assertEqual(
            config.gold_processor,
            (
                "spark_teaching_progress_to_gold:"
                "run_teaching_progress_gold"
            ),
        )
        self.assertEqual(
            config.gold_table,
            (
                "lakehouse.gold."
                "teaching_progress_metrics"
            ),
        )

    def test_invalid_dataset_fails_immediately(
        self,
    ):
        with self.assertRaises(ValueError):
            require_orchestration_config(
                "education.not_registered"
            )

    def test_deterministic_batch_id_is_retry_stable(
        self,
    ):
        first = deterministic_batch_id(
            LEARNING_OUTCOMES_DATASET,
            "manual__day7_5_test",
        )
        second = deterministic_batch_id(
            LEARNING_OUTCOMES_DATASET,
            "manual__day7_5_test",
        )

        self.assertEqual(first, second)
        self.assertTrue(
            first.startswith("airflow_api_")
        )
        self.assertEqual(
            len(first),
            len("airflow_api_") + 20,
        )

    def test_batch_identity_changes_by_dataset_and_run(
        self,
    ):
        a = deterministic_batch_id(
            LEARNING_OUTCOMES_DATASET,
            "run-a",
        )
        b = deterministic_batch_id(
            TEACHING_PROGRESS_DATASET,
            "run-a",
        )
        c = deterministic_batch_id(
            LEARNING_OUTCOMES_DATASET,
            "run-b",
        )

        self.assertEqual(
            len({a, b, c}),
            3,
        )

    def test_batch_identity_matches_frozen_hash_contract(
        self,
    ):
        dataset = LEARNING_OUTCOMES_DATASET
        run_id = "manual__frozen"

        expected = (
            "airflow_api_"
            + hashlib.sha256(
                f"{dataset}\n{run_id}".encode(
                    "utf-8"
                )
            ).hexdigest()[:20]
        )

        self.assertEqual(
            deterministic_batch_id(
                dataset,
                run_id,
            ),
            expected,
        )

    def test_exact_eight_commands(self):
        self.assertEqual(
            tuple(COMMANDS),
            (
                "api-preflight",
                "ingest-bronze",
                "validate-bronze",
                "process-silver",
                "validate-silver",
                "process-gold",
                "validate-gold",
                "trino-smoke-test",
            ),
        )

    def test_source_url_uses_registry_path(
        self,
    ):
        config = get_dataset_config(
            TEACHING_PROGRESS_DATASET
        )

        url = source_api_url(config)

        self.assertTrue(
            url.endswith(
                config.source_api_path
            )
        )

    def test_delete_support_is_preserved(self):
        learning = get_dataset_config(
            LEARNING_OUTCOMES_DATASET
        )
        teaching = get_dataset_config(
            TEACHING_PROGRESS_DATASET
        )

        self.assertTrue(
            learning.delete_supported
        )
        self.assertFalse(
            teaching.delete_supported
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
