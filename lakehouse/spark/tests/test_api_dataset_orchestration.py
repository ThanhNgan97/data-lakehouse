import hashlib
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from api_dataset_registry import (
    DATASETS,
    LEARNING_OUTCOMES_DATASET,
    TEACHING_PROGRESS_DATASET,
    get_dataset_config,
)
from api_dataset_orchestration import (
    COMMANDS,
    command_prepare_silver_candidate,
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
            self.assertTrue(config.dbt_selector)
            self.assertNotIn(":", config.dbt_selector)
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
            config.dbt_selector,
            "agg_learning_outcomes_by_period",
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
            config.dbt_selector,
            "fct_teaching_progress",
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

    def test_exact_eleven_commands(self):
        self.assertEqual(
            tuple(COMMANDS),
            (
                "api-preflight",
                "ingest-bronze",
                "validate-bronze",
                "process-silver",
                "prepare-silver-candidate",
                "validate-silver",
                "prepare-gold",
                "process-gold",
                "validate-gold",
                "publish-gold",
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

    @patch("api_dataset_orchestration.save_candidate_state")
    @patch("api_dataset_orchestration.resolve_snapshot_identity")
    @patch("api_dataset_orchestration.get_nessie_reference_hash")
    @patch("api_dataset_orchestration._latest_committed_batch_id")
    @patch("api_dataset_orchestration._load_callable")
    @patch("api_dataset_orchestration.get_spark_session")
    def test_prepare_candidate_does_not_merge_and_persists_exact_identity(
        self,
        get_spark,
        load_callable,
        latest_batch,
        reference_hash,
        resolve_identity,
        save_state,
    ):
        config = get_dataset_config(TEACHING_PROGRESS_DATASET)
        spark = MagicMock()
        get_spark.return_value = spark
        latest_batch.return_value = "batch-7"
        processor = MagicMock(return_value={"branch_silver_count": 4})
        load_callable.return_value = processor
        reference_hash.side_effect = (None, "base-hash")
        resolve_identity.side_effect = (
            SimpleNamespace(snapshot_id="silver-7", commit_hash="candidate-hash"),
            SimpleNamespace(snapshot_id="quarantine-7", commit_hash="candidate-hash"),
        )
        save_state.return_value = "/state/candidate.json"

        command_prepare_silver_candidate(config, "manual__7")

        processor.assert_called_once()
        arguments = processor.call_args.kwargs
        self.assertFalse(arguments["merge_to_main"])
        self.assertTrue(arguments["candidate_branch"].startswith("gx_api_silver_"))
        saved = save_state.call_args.args[0]
        self.assertEqual(saved.base_hash, "base-hash")
        self.assertEqual(saved.candidate_hash, "candidate-hash")
        self.assertEqual(len(saved.assets), 2)
        self.assertEqual(saved.assets[0].snapshot_id, "silver-7")


if __name__ == "__main__":
    unittest.main(verbosity=2)
