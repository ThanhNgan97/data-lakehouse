from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import database_candidate_writer as writer
from gx_candidate_state import CandidateAssetIdentity, CandidateState


TEST_ROOT = Path(__file__).resolve().parent


def candidate_state():
    return CandidateState(
        dataset_id="education.teaching_progress",
        source_kind="database",
        run_id="airbyte__42",
        candidate_branch=writer.retry_stable_branch_name(
            "gx_database_silver", "education.teaching_progress", "airbyte__42"
        ),
        base_reference="main",
        base_hash="base-1",
        candidate_hash="candidate-1",
        assets=(
            CandidateAssetIdentity(
                "lakehouse.silver.teaching_progress", "snapshot-1"
            ),
        ),
    )


class DatabaseCandidateWriterTest(unittest.TestCase):
    def files(self, directory: str):
        manifest = Path(directory) / "manifest.json"
        decision = Path(directory) / "decision.json"
        manifest.write_text(
            json.dumps({"entities": {"progress": ["staging/progress.parquet"]}}),
            encoding="utf-8",
        )
        decision.write_text("{}", encoding="utf-8")
        return manifest, decision

    @patch("database_candidate_writer.load_candidate_state")
    @patch("database_candidate_writer.candidate_state_path")
    @patch("database_candidate_writer.get_dataset_config")
    @patch("database_candidate_writer.RoutingDecision")
    def test_retry_replays_immutable_state_without_spark(
        self, routing, config, state_path, load
    ):
        routing.return_value.registered_dataset_id = "education.teaching_progress"
        config.return_value.dataset = "education.teaching_progress"
        load.return_value = candidate_state()
        with tempfile.TemporaryDirectory(dir=TEST_ROOT) as directory:
            manifest, decision = self.files(directory)
            existing = Path(directory) / "candidate.json"
            existing.write_text("{}", encoding="utf-8")
            state_path.return_value = existing
            result = writer.prepare_database_candidate(
                manifest_path=str(manifest),
                decision_file=str(decision),
                run_id="airbyte__42",
            )
        self.assertEqual(result, candidate_state())

    @patch("database_candidate_writer.RoutingDecision")
    def test_unmapped_database_is_not_configured(self, routing):
        routing.return_value.registered_dataset_id = None
        with tempfile.TemporaryDirectory(dir=TEST_ROOT) as directory:
            manifest, decision = self.files(directory)
            with self.assertRaisesRegex(RuntimeError, "NOT_CONFIGURED"):
                writer.prepare_database_candidate(
                    manifest_path=str(manifest),
                    decision_file=str(decision),
                    run_id="airbyte__42",
                )


if __name__ == "__main__":
    unittest.main()
