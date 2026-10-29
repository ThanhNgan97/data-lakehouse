from __future__ import annotations

import unittest
from unittest.mock import patch

import generic_candidate_writer as writer


class GenericCandidateWriterTest(unittest.TestCase):
    @patch("generic_candidate_writer._prepare_file_candidate", return_value="file-state")
    def test_registered_file_dispatches_to_file_candidate(self, prepare):
        result = writer.prepare_generic_candidate(
            source_kind="file", input_path="input.csv", manifest_path=None,
            decision_file="decision.json", run_id="manual__1"
        )
        self.assertEqual(result, "file-state")
        prepare.assert_called_once()

    @patch("generic_candidate_writer._prepare_database_candidate", return_value="db-state")
    def test_database_dispatches_to_database_candidate(self, prepare):
        result = writer.prepare_generic_candidate(
            source_kind="database", input_path=None, manifest_path="manifest.json",
            decision_file="decision.json", run_id="manual__2"
        )
        self.assertEqual(result, "db-state")
        prepare.assert_called_once()

    def test_unsupported_source_fails_not_configured(self):
        with self.assertRaisesRegex(RuntimeError, "NOT_CONFIGURED"):
            writer.prepare_generic_candidate(
                source_kind="streaming", input_path=None, manifest_path=None,
                decision_file="decision.json", run_id="manual__3"
            )


if __name__ == "__main__":
    unittest.main()
