from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from gx_candidate_state import (
    CandidateAssetIdentity,
    CandidateState,
    CandidateStateError,
    candidate_state_path,
    load_candidate_state,
    retry_stable_branch_name,
    save_candidate_state,
)

TEST_DIRECTORY = Path(__file__).resolve().parent


def state(**overrides):
    values = {
        "dataset_id": "education.teaching_progress",
        "source_kind": "file",
        "run_id": "manual__42",
        "candidate_branch": "gx_file_0123456789abcdef",
        "base_reference": "main",
        "base_hash": "base-123",
        "candidate_hash": "candidate-456",
        "assets": (
            CandidateAssetIdentity(
                "lakehouse.silver.teaching_progress", "snapshot-7"
            ),
        ),
    }
    values.update(overrides)
    return CandidateState(**values)


class CandidateStateTest(unittest.TestCase):
    def test_retry_uses_the_same_branch_but_another_run_does_not(self):
        first = retry_stable_branch_name(
            "gx_file", "education.teaching_progress", "manual__42"
        )
        retry = retry_stable_branch_name(
            "gx_file", "education.teaching_progress", "manual__42"
        )
        other = retry_stable_branch_name(
            "gx_file", "education.teaching_progress", "manual__43"
        )
        self.assertEqual(first, retry)
        self.assertNotEqual(first, other)

    def test_round_trip_preserves_exact_candidate_identity(self):
        with tempfile.TemporaryDirectory(dir=TEST_DIRECTORY) as folder:
            path = candidate_state_path(
                folder, "education.teaching_progress", "manual__42"
            )
            save_candidate_state(state(), path)
            restored = load_candidate_state(path)

        self.assertEqual(restored, state())
        self.assertEqual(restored.asset(restored.assets[0].data_asset_ref).snapshot_id, "snapshot-7")

    def test_retry_can_replay_but_cannot_replace_candidate_identity(self):
        with tempfile.TemporaryDirectory(dir=TEST_DIRECTORY) as folder:
            path = Path(folder) / "candidate.json"
            save_candidate_state(state(), path)
            save_candidate_state(state(), path)
            with self.assertRaisesRegex(CandidateStateError, "different identity"):
                save_candidate_state(state(candidate_hash="candidate-789"), path)

    def test_state_path_does_not_embed_untrusted_identifiers(self):
        with tempfile.TemporaryDirectory(dir=TEST_DIRECTORY) as folder:
            root = Path(folder).resolve()
            path = candidate_state_path(root, "../education", "../../run")
            self.assertEqual(path.parents[1], root)
            self.assertNotIn("education", str(path.relative_to(root)))

    def test_main_branch_empty_assets_and_duplicates_are_rejected(self):
        duplicate = CandidateAssetIdentity(
            "lakehouse.silver.teaching_progress", "snapshot-8"
        )
        invalid = (
            {"candidate_branch": "main"},
            {"assets": ()},
            {"assets": state().assets + (duplicate,)},
        )
        for overrides in invalid:
            with self.subTest(overrides=overrides), self.assertRaises(CandidateStateError):
                state(**overrides)

    def test_unsafe_asset_is_rejected(self):
        with self.assertRaises(CandidateStateError):
            CandidateAssetIdentity("lakehouse.silver.x; DROP TABLE y", "7")


if __name__ == "__main__":
    unittest.main(verbosity=2)
