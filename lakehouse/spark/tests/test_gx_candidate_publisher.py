from __future__ import annotations

import io
import json
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from gx_candidate_publisher import CandidatePublishError, publish_api_candidate
from gx_candidate_state import CandidateAssetIdentity, CandidateState
from gx_universal_gate import SnapshotIdentity


PRIMARY = "lakehouse.silver.teaching_progress"
QUARANTINE = "lakehouse.silver.teaching_progress_quarantine"


class Response(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, *_args):
        self.close()


def candidate_state():
    return CandidateState(
        dataset_id="education.teaching_progress",
        source_kind="api",
        run_id="manual__12",
        candidate_branch="gx_api_silver_0123456789abcdef",
        base_reference="main",
        base_hash="base-1",
        candidate_hash="candidate-1",
        assets=(
            CandidateAssetIdentity(PRIMARY, "snapshot-1"),
            CandidateAssetIdentity(QUARANTINE, "snapshot-q"),
        ),
    )


def gx_result(**approval_overrides):
    approval = {
        "validation_id": "validation-1",
        "data_asset_ref": PRIMARY,
        "validated_branch": "gx_api_silver_0123456789abcdef",
        "validated_snapshot_id": "snapshot-1",
        "validated_commit_hash": "candidate-1",
        "merge_allowed": True,
    }
    approval.update(approval_overrides)
    return {
        "validation_id": approval["validation_id"],
        "merge_allowed": True,
        "approval": approval,
    }


def identity(asset, branch, snapshot, commit):
    return SnapshotIdentity(branch, snapshot, commit)


class CandidatePublisherTest(unittest.TestCase):
    def setUp(self):
        self.route = patch(
            "gx_candidate_publisher.resolve_route",
            return_value=SimpleNamespace(data_asset_ref=PRIMARY),
        )
        self.load = patch(
            "gx_candidate_publisher.load_candidate_state",
            return_value=candidate_state(),
        )
        self.route.start()
        self.load.start()
        self.addCleanup(self.route.stop)
        self.addCleanup(self.load.stop)

    @patch("gx_candidate_publisher.resolve_snapshot_identity")
    def test_exact_approval_merges_with_base_and_candidate_hash_cas(self, resolve):
        resolve.side_effect = (
            identity(PRIMARY, "gx_api_silver_0123456789abcdef", "snapshot-1", "candidate-1"),
            identity(QUARANTINE, "gx_api_silver_0123456789abcdef", "snapshot-q", "candidate-1"),
            identity(PRIMARY, "main", "old-snapshot", "base-1"),
            identity(PRIMARY, "main", "snapshot-1", "published-1"),
            identity(QUARANTINE, "main", "snapshot-q", "published-1"),
        )
        opener = MagicMock(return_value=Response(json.dumps({
            "wasApplied": True,
            "wasSuccessful": True,
            "resultantTargetHash": "published-1",
        }).encode()))

        result = publish_api_candidate(
            dataset_id="education.teaching_progress",
            run_id="manual__12",
            gx_result=gx_result(),
            opener=opener,
        )

        call = opener.call_args.args[0]
        payload = json.loads(call.data)
        self.assertIn("/api/v2/trees/main@base-1/history/merge", call.full_url)
        self.assertEqual(payload["fromHash"], "candidate-1")
        self.assertEqual(payload["fromRefName"], "gx_api_silver_0123456789abcdef")
        self.assertEqual(result["status"], "PUBLISHED")
        self.assertEqual(result["publication_hash"], "published-1")

    @patch("gx_candidate_publisher.resolve_snapshot_identity")
    def test_retry_after_success_is_idempotent(self, resolve):
        resolve.side_effect = (
            identity(PRIMARY, "gx_api_silver_0123456789abcdef", "snapshot-1", "candidate-1"),
            identity(QUARANTINE, "gx_api_silver_0123456789abcdef", "snapshot-q", "candidate-1"),
            identity(PRIMARY, "main", "snapshot-1", "published-1"),
            identity(QUARANTINE, "main", "snapshot-q", "published-1"),
        )
        opener = MagicMock()
        result = publish_api_candidate(
            dataset_id="education.teaching_progress",
            run_id="manual__12",
            gx_result=gx_result(),
            opener=opener,
        )
        self.assertEqual(result["status"], "ALREADY_PUBLISHED")
        opener.assert_not_called()

    @patch("gx_candidate_publisher.resolve_snapshot_identity")
    def test_main_change_blocks_merge(self, resolve):
        resolve.side_effect = (
            identity(PRIMARY, "gx_api_silver_0123456789abcdef", "snapshot-1", "candidate-1"),
            identity(QUARANTINE, "gx_api_silver_0123456789abcdef", "snapshot-q", "candidate-1"),
            identity(PRIMARY, "main", "other-snapshot", "other-main"),
        )
        with self.assertRaisesRegex(CandidatePublishError, "Main changed"):
            publish_api_candidate(
                dataset_id="education.teaching_progress",
                run_id="manual__12",
                gx_result=gx_result(),
            )

    def test_mismatched_approval_is_rejected_before_nessie(self):
        with self.assertRaisesRegex(CandidatePublishError, "does not match"):
            publish_api_candidate(
                dataset_id="education.teaching_progress",
                run_id="manual__12",
                gx_result=gx_result(validated_commit_hash="candidate-other"),
            )


if __name__ == "__main__":
    unittest.main(verbosity=2)
