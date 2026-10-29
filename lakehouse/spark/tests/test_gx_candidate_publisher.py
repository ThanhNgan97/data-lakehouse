from __future__ import annotations

import io
import json
import unittest
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from gx_candidate_publisher import (
    CandidatePublishError,
    _delete_candidate_branch,
    publish_api_candidate,
    publish_file_candidate,
)
from gx_candidate_state import CandidateAssetIdentity, CandidateState
from gx_approval_receipt import sign_approval_receipt
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
    approval = sign_approval_receipt({
        "validation_id": "validation-1",
        "dataset_id": "education.teaching_progress",
        "data_layer": "silver",
        "contract_version": "1.0.0",
        "run_id": "manual__12",
        "source_kind": "api",
        "audience": "silver-publisher",
        "data_asset_ref": PRIMARY,
        "validated_branch": "gx_api_silver_0123456789abcdef",
        "validated_snapshot_id": "snapshot-1",
        "validated_commit_hash": "candidate-1",
        "candidate_hash": "candidate-1",
    })
    if approval_overrides:
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
        self.secret = patch.dict(
            "os.environ", {
                "GX_APPROVAL_SIGNING_KEY_ID": "test-key",
                "GX_APPROVAL_PRIVATE_KEY": "mhhz/j9TRR3mbxkFcd8Alqxr7N4NBZUtj6gXGn7xt+Y=",
                "GX_APPROVAL_PUBLIC_KEYS": '{"test-key":"7VfexdIjq7grSVgW/4x+kP4d1wg+296V+txcBCOhHOA="}',
            }
        )
        self.secret.start()
        self.addCleanup(self.secret.stop)
        self.route = patch(
            "gx_candidate_publisher.resolve_route",
            return_value=SimpleNamespace(
                data_asset_ref=PRIMARY, contract_version="1.0.0"
            ),
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
            identity(PRIMARY, "main", "old-snapshot", "base-1"),
            identity(PRIMARY, "gx_api_silver_0123456789abcdef", "snapshot-1", "candidate-1"),
            identity(QUARANTINE, "gx_api_silver_0123456789abcdef", "snapshot-q", "candidate-1"),
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
    def test_file_source_uses_the_same_guarded_publisher(self, resolve):
        file_state = replace(candidate_state(), source_kind="file")
        resolve.side_effect = (
            identity(PRIMARY, "main", "snapshot-1", "published-1"),
            identity(QUARANTINE, "main", "snapshot-q", "published-1"),
        )
        with patch(
            "gx_candidate_publisher.load_candidate_state", return_value=file_state
        ):
            result = publish_file_candidate(
                dataset_id=file_state.dataset_id,
                run_id=file_state.run_id,
                gx_result={
                    **gx_result(),
                    "approval": sign_approval_receipt({
                        "validation_id": "validation-1",
                        "dataset_id": file_state.dataset_id,
                        "data_layer": "silver",
                        "contract_version": "1.0.0",
                        "run_id": file_state.run_id,
                        "source_kind": "file",
                        "audience": "silver-publisher",
                        "data_asset_ref": PRIMARY,
                        "validated_branch": file_state.candidate_branch,
                        "validated_snapshot_id": "snapshot-1",
                        "validated_commit_hash": file_state.candidate_hash,
                        "candidate_hash": file_state.candidate_hash,
                    }),
                },
                opener=MagicMock(),
            )
        self.assertEqual(result["status"], "ALREADY_PUBLISHED")

    @patch("gx_candidate_publisher.resolve_snapshot_identity")
    def test_main_change_blocks_merge(self, resolve):
        resolve.side_effect = (
            identity(PRIMARY, "main", "other-snapshot", "other-main"),
        )
        with self.assertRaisesRegex(CandidatePublishError, "Main changed"):
            publish_api_candidate(
                dataset_id="education.teaching_progress",
                run_id="manual__12",
                gx_result=gx_result(),
            )

    def test_mismatched_approval_is_rejected_before_nessie(self):
        with self.assertRaisesRegex(CandidatePublishError, "signature is invalid"):
            publish_api_candidate(
                dataset_id="education.teaching_progress",
                run_id="manual__12",
                gx_result=gx_result(validated_commit_hash="candidate-other"),
            )

    def test_published_candidate_cleanup_uses_expected_hash(self):
        opener = MagicMock(return_value=Response(b"{}"))
        with patch.dict(
            "os.environ", {"GX_DELETE_PUBLISHED_CANDIDATE_BRANCH": "true"}
        ):
            _delete_candidate_branch(candidate_state(), opener=opener)
        call = opener.call_args.args[0]
        self.assertEqual(call.get_method(), "DELETE")
        self.assertIn(
            "/trees/branch/gx_api_silver_0123456789abcdef/candidate-1",
            call.full_url,
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
