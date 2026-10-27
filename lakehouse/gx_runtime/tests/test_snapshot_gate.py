from __future__ import annotations

import io
import json
import unittest

from gx_runtime.errors import QualityGateRejectedError, SnapshotMismatchError
from gx_runtime.models import (
    EngineValidationResult,
    ExecutionEngine,
    ExecutionMode,
    SuiteReference,
    ValidationRequest,
)
from gx_runtime.policy import ValidationPolicy
from gx_runtime.snapshot_gate import (
    NessieSnapshotResolver,
    SnapshotAwareQualityGate,
    SnapshotIdentity,
)


def summary(*, executed=True, merge_allowed=True):
    request = ValidationRequest(
        data_asset_ref="lakehouse.silver.teaching_progress",
        dataset_id="education.teaching_progress",
        data_layer="silver",
        execution_engine=ExecutionEngine.SQL,
        contract_version="1.0.0",
        suites=(SuiteReference("teaching_progress_silver_core", "1.0.0"),),
        run_id="phase5-gate-test",
        execution_mode=(ExecutionMode.ENFORCE if executed else ExecutionMode.OFF),
        nessie_branch="candidate_phase5",
        snapshot_id="123456",
        nessie_commit_hash="abc123",
    )
    result = None
    if executed:
        result = EngineValidationResult(
            success=merge_allowed,
            evaluated_expectations=1,
            successful_expectations=1 if merge_allowed else 0,
        )
    generated = ValidationPolicy().apply(request, result)
    if generated.merge_allowed != merge_allowed:
        raise AssertionError("fixture policy mismatch")
    return generated


def identity(**overrides):
    values = {
        "data_asset_ref": "lakehouse.silver.teaching_progress",
        "nessie_branch": "candidate_phase5",
        "snapshot_id": "123456",
        "nessie_commit_hash": "abc123",
    }
    values.update(overrides)
    return SnapshotIdentity(**values)


class SnapshotAwareQualityGateTest(unittest.TestCase):
    def test_exact_identity_returns_publish_approval_receipt(self):
        approval = SnapshotAwareQualityGate.verify(summary(), identity())

        self.assertTrue(approval.merge_allowed)
        self.assertEqual(approval.validated_snapshot_id, "123456")
        self.assertEqual(approval.validated_commit_hash, "abc123")
        self.assertEqual(approval.validation_id, summary().validation_id)

    def test_snapshot_change_invalidates_old_validation(self):
        with self.assertRaisesRegex(SnapshotMismatchError, "SNAPSHOT_MISMATCH"):
            SnapshotAwareQualityGate.verify(
                summary(), identity(snapshot_id="123457")
            )

    def test_commit_change_invalidates_even_unchanged_table_snapshot(self):
        with self.assertRaisesRegex(SnapshotMismatchError, "nessie_commit_hash"):
            SnapshotAwareQualityGate.verify(
                summary(), identity(nessie_commit_hash="def456")
            )

    def test_branch_and_asset_changes_are_rejected(self):
        for changed in (
            {"nessie_branch": "candidate_other"},
            {"data_asset_ref": "lakehouse.silver.other_table"},
        ):
            with self.subTest(changed=changed), self.assertRaises(
                SnapshotMismatchError
            ):
                SnapshotAwareQualityGate.verify(summary(), identity(**changed))

    def test_off_mode_cannot_authorize_publish(self):
        with self.assertRaisesRegex(QualityGateRejectedError, "not executed"):
            SnapshotAwareQualityGate.verify(summary(executed=False), identity())

    def test_failed_quality_policy_cannot_authorize_publish(self):
        with self.assertRaisesRegex(QualityGateRejectedError, "does not allow"):
            SnapshotAwareQualityGate.verify(
                summary(merge_allowed=False), identity()
            )

    def test_summary_serializes_explicit_validated_identity(self):
        payload = summary().as_dict()

        self.assertEqual(payload["layer"], "silver")
        self.assertEqual(payload["validated_branch"], "candidate_phase5")
        self.assertEqual(payload["validated_snapshot_id"], "123456")
        self.assertEqual(payload["validated_commit_hash"], "abc123")
        self.assertEqual(payload["critical_failure_count"], 0)


class FakeResponse(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, *_args):
        self.close()
        return False


class NessieSnapshotResolverTest(unittest.TestCase):
    def test_content_is_resolved_at_the_observed_branch_hash(self):
        urls = []
        payloads = iter(
            (
                {"type": "BRANCH", "name": "candidate a", "hash": "abc123"},
                {"type": "ICEBERG_TABLE", "snapshotId": 123456},
            )
        )

        def open_fixture(url, **_kwargs):
            urls.append(url)
            return FakeResponse(json.dumps(next(payloads)).encode("utf-8"))

        resolved = NessieSnapshotResolver(
            "http://nessie:19120/api/v1", opener=open_fixture
        ).resolve("lakehouse.silver.teaching_progress", "candidate a")

        self.assertEqual(resolved, identity(nessie_branch="candidate a"))
        self.assertIn("/trees/tree/candidate%20a", urls[0])
        self.assertIn("/contents/silver.teaching_progress?", urls[1])
        self.assertIn("ref=candidate+a", urls[1])
        self.assertIn("hashOnRef=abc123", urls[1])


if __name__ == "__main__":
    unittest.main()
