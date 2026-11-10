import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import worker


class WorkerRetryTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.payload = dict(dataset="education.learning_outcomes", run_id="retry-test", expected_head="a" * 64)
        self.job, ref, _ = worker.identity(self.payload["dataset"], self.payload["run_id"])
        self.state = dict(job_id=self.job, ref=ref, status="failed", **self.payload,
                          error="dbt build failed; inspect private worker build.log")
        for name, value in (("ROOT", self.root), ("STATES", {})):
            patcher = patch.object(worker, name, value)
            patcher.start()
            self.addCleanup(patcher.stop)
        worker.save(self.job, self.state)
        (self.root / self.job / "build.log").write_text("Failed to resolve 'trino': Temporary failure in name resolution")

    def test_dns_retry_preserves_evidence_and_same_identity(self):
        response = Mock()
        response.json.return_value = {"hash": self.payload["expected_head"]}
        with patch.object(worker.requests, "get", return_value=response), patch.object(worker.POOL, "submit") as submit:
            worker.submit_job(self.payload)
            worker.submit_job(self.payload)
        submit.assert_called_once_with(worker.execute, self.payload)
        self.assertEqual(worker.read(self.job)["attempt"], 2)
        self.assertTrue((self.root / self.job / "build.attempt-1.log").exists())
        self.assertTrue((self.root / self.job / "status.attempt-1.json").exists())

    def test_changed_candidate_never_requeues(self):
        response = Mock()
        response.json.return_value = {"hash": "changed"}
        with patch.object(worker.requests, "get", return_value=response), patch.object(worker.POOL, "submit") as submit:
            with self.assertRaisesRegex(ValueError, "Candidate changed"):
                worker.submit_job(self.payload)
        submit.assert_not_called()

    def test_sql_test_failure_cleanup_failure_and_retry_limit_are_refused(self):
        cases = [dict(error="dbt SQL test failed"), dict(catalog_cleanup_failed=True),
                 dict(connection_cleanup_failed=True), dict(attempt=3)]
        for changes in cases:
            with self.subTest(changes=changes):
                worker.save(self.job, dict(self.state, **changes))
                with patch.object(worker.POOL, "submit") as submit, self.assertRaises(ValueError):
                    worker.submit_job(self.payload)
                submit.assert_not_called()

    def test_changed_payload_is_refused(self):
        with self.assertRaisesRegex(ValueError, "identity/expected head"):
            worker.submit_job(dict(self.payload, expected_head="b" * 64))

    def test_passed_job_never_rebuilds(self):
        worker.save(self.job, dict(self.state, status="passed"))
        with patch.object(worker.POOL, "submit") as submit:
            worker.submit_job(self.payload)
        submit.assert_not_called()


if __name__ == "__main__":
    unittest.main()
