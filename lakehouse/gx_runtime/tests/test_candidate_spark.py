from __future__ import annotations

import unittest

from gx_runtime.candidate_spark import CandidateSparkSessions


class FakeConf:
    def __init__(self):
        self.values = {}

    def set(self, key, value):
        self.values[key] = value


class FakeSession:
    def __init__(self):
        self.conf = FakeConf()
        self.children = []
        self.stopped = False

    def newSession(self):
        child = FakeSession()
        self.children.append(child)
        return child

    def stop(self):
        self.stopped = True


class CandidateSparkSessionsTest(unittest.TestCase):
    def test_branch_gets_an_isolated_catalog_alias(self):
        sessions = CandidateSparkSessions()
        base = FakeSession()
        sessions._base = base

        first, first_alias = sessions.for_branch("candidate_1")
        second, second_alias = sessions.for_branch("candidate_2")

        self.assertNotEqual(first_alias, second_alias)
        self.assertEqual(
            first.conf.values[f"spark.sql.catalog.{first_alias}.ref"],
            "candidate_1",
        )
        self.assertEqual(
            second.conf.values[f"spark.sql.catalog.{second_alias}.ref"],
            "candidate_2",
        )

    def test_main_is_rejected(self):
        sessions = CandidateSparkSessions()
        sessions._base = FakeSession()
        with self.assertRaisesRegex(ValueError, "non-main"):
            sessions.for_branch("main")

    def test_close_stops_lazy_base_session(self):
        sessions = CandidateSparkSessions()
        base = FakeSession()
        sessions._base = base
        sessions.close()
        self.assertTrue(base.stopped)
        self.assertIsNone(sessions._base)


if __name__ == "__main__":
    unittest.main(verbosity=2)
