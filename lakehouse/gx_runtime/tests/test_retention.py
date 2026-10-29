from __future__ import annotations

import os
import io
import json
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

from gx_runtime.errors import ValidationConfigurationError
from gx_runtime.retention import cleanup


class GXRetentionTest(unittest.TestCase):
    def setUp(self):
        self.now = datetime(2026, 10, 28, tzinfo=timezone.utc)
        self.client = MagicMock()
        self.client.get_paginator.return_value.paginate.side_effect = lambda **_kwargs: [
            {
                "Contents": [
                    {"Key": "gx/item-old", "LastModified": self.now - timedelta(days=91)},
                    {"Key": "gx/item-new", "LastModified": self.now - timedelta(days=1)},
                ]
            }
        ]

    def test_dry_run_never_deletes(self):
        report = cleanup(
            self.client, bucket="quality", retention_days=90, dry_run=True, now=self.now
        )
        self.assertGreater(report.eligible, 0)
        self.assertEqual(report.deleted, 0)
        self.client.delete_objects.assert_not_called()

    def test_explicit_cleanup_is_bounded_and_deletes_eligible_objects(self):
        report = cleanup(
            self.client,
            bucket="quality",
            retention_days=90,
            dry_run=False,
            max_delete=2,
            now=self.now,
        )
        self.assertEqual(report.deleted, 2)
        self.assertTrue(report.capped)
        self.client.delete_objects.assert_called_once()

    def test_prefix_escape_is_rejected(self):
        with patch.dict(os.environ, {"GX_RETENTION_PREFIXES": "../other"}), self.assertRaises(
            ValidationConfigurationError
        ):
            cleanup(self.client, bucket="quality", retention_days=90)

    def test_active_quarantine_uses_longer_retention(self):
        self.client.get_paginator.return_value.paginate.side_effect = lambda **kwargs: [
            {
                "Contents": [
                    {
                        "Key": "gx/quarantine/dataset/item.json",
                        "LastModified": self.now - timedelta(days=100),
                    }
                ]
            }
        ] if kwargs["Prefix"] == "gx/quarantine/" else [{"Contents": []}]
        self.client.get_object.return_value = {
            "Body": io.BytesIO(json.dumps({"status": "QUARANTINED"}).encode())
        }
        with patch.dict(os.environ, {"GX_QUARANTINE_ACTIVE_RETENTION_DAYS": "365"}):
            report = cleanup(
                self.client, bucket="quality", retention_days=90, dry_run=False, now=self.now
            )
        self.assertEqual(report.eligible, 0)
        self.client.delete_objects.assert_not_called()


if __name__ == "__main__":
    unittest.main()
