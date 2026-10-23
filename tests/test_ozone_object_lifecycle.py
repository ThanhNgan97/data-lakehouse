from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys


SPARK_DIR = Path(__file__).resolve().parents[1] / "lakehouse" / "spark"
sys.path.insert(0, str(SPARK_DIR))

from ozone_object_lifecycle import archive_key_for, archive_object, list_expired_objects


class FakePaginator:
    def __init__(self, pages):
        self.pages = pages

    def paginate(self, **_kwargs):
        return self.pages


class FakeS3:
    def __init__(self):
        self.objects = {"staging/manual/id/file.csv": b"payload"}
        self.deleted = []
        self.pages = []

    def head_object(self, *, Bucket, Key):
        del Bucket
        return {"ContentLength": len(self.objects[Key])}

    def copy_object(self, *, Bucket, CopySource, Key):
        del Bucket
        self.objects[Key] = self.objects[CopySource["Key"]]

    def delete_object(self, *, Bucket, Key):
        del Bucket
        self.deleted.append(Key)
        del self.objects[Key]

    def get_paginator(self, name):
        assert name == "list_objects_v2"
        return FakePaginator(self.pages)


def test_archive_key_preserves_source_structure():
    assert archive_key_for("staging/manual/id/file.csv") == (
        "archive/processed/manual/id/file.csv"
    )


def test_archive_verifies_copy_before_deleting_source():
    client = FakeS3()
    result = archive_object(
        client,
        bucket="university-lakehouse",
        source_key="staging/manual/id/file.csv",
    )
    assert result.archive_key == "archive/processed/manual/id/file.csv"
    assert result.content_length == 7
    assert client.deleted == ["staging/manual/id/file.csv"]
    assert client.objects[result.archive_key] == b"payload"


def test_retention_returns_only_expired_objects():
    client = FakeS3()
    now = datetime(2026, 10, 21, tzinfo=timezone.utc)
    client.pages = [
        {
            "Contents": [
                {
                    "Key": "archive/processed/old.csv",
                    "Size": 10,
                    "LastModified": now - timedelta(days=91),
                },
                {
                    "Key": "archive/processed/new.csv",
                    "Size": 20,
                    "LastModified": now - timedelta(days=2),
                },
            ]
        }
    ]
    candidates = list_expired_objects(
        client,
        bucket="university-lakehouse",
        prefix="archive/processed",
        retention_days=90,
        now=now,
    )
    assert [item["key"] for item in candidates] == ["archive/processed/old.csv"]
