import sys
from pathlib import Path

import pytest


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "backend"))

from services.url_ingestion.exceptions import InvalidUrlError, UnsupportedProviderError
from services.url_ingestion.resolver import resolve_url
from services.url_ingestion.state import derive_job_status


def test_resolver_detects_direct_http_file():
    source = resolve_url("https://example.com/report.pdf")
    assert (source.provider, source.resource_type) == ("HTTP", "FILE")


def test_resolver_detects_google_drive_file_and_folder():
    drive_file = resolve_url("https://drive.google.com/file/d/file_123/view")
    drive_folder = resolve_url("https://drive.google.com/drive/u/0/folders/folder_456")
    assert (drive_file.provider, drive_file.resource_type, drive_file.resource_id) == (
        "GOOGLE_DRIVE", "FILE", "file_123",
    )
    assert (drive_folder.provider, drive_folder.resource_type, drive_folder.resource_id) == (
        "GOOGLE_DRIVE", "FOLDER", "folder_456",
    )


def test_resolver_rejects_local_path_and_unrecognized_drive_url():
    with pytest.raises(InvalidUrlError):
        resolve_url(r"D:\data\report.pdf")
    with pytest.raises(UnsupportedProviderError):
        resolve_url("https://drive.google.com/drive/my-drive")


@pytest.mark.parametrize(
    ("states", "cancelled", "expected"),
    [
        (["QUEUED"], False, "PROCESSING"),
        (["COMPLETED", "DUPLICATE"], False, "COMPLETED"),
        (["COMPLETED", "FAILED"], False, "PARTIAL_SUCCESS"),
        (["UNSUPPORTED", "FAILED"], False, "FAILED"),
        (["CANCELLED", "CANCELLED"], True, "CANCELLED"),
    ],
)
def test_job_status_truth_table(states, cancelled, expected):
    assert derive_job_status(states, cancelled) == expected
