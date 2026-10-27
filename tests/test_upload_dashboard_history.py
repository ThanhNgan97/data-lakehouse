import json
import sys
from pathlib import Path
from types import SimpleNamespace


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "backend"))

from api.routes import upload


def test_dashboard_xcom_is_normalized(monkeypatch):
    class Response:
        status_code = 200

        @staticmethod
        def json():
            return {
                "value": (
                    "{'dashboard_slug': 'auto-iot-summary', "
                    "'dashboard_title': 'IoT Summary', "
                    "'dashboard_uuid': 'dashboard-uuid', 'imported': True}"
                )
            }

    monkeypatch.setattr(upload.requests, "get", lambda *args, **kwargs: Response())

    assert upload._dashboard_from_xcom("dag-run") == {
        "slug": "auto-iot-summary",
        "title": "IoT Summary",
        "uuid": "dashboard-uuid",
        "imported": True,
    }


def test_dashboard_metadata_round_trips_through_history_record():
    record = SimpleNamespace(
        dashboard_slug=None,
        dashboard_title=None,
        dashboard_uuid=None,
        dashboard_status="pending",
    )

    upload._save_dashboard(record, {
        "slug": "auto-train-summary",
        "title": "Train Summary",
        "uuid": "train-uuid",
        "imported": True,
    })

    assert upload._stored_dashboard(record) == {
        "slug": "auto-train-summary",
        "title": "Train Summary",
        "uuid": "train-uuid",
        "imported": True,
        "status": "ready",
    }


def test_database_pipeline_run_id_is_strictly_scoped_to_airbyte_jobs():
    assert upload._is_airbyte_pipeline_run("airbyte__78") is True
    assert upload._is_airbyte_pipeline_run("airbyte__78/../other") is False
    assert upload._is_airbyte_pipeline_run("manual__78") is False


def test_gx_xcom_is_bounded_and_normalized(monkeypatch):
    class Response:
        status_code = 200

        @staticmethod
        def json():
            return {
                "value": json.dumps({
                    "status": "PASSED",
                    "validation_id": "validation-1",
                    "validation_uri": "s3://bucket/result.json",
                    "merge_allowed": True,
                    "dataset_id": "education.teaching_progress",
                    "source_kind": "file",
                    "raw_rows": ["must-not-leak"],
                })
            }

    monkeypatch.setattr(upload.requests, "get", lambda *args, **kwargs: Response())
    result = upload._gx_from_xcom("dag-run")
    assert result["status"] == "PASSED"
    assert result["merge_allowed"] is True
    assert "raw_rows" not in result


def test_gx_task_state_never_claims_pass_without_xcom():
    assert upload._gx_task_state([
        {"task_id": "gx_validate_file_upload", "state": "success"}
    ]) == {"status": "RESULT_UNAVAILABLE"}
    assert upload._gx_task_state([]) == {"status": "NOT_AVAILABLE"}
