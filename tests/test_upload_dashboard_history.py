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
