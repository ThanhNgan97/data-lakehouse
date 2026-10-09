import sys
from pathlib import Path
from types import SimpleNamespace

from fastapi import Response

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "backend"))

from api.routes.api_sources import SaveApiSourceRequest, _persisted_metadata, save_api_source
from core.security import decrypt_secret, encrypt_secret
from db.models import ApiSource


def test_api_source_metadata_is_normalized_for_persistence():
    request = SaveApiSourceRequest(
        name="Learning Outcome",
        dataset_id="education.learning_outcomes",
        url="https://example.test/api",
        auth_type="none",
        metadata_info={
            "schema_version": "1.2",
            "source_system": "CTU IOC",
            "data_as_of": "2026-10-09",
            "pagination": {"page": 1, "limit": 100},
            "data": [{"must": "not be persisted"}],
        },
    )

    assert _persisted_metadata(request) == {
        "dataset_id": "education.learning_outcomes",
        "schema_version": "1.2",
        "source_system": "CTU IOC",
        "data_as_of": "2026-10-09",
        "pagination": {"page": 1, "limit": 100},
    }


def test_api_source_credential_is_encrypted_and_not_serialized():
    plaintext = "private-bearer-token"
    ciphertext = encrypt_secret(plaintext)
    source = ApiSource(
        id=1,
        name="Learning Outcome",
        dataset_id="education.learning_outcomes",
        url="https://example.test/api",
        auth_type="bearer",
        credential_ciphertext=ciphertext,
    )

    payload = source.to_dict()

    assert ciphertext != plaintext
    assert decrypt_secret(ciphertext) == plaintext
    assert payload["credential_configured"] is True
    assert "credential_ciphertext" not in payload


class _FakeQuery:
    def __init__(self, result):
        self.result = result

    def filter(self, *conditions):
        return self

    def first(self):
        return self.result


class _FakeSession:
    def __init__(self, existing=None):
        self.existing = existing
        self.added = []
        self.committed = False

    def query(self, model):
        return _FakeQuery(self.existing)

    def add(self, value):
        self.added.append(value)

    def commit(self):
        self.committed = True

    def refresh(self, value):
        value.id = value.id or 10


def test_saving_the_same_url_updates_metadata_and_preserves_stored_credential():
    ciphertext = encrypt_secret("existing-token")
    source = ApiSource(
        id=4,
        user_id=7,
        name="Old name",
        dataset_id="education.learning_outcomes",
        url="https://example.test/api",
        auth_type="bearer",
        credential_ciphertext=ciphertext,
        metadata_info={"schema_version": "1.0"},
    )
    db = _FakeSession(existing=source)
    response = Response(status_code=201)
    request = SaveApiSourceRequest(
        name="New name",
        dataset_id="education.learning_outcomes",
        url="https://example.test/api",
        auth_type="bearer",
        metadata_info={"schema_version": "2.0"},
    )

    result = save_api_source(
        request=request,
        response=response,
        current_user=SimpleNamespace(id=7),
        db=db,
    )

    assert response.status_code == 200
    assert db.committed is True
    assert db.added == []
    assert source.name == "New name"
    assert source.metadata_info["schema_version"] == "2.0"
    assert source.credential_ciphertext == ciphertext
    assert result["credential_configured"] is True

