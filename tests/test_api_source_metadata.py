import sys
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi import HTTPException, Response

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "backend"))

from api.routes import api_sources
from api.routes.api_sources import (
    ConfigureApiCredentialRequest,
    LoginAuthenticationRequest,
    SaveApiSourceRequest,
    _extract_token,
    _bounded_gx_evidence,
    _parse_airflow_xcom,
    _login_for_token,
    _persisted_metadata,
    configure_api_source_credentials,
    save_api_source,
)
from core.security import decrypt_secret, encrypt_secret
from db.models import ApiSource


def test_gx_evidence_is_flattened_and_bounded_for_portal():
    value = _parse_airflow_xcom('{"status":"PASSED","validation_id":"v-1",'
        '"merge_allowed":true,"raw_rows":[1,2],"approval":{'
        '"data_asset_ref":"lakehouse.silver.teaching_progress",'
        '"validated_branch":"main","validated_snapshot_id":"42",'
        '"validated_commit_hash":"commit-42","secret":"drop"}}')
    evidence = _bounded_gx_evidence("gx_validate_silver", "Silver", value)

    assert evidence["status"] == "PASSED"
    assert evidence["snapshot_id"] == "42"
    assert evidence["nessie_commit_hash"] == "commit-42"
    assert "raw_rows" not in evidence
    assert "approval" not in evidence
    assert "secret" not in str(evidence)


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


def test_nested_token_field_is_supported():
    assert _extract_token({"data": {"access_token": " token-value "}}, "data.access_token") == "token-value"
    assert _extract_token({"access_token": "token-value"}, "data.access_token") is None


def test_upstream_login_rejection_does_not_look_like_lakehouse_session_expiry(monkeypatch):
    upstream_response = SimpleNamespace(status_code=401)
    monkeypatch.setattr(api_sources.requests, "post", lambda *args, **kwargs: upstream_response)
    request = LoginAuthenticationRequest(
        login_url="https://example.test/auth/login",
        username="api-user",
        password="wrong-password",
    )

    with pytest.raises(HTTPException) as raised:
        _login_for_token(request)

    assert raised.value.status_code == 422
    assert "API đăng nhập từ chối xác thực" in raised.value.detail


def test_login_reauthentication_stores_token_but_never_password(monkeypatch):
    source = ApiSource(
        id=8,
        user_id=7,
        name="Teaching Progress",
        dataset_id="education.teaching_progress",
        url="https://example.test/api",
        auth_type="login",
    )
    db = _FakeSession(existing=source)
    login = LoginAuthenticationRequest(
        login_url="https://example.test/auth/login",
        username="api-user",
        password="never-store-this",
        token_field="data.token",
    )
    request = ConfigureApiCredentialRequest(auth_type="login", login=login)
    monkeypatch.setattr(api_sources, "_login_for_token", lambda value: "fresh-token")

    result = configure_api_source_credentials(
        source_id=8,
        request=request,
        current_user=SimpleNamespace(id=7),
        db=db,
    )

    assert db.committed is True
    assert decrypt_secret(source.credential_ciphertext) == "fresh-token"
    assert source.auth_config["username"] == "api-user"
    assert "password" not in source.auth_config
    assert "never-store-this" not in str(result)

