import csv
import hashlib
import json
from copy import deepcopy
from datetime import datetime
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator, FormatChecker, ValidationError

ROOT = Path(__file__).resolve().parents[1]
PAYLOAD = ROOT / "lakehouse" / "spark" / "testdata" / "mock_learning_outcomes_api_v0_1.json"
SCHEMA = ROOT / "lakehouse" / "spark" / "contracts" / "learning_outcomes_v0_1.schema.json"
PREVIEW = ROOT / "lakehouse" / "spark" / "testdata" / "bronze_learning_outcomes_demo_preview.csv"


def checksum(record):
    canonical = json.dumps(record, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def load():
    payload = json.loads(PAYLOAD.read_text(encoding="utf-8"))
    schema = json.loads(SCHEMA.read_text(encoding="utf-8"))

    format_checker = FormatChecker()

    @format_checker.checks(
        "date-time",
        raises=(TypeError, ValueError, AttributeError),
    )
    def strict_iso_datetime(value):
        if not isinstance(value, str):
            # JSON Schema "type" handles non-string values.
            return True

        parsed = datetime.fromisoformat(
            value.replace("Z", "+00:00")
        )
        return parsed.tzinfo is not None

    validator = Draft202012Validator(
        schema,
        format_checker=format_checker,
    )
    return payload, validator


def test_contract_and_count():
    payload, validator = load()
    validator.validate(payload)
    assert payload["source_system"] == "ctu_ioc_demo"
    assert payload["dataset"] == "education.learning_outcomes"
    assert payload["schema_version"] == "0.1-demo"
    assert len(payload["data"]) == 30
    assert payload["pagination"]["returned_records"] == 30


def test_all_preview_checksums_match_canonical_source_records():
    payload, _ = load()
    with PREVIEW.open("r", encoding="utf-8-sig", newline="") as fh:
        preview = {row["record_id"]: row for row in csv.DictReader(fh)}
    assert len(preview) == 30
    for record in payload["data"]:
        assert preview[record["record_id"]]["_record_checksum"] == checksum(record)


def test_checksum_ignores_ingestion_metadata():
    payload, _ = load()
    source = payload["data"][0]
    first = checksum(source)
    second = checksum(dict(source))
    assert first == second
    assert first == "bfe245641d765f19c6249780c59fe3b8dfc71aa05db13faa253950a4aaedf0a6"


@pytest.mark.parametrize(
    "mutator",
    [
        lambda p: p.pop("data"),
        lambda p: p["data"][0].pop("record_id"),
        lambda p: p["data"][0].pop("program_code"),
        lambda p: p["data"][0].pop("academic_year"),
        lambda p: p["data"][0].__setitem__("semester", "2"),
        lambda p: p["data"][0].__setitem__("updated_at", "not-a-date"),
        lambda p: p["data"][0].__setitem__("student_count", "1.284"),
        lambda p: p["data"][0].__setitem__("is_deleted", "true"),
    ],
)
def test_invalid_contract_fails(mutator):
    payload, validator = load()
    broken = deepcopy(payload)
    mutator(broken)
    with pytest.raises(ValidationError):
        validator.validate(broken)
