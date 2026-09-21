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
    """Mirror production checksum compatibility for fixtures."""
    legacy_names = {
        "ma_ban_ghi": "record_id",
        "ma_chuong_trinh": "program_code",
        "ten_chuong_trinh": "program_name",
        "nam_hoc": "academic_year",
        "hoc_ky": "semester",
        "so_sinh_vien": "student_count",
        "so_luot_hoc_phan_dat": "passed_course_count",
        "tong_luot_hoc_phan": "attempted_course_count",
        "tong_diem_gpa": "gpa_point_sum",
        "so_sinh_vien_tinh_gpa": "gpa_student_count",
        "so_sinh_vien_canh_bao": "warning_student_count",
        "so_sinh_vien_nguy_co_nghi_hoc": (
            "dropout_risk_student_count"
        ),
        "so_sinh_vien_dung_tien_do": (
            "on_track_student_count"
        ),
        "so_sinh_vien_danh_gia_tien_do": (
            "progress_evaluated_student_count"
        ),
        "thoi_gian_cap_nhat_nguon": "updated_at",
        "da_xoa": "is_deleted",
    }

    canonical_record = {
        legacy_name: record[current_name]
        for current_name, legacy_name
        in legacy_names.items()
    }

    canonical = json.dumps(
        canonical_record,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )

    return hashlib.sha256(
        canonical.encode("utf-8")
    ).hexdigest()



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
    assert payload["schema_version"] == "1.0"
    assert len(payload["data"]) == 30
    assert payload["pagination"]["returned_records"] == 30


def test_all_preview_checksums_match_canonical_source_records():
    payload, _ = load()
    with PREVIEW.open("r", encoding="utf-8-sig", newline="") as fh:
        preview = {row["ma_ban_ghi"]: row for row in csv.DictReader(fh)}
    assert len(preview) == 30
    for record in payload["data"]:
        assert preview[record["ma_ban_ghi"]]["_record_checksum"] == checksum(record)


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
        lambda p: p["data"][0].pop("ma_ban_ghi"),
        lambda p: p["data"][0].pop("ma_chuong_trinh"),
        lambda p: p["data"][0].pop("nam_hoc"),
        lambda p: p["data"][0].__setitem__("hoc_ky", "2"),
        lambda p: p["data"][0].__setitem__("thoi_gian_cap_nhat_nguon", "not-a-date"),
        lambda p: p["data"][0].__setitem__("so_sinh_vien", "1.284"),
        lambda p: p["data"][0].__setitem__("da_xoa", "true"),
    ],
)
def test_invalid_contract_fails(mutator):
    payload, validator = load()
    broken = deepcopy(payload)
    mutator(broken)
    with pytest.raises(ValidationError):
        validator.validate(broken)
