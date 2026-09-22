import ast
import csv
import hashlib
import json
from copy import deepcopy
from datetime import datetime
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator, FormatChecker, ValidationError


ROOT = Path(__file__).resolve().parents[1]

SOURCE_DATA = (
    ROOT
    / "ctu_ioc_mock_api"
    / "app"
    / "data"
    / "learning_outcomes.json"
)

MOCK_MAIN = (
    ROOT
    / "ctu_ioc_mock_api"
    / "app"
    / "main.py"
)

SCHEMA = (
    ROOT
    / "lakehouse"
    / "spark"
    / "contracts"
    / "learning_outcomes.schema.json"
)

REGISTRY = (
    ROOT
    / "lakehouse"
    / "spark"
    / "api_dataset_registry.py"
)

INGESTION = (
    ROOT
    / "lakehouse"
    / "spark"
    / "api_ingestion.py"
)

SILVER = (
    ROOT
    / "lakehouse"
    / "spark"
    / "spark_learning_outcomes_to_silver.py"
)

PREVIEW = (
    ROOT
    / "lakehouse"
    / "spark"
    / "testdata"
    / "bronze_learning_outcomes_demo_preview.csv"
)


EXPECTED_SOURCE_FIELDS = (
    "record_id",
    "program_code",
    "program_name",
    "academic_year",
    "semester",
    "student_count",
    "passed_course_count",
    "attempted_course_count",
    "gpa_point_sum",
    "gpa_student_count",
    "warning_student_count",
    "dropout_risk_student_count",
    "on_track_student_count",
    "progress_evaluated_student_count",
    "updated_at",
    "is_deleted",
)

EXPECTED_SOURCE_TO_CANONICAL = {
    "record_id": "ma_ban_ghi",
    "program_code": "ma_chuong_trinh",
    "program_name": "ten_chuong_trinh",
    "academic_year": "nam_hoc",
    "semester": "hoc_ky",
    "student_count": "so_sinh_vien",
    "passed_course_count": "so_luot_hoc_phan_dat",
    "attempted_course_count": "tong_luot_hoc_phan",
    "gpa_point_sum": "tong_diem_gpa",
    "gpa_student_count": "so_sinh_vien_tinh_gpa",
    "warning_student_count": "so_sinh_vien_canh_bao",
    "dropout_risk_student_count": "so_sinh_vien_nguy_co_nghi_hoc",
    "on_track_student_count": "so_sinh_vien_dung_tien_do",
    "progress_evaluated_student_count": "so_sinh_vien_danh_gia_tien_do",
    "updated_at": "thoi_gian_cap_nhat_nguon",
    "is_deleted": "da_xoa",
}

EXPECTED_METADATA_FIELDS = (
    "_source_system",
    "_source_type",
    "_dataset",
    "_schema_version",
    "_ingestion_mode",
    "_batch_id",
    "_source_updated_at",
    "_ingested_at",
    "_record_checksum",
)

GOLDEN_FIRST_CHECKSUM = (
    "bfe245641d765f19c6249780c59fe3b8"
    "dfc71aa05db13faa253950a4aaedf0a6"
)


def _literal_assignment(path: Path, name: str):
    tree = ast.parse(path.read_text(encoding="utf-8"))

    for node in tree.body:
        if not isinstance(node, (ast.Assign, ast.AnnAssign)):
            continue

        if isinstance(node, ast.Assign):
            targets = node.targets
            value = node.value
        else:
            targets = [node.target]
            value = node.value

        for target in targets:
            if isinstance(target, ast.Name) and target.id == name:
                return ast.literal_eval(value)

    raise AssertionError(f"{name} not found in {path}")


def _load_source_records():
    payload = json.loads(
        SOURCE_DATA.read_text(encoding="utf-8")
    )
    records = payload["data"]

    assert isinstance(records, list)
    return records


def _mock_constant(name: str):
    return _literal_assignment(MOCK_MAIN, name)


def _build_payload():
    records = deepcopy(_load_source_records())

    return {
        "source_system": _mock_constant("SOURCE_SYSTEM"),
        "dataset": _mock_constant("DATASET"),
        "schema_version": _mock_constant("SCHEMA_VERSION"),
        "generated_at": "2026-09-22T00:00:00+07:00",
        "data_as_of": max(
            record["updated_at"]
            for record in records
        ),
        "data": records,
        "pagination": {
            "returned_records": len(records),
            "has_more": False,
            "next_cursor": None,
        },
    }


def _validator():
    schema = json.loads(
        SCHEMA.read_text(encoding="utf-8")
    )

    format_checker = FormatChecker()

    @format_checker.checks(
        "date-time",
        raises=(TypeError, ValueError, AttributeError),
    )
    def strict_iso_datetime(value):
        if not isinstance(value, str):
            return True

        parsed = datetime.fromisoformat(
            value.replace("Z", "+00:00")
        )
        return parsed.tzinfo is not None

    return Draft202012Validator(
        schema,
        format_checker=format_checker,
    )


def checksum(record):
    """Mirror the Day 3 source-native deterministic checksum."""
    canonical = json.dumps(
        record,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )

    return hashlib.sha256(
        canonical.encode("utf-8")
    ).hexdigest()


def test_active_contract_and_count():
    payload = _build_payload()
    _validator().validate(payload)

    assert payload["source_system"] == "ctu_ioc"
    assert payload["dataset"] == "education.learning_outcomes"
    assert payload["schema_version"] == "2.0"
    assert len(payload["data"]) == 30
    assert payload["pagination"]["returned_records"] == 30


def test_source_native_field_set_is_exact_for_baseline():
    records = _load_source_records()

    assert len(records) == 30
    assert all(
        tuple(record.keys()) == EXPECTED_SOURCE_FIELDS
        for record in records
    )
    assert len({
        record["record_id"]
        for record in records
    }) == 30


def test_registry_mapping_is_complete_and_one_to_one():
    source_fields = _literal_assignment(
        REGISTRY,
        "LEARNING_OUTCOMES_FIELDS",
    )
    mapping = _literal_assignment(
        REGISTRY,
        "LEARNING_OUTCOMES_SOURCE_TO_CANONICAL",
    )

    assert source_fields == EXPECTED_SOURCE_FIELDS
    assert mapping == EXPECTED_SOURCE_TO_CANONICAL
    assert set(mapping) == set(EXPECTED_SOURCE_FIELDS)
    assert len(set(mapping.values())) == 16


def test_all_historical_preview_checksums_match_source_native_records():
    records = _load_source_records()

    with PREVIEW.open(
        "r",
        encoding="utf-8-sig",
        newline="",
    ) as fh:
        preview = {
            row["ma_ban_ghi"]: row
            for row in csv.DictReader(fh)
        }

    assert len(preview) == 30

    mismatches = [
        record["record_id"]
        for record in records
        if preview[record["record_id"]]["_record_checksum"]
        != checksum(record)
    ]

    assert mismatches == []


def test_checksum_is_deterministic_and_preserves_golden_identity():
    source = _load_source_records()[0]

    first = checksum(source)
    second = checksum(dict(source))

    assert first == second
    assert first == GOLDEN_FIRST_CHECKSUM


def test_extra_source_field_is_allowed_by_v2_contract():
    payload = _build_payload()
    evolved = deepcopy(payload)

    evolved["data"][0]["future_source_note"] = "day3-probe"

    _validator().validate(evolved)


def test_extra_source_field_is_not_part_of_canonical_mapping():
    mapping = _literal_assignment(
        REGISTRY,
        "LEARNING_OUTCOMES_SOURCE_TO_CANONICAL",
    )

    assert "future_source_note" not in mapping
    assert set(mapping) == set(EXPECTED_SOURCE_FIELDS)


@pytest.mark.parametrize(
    "mutator",
    [
        lambda p: p.pop("data"),
        lambda p: p["data"][0].pop("record_id"),
        lambda p: p["data"][0].pop("program_code"),
        lambda p: p["data"][0].pop("academic_year"),
        lambda p: p["data"][0].__setitem__("semester", "2"),
        lambda p: p["data"][0].__setitem__(
            "updated_at",
            "not-a-date",
        ),
        lambda p: p["data"][0].__setitem__(
            "student_count",
            "1284",
        ),
        lambda p: p["data"][0].__setitem__(
            "is_deleted",
            "true",
        ),
    ],
)
def test_invalid_contract_fails(mutator):
    broken = _build_payload()
    mutator(broken)

    with pytest.raises(ValidationError):
        _validator().validate(broken)


def test_bronze_metadata_contract_remains_nine_fields():
    ingestion_metadata = _literal_assignment(
        INGESTION,
        "METADATA_COLUMNS",
    )
    silver_metadata = _literal_assignment(
        SILVER,
        "BRONZE_METADATA_FIELDS",
    )

    assert ingestion_metadata == EXPECTED_METADATA_FIELDS
    assert silver_metadata == EXPECTED_METADATA_FIELDS


def test_generic_ingestion_has_no_learning_outcomes_canonical_hardcodes():
    text = INGESTION.read_text(encoding="utf-8")

    forbidden = set(
        EXPECTED_SOURCE_TO_CANONICAL.values()
    )

    remaining = sorted(
        field
        for field in forbidden
        if field in text
    )

    assert remaining == []
