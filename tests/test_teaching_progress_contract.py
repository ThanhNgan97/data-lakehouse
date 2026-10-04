import ast
import json
from copy import deepcopy
from datetime import datetime
from pathlib import Path

import pytest
from jsonschema import (
    Draft202012Validator,
    FormatChecker,
    ValidationError,
)


ROOT = Path(__file__).resolve().parents[1]

SOURCE_DATA = (
    ROOT
    / "ctu_ioc_mock_api"
    / "app"
    / "data"
    / "teaching_progress.json"
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
    / "teaching_progress.schema.json"
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

GENERIC_SILVER_DIR = (
    ROOT
    / "lakehouse"
    / "spark"
)

EXPECTED_SOURCE_FIELDS = (
    "record_id",
    "unit_code",
    "unit_name",
    "course_section_code",
    "academic_year",
    "semester",
    "progress_percent",
    "updated_at",
)

EXPECTED_MAPPING = {
    "record_id": "ma_ban_ghi",
    "unit_code": "ma_don_vi",
    "unit_name": "ten_don_vi",
    "course_section_code": "ma_lop_hoc_phan",
    "academic_year": "nam_hoc",
    "semester": "hoc_ky",
    "progress_percent": "ty_le_tien_do_giang_day",
    "updated_at": "thoi_gian_cap_nhat_nguon",
}


def _literal_assignment(path: Path, name: str):
    tree = ast.parse(
        path.read_text(encoding="utf-8")
    )

    for node in tree.body:
        if not isinstance(
            node,
            (ast.Assign, ast.AnnAssign),
        ):
            continue

        if isinstance(node, ast.Assign):
            targets = node.targets
            value = node.value
        else:
            targets = [node.target]
            value = node.value

        for target in targets:
            if (
                isinstance(target, ast.Name)
                and target.id == name
            ):
                return ast.literal_eval(value)

    raise AssertionError(
        f"{name} not found in {path}"
    )


def _load_source_payload():
    return json.loads(
        SOURCE_DATA.read_text(
            encoding="utf-8"
        )
    )


def _build_api_payload():
    source = _load_source_payload()
    records = deepcopy(source["data"])

    return {
        "source_system": "ctu_ioc",
        "dataset": "education.teaching_progress",
        "schema_version": "1.0-demo",
        "generated_at": (
            "2026-09-23T00:00:00+07:00"
        ),
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
        SCHEMA.read_text(
            encoding="utf-8"
        )
    )

    format_checker = FormatChecker()

    @format_checker.checks(
        "date-time",
        raises=(
            TypeError,
            ValueError,
            AttributeError,
        ),
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


def test_teaching_demo_contract_and_count():
    payload = _build_api_payload()
    _validator().validate(payload)

    assert (
        payload["dataset"]
        == "education.teaching_progress"
    )
    assert (
        payload["schema_version"]
        == "1.0-demo"
    )
    assert len(payload["data"]) == 30


def test_source_file_explicitly_marks_demo_contract():
    source = _load_source_payload()

    assert "DEMO / ASSUMED CONTRACT" in (
        source["contract_status"]
    )
    assert (
        "NOT VERIFIED CTU IOC PRODUCTION CONTRACT"
        in source["contract_status"]
    )


def test_source_native_field_set_is_exact_for_baseline():
    records = _load_source_payload()["data"]

    assert len(records) == 30
    assert all(
        tuple(record.keys())
        == EXPECTED_SOURCE_FIELDS
        for record in records
    )


def test_record_id_and_business_key_are_unique():
    records = _load_source_payload()["data"]

    ids = {
        record["record_id"]
        for record in records
    }
    keys = {
        (
            record["course_section_code"],
            record["academic_year"],
            record["semester"],
        )
        for record in records
    }

    assert len(ids) == 30
    assert len(keys) == 30


def test_registry_literals_match_frozen_demo_contract():
    source_fields = _literal_assignment(
        REGISTRY,
        "TEACHING_PROGRESS_FIELDS",
    )
    mapping = _literal_assignment(
        REGISTRY,
        "TEACHING_PROGRESS_SOURCE_TO_CANONICAL",
    )

    assert source_fields == EXPECTED_SOURCE_FIELDS
    assert mapping == EXPECTED_MAPPING


def test_delete_flag_is_not_in_source_contract():
    assert "is_deleted" not in EXPECTED_SOURCE_FIELDS
    assert "da_xoa" not in EXPECTED_MAPPING.values()

    for record in _load_source_payload()["data"]:
        assert "is_deleted" not in record


def test_source_evolution_extra_field_is_allowed():
    payload = _build_api_payload()
    payload["data"][0][
        "future_teaching_source_note"
    ] = "day6-probe"

    _validator().validate(payload)


@pytest.mark.parametrize(
    "mutator",
    [
        lambda p: p.pop("data"),
        lambda p: p["data"][0].pop(
            "record_id"
        ),
        lambda p: p["data"][0].pop(
            "course_section_code"
        ),
        lambda p: p["data"][0].__setitem__(
            "semester",
            "1",
        ),
        lambda p: p["data"][0].__setitem__(
            "progress_percent",
            "75.0",
        ),
        lambda p: p["data"][0].__setitem__(
            "updated_at",
            "not-a-date",
        ),
    ],
)
def test_structurally_invalid_contract_fails(
    mutator,
):
    broken = _build_api_payload()
    mutator(broken)

    with pytest.raises(ValidationError):
        _validator().validate(broken)


def test_business_dq_values_are_not_overconstrained_at_source():
    payload = _build_api_payload()

    payload["data"][0][
        "progress_percent"
    ] = 120.0
    payload["data"][1][
        "unit_code"
    ] = ""
    payload["data"][2][
        "semester"
    ] = 0

    # Structural source validation succeeds.
    # Silver DQ will own these demo business rules.
    _validator().validate(payload)


def test_mock_api_declares_teaching_route_and_demo_constants():
    text = MOCK_MAIN.read_text(
        encoding="utf-8"
    )

    assert (
        "/api/v1/education/teaching-progress"
        in text
    )
    assert (
        'TEACHING_DATASET = '
        '"education.teaching_progress"'
        in text
    )
    assert (
        'TEACHING_SCHEMA_VERSION = '
        '"1.0-demo"'
        in text
    )
    assert "X-Demo-Contract" in text


def test_generic_ingestion_has_no_teaching_literals():
    text = INGESTION.read_text(
        encoding="utf-8"
    )

    forbidden = (
        "education.teaching_progress",
        "unit_code",
        "course_section_code",
        "progress_percent",
        "ma_don_vi",
        "ma_lop_hoc_phan",
        "ty_le_tien_do_giang_day",
    )

    for literal in forbidden:
        assert literal not in text


def test_generic_silver_modules_have_no_teaching_literals():
    forbidden = (
        "education.teaching_progress",
        "unit_code",
        "course_section_code",
        "progress_percent",
        "ma_don_vi",
        "ma_lop_hoc_phan",
        "ty_le_tien_do_giang_day",
    )

    for path in sorted(
        GENERIC_SILVER_DIR.glob(
            "generic_silver_*.py"
        )
    ):
        text = path.read_text(
            encoding="utf-8"
        )

        for literal in forbidden:
            assert literal not in text
