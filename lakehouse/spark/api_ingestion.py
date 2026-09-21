# -*- coding: utf-8 -*-
"""Generic API/JSON -> Bronze ingestion engine.

The adapter validates the external contract, preserves source/business fields,
adds Lakehouse metadata, creates a Spark DataFrame with an explicit schema,
and delegates physical Parquet writing to the shared Bronze writer.
"""

from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator, FormatChecker
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType, TimestampType

from api_dataset_registry import ApiDatasetConfig, get_dataset_config
from bronze_writer import read_parquet_object, write_parquet_object


METADATA_COLUMNS = (
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


def canonical_source_checksum(
    source_record: dict[str, Any],
) -> str:
    """Return a stable source checksum across the naming-only migration.

    The learning-outcomes contract renamed business keys from English
    to Vietnamese. _record_checksum is technical lineage, so records
    with otherwise identical source values retain their prior identity.
    Other generic API schemas keep normal raw-record serialization.
    """
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

    checksum_record = source_record

    if set(source_record) == set(legacy_names):
        checksum_record = {
            legacy_name: source_record[current_name]
            for current_name, legacy_name
            in legacy_names.items()
        }

    canonical = json.dumps(
        checksum_record,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )

    return hashlib.sha256(
        canonical.encode("utf-8")
    ).hexdigest()



def _parse_iso_datetime(value: str, *, field_name: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} is not a valid ISO-8601 datetime: {value!r}") from exc
    if parsed.tzinfo is None:
        raise ValueError(f"{field_name} must contain a timezone offset: {value!r}")
    # Spark TimestampType is timezone-less at the Python boundary; normalize to UTC.
    return parsed.astimezone(timezone.utc).replace(tzinfo=None)


def _load_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as fh:
        payload = json.load(fh)
    if not isinstance(payload, dict):
        raise ValueError("Top-level API response must be a JSON object")
    return payload


def validate_contract(payload: dict[str, Any], config: ApiDatasetConfig) -> None:
    with config.json_schema_path.open("r", encoding="utf-8") as fh:
        schema = json.load(fh)

    validator = Draft202012Validator(schema, format_checker=FormatChecker())
    errors = sorted(validator.iter_errors(payload), key=lambda e: list(e.path))
    if errors:
        details = []
        for err in errors[:20]:
            location = ".".join(str(p) for p in err.path) or "<root>"
            details.append(f"{location}: {err.message}")
        raise ValueError("API contract validation failed:\n - " + "\n - ".join(details))

    if payload.get("dataset") != config.dataset:
        raise ValueError(
            f"Payload dataset={payload.get('dataset')!r} does not match config={config.dataset!r}"
        )
    if payload.get("schema_version") != config.schema_version:
        raise ValueError(
            "Payload schema_version="
            f"{payload.get('schema_version')!r} does not match config={config.schema_version!r}"
        )

    # Do not rely only on jsonschema optional date-time format support.
    # Parse ISO-8601 timestamps explicitly so invalid strings always fail.
    _parse_iso_datetime(
        payload["generated_at"],
        field_name="generated_at",
    )

    if payload.get("data_as_of") is not None:
        _parse_iso_datetime(
            payload["data_as_of"],
            field_name="data_as_of",
        )

    for index, record in enumerate(payload["data"]):
        source_updated_at_field = config.source_updated_at_field
        _parse_iso_datetime(
            record[source_updated_at_field],
            field_name=(
                f"data[{index}].{source_updated_at_field}"
            ),
        )


def _metadata_schema() -> StructType:
    return StructType([
        StructField("_source_system", StringType(), False),
        StructField("_source_type", StringType(), False),
        StructField("_dataset", StringType(), False),
        StructField("_schema_version", StringType(), False),
        StructField("_ingestion_mode", StringType(), False),
        StructField("_batch_id", StringType(), False),
        StructField("_source_updated_at", TimestampType(), False),
        StructField("_ingested_at", TimestampType(), False),
        StructField("_record_checksum", StringType(), False),
    ])


def _full_schema(config: ApiDatasetConfig) -> StructType:
    return StructType(list(config.spark_source_schema.fields) + list(_metadata_schema().fields))


def _prepare_rows(
    payload: dict[str, Any],
    config: ApiDatasetConfig,
    *,
    ingestion_mode: str,
    batch_id: str,
    ingested_at: datetime,
) -> list[dict[str, Any]]:
    source_rows = payload["data"]
    if not source_rows:
        raise ValueError("data[] is empty; refusing to create an empty Bronze batch")

    prepared = []
    for index, source_record in enumerate(source_rows):
        # JSON Schema already rejects missing/additional fields and bad primitive types.
        checksum = canonical_source_checksum(source_record)
        source_updated_at_field = (
            config.source_updated_at_field
        )

        source_updated_at = _parse_iso_datetime(
            source_record[source_updated_at_field],
            field_name=(
                f"data[{index}].{source_updated_at_field}"
            ),
        )

        row = dict(source_record)
        row[source_updated_at_field] = source_updated_at
        row.update({
            "_source_system": payload["source_system"],
            "_source_type": "API",
            "_dataset": payload["dataset"],
            "_schema_version": payload["schema_version"],
            "_ingestion_mode": ingestion_mode,
            "_batch_id": batch_id,
            "_source_updated_at": source_updated_at,
            "_ingested_at": ingested_at,
            "_record_checksum": checksum,
        })
        prepared.append(row)
    return prepared


def build_spark_dataframe(
    spark: SparkSession,
    payload: dict[str, Any],
    config: ApiDatasetConfig,
    *,
    ingestion_mode: str,
    batch_id: str,
    ingested_at: datetime,
):
    prepared = _prepare_rows(
        payload,
        config,
        ingestion_mode=ingestion_mode,
        batch_id=batch_id,
        ingested_at=ingested_at,
    )
    return spark.createDataFrame(prepared, schema=_full_schema(config))


def make_batch_id(source_system: str) -> str:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{source_system}_{stamp}_{uuid.uuid4().hex[:8]}"


@dataclass(frozen=True)
class IngestionResult:
    object_key: str
    input_count: int
    dataframe_count: int
    readback_count: int
    batch_id: str
    sample_record_id: str
    sample_checksum: str


def ingest_payload_to_bronze(
    spark: SparkSession,
    payload: dict[str, Any],
    *,
    dataset: str,
    ingestion_mode: str = "FULL_DEMO",
    batch_id: str | None = None,
    expected_count: int | None = None,
    input_label: str = "response.json()",
) -> IngestionResult:
    config = get_dataset_config(dataset)
    validate_contract(payload, config)

    input_count = len(payload["data"])
    if expected_count is not None and input_count != expected_count:
        raise ValueError(f"Input record count mismatch: expected={expected_count}, actual={input_count}")

    batch_id = batch_id or make_batch_id(payload["source_system"])
    ingested_at = datetime.now(timezone.utc).replace(tzinfo=None)

    spark.conf.set("spark.sql.session.timeZone", "UTC")
    df = build_spark_dataframe(
        spark,
        payload,
        config,
        ingestion_mode=ingestion_mode,
        batch_id=batch_id,
        ingested_at=ingested_at,
    )

    print("\n=== API BRONZE SPARK SCHEMA ===")
    df.printSchema()

    dataframe_count = df.count()
    if dataframe_count != input_count:
        raise AssertionError(
            f"DataFrame record count mismatch: input={input_count}, dataframe={dataframe_count}"
        )

    missing_metadata = [c for c in METADATA_COLUMNS if c not in df.columns]
    if missing_metadata:
        raise AssertionError(f"Missing Bronze metadata columns: {missing_metadata}")

    object_key = f"{config.bronze_prefix}batch_id={batch_id}/data.parquet"
    object_key, written_count = write_parquet_object(df, object_key)
    if written_count != dataframe_count:
        raise AssertionError(
            f"Writer record count mismatch: dataframe={dataframe_count}, written={written_count}"
        )

    readback = read_parquet_object(object_key)
    readback_count = len(readback.index)
    if readback_count != input_count:
        raise AssertionError(
            f"Read-back record count mismatch: input={input_count}, readback={readback_count}"
        )

    sample_source = payload["data"][0]

    sample_id_field = config.record_id_field
    sample_id = sample_source[sample_id_field]

    expected_checksum = canonical_source_checksum(
        sample_source
    )

    sample_after = readback.loc[
        readback[sample_id_field] == sample_id
    ]

    if len(sample_after.index) != 1:
        raise AssertionError(
            f"Sample {sample_id_field}={sample_id!r} "
            "not found exactly once after read-back"
        )

    actual_checksum = str(
        sample_after.iloc[0]["_record_checksum"]
    )

    if actual_checksum != expected_checksum:
        raise AssertionError(
            f"Checksum mismatch for {sample_id}: "
            f"expected={expected_checksum}, "
            f"actual={actual_checksum}"
        )

    sample = sample_after.iloc[0]
    failed_comparisons = []

    for field_name in config.sample_validation_fields:

        expected = sample_source[field_name]
        actual = sample[field_name]

        if isinstance(expected, bool):
            ok = bool(actual) is expected

        elif (
            isinstance(expected, int)
            and not isinstance(expected, bool)
        ):
            ok = int(actual) == expected

        elif isinstance(expected, float):
            ok = (
                abs(
                    float(actual)
                    - float(expected)
                )
                < 1e-9
            )

        else:
            ok = str(actual) == str(expected)

        if not ok:
            failed_comparisons.append(
                field_name
            )

    if failed_comparisons:
        raise AssertionError(
            "Sample value mismatch after read-back: "
            f"{failed_comparisons}"
        )

    print("\n=== API BRONZE VALIDATION ===")
    print(f"A. Input JSON load: PASS ({input_label})")
    print(f"B. Input record count: {input_count}")
    print(f"C. DataFrame record count: {dataframe_count}")
    print(f"E. Metadata columns: PASS ({len(METADATA_COLUMNS)}/{len(METADATA_COLUMNS)})")
    print(f"F. Bronze object: s3://university-lakehouse/{object_key}")
    print("G. Parquet read-back: PASS")
    print(f"H. Read-back record count: {readback_count}")
    print(f"I. Sample values: PASS ({sample_id})")
    print("J. pandas read-back dtypes:")
    print(readback.dtypes.to_string())
    print(f"K. Checksum deterministic for sample: PASS ({expected_checksum})")

    return IngestionResult(
        object_key=object_key,
        input_count=input_count,
        dataframe_count=dataframe_count,
        readback_count=readback_count,
        batch_id=batch_id,
        sample_record_id=sample_id,
        sample_checksum=expected_checksum,
    )


def ingest_json_file_to_bronze(
    spark: SparkSession,
    input_path: str | Path,
    *,
    dataset: str,
    ingestion_mode: str = "FULL_DEMO",
    batch_id: str | None = None,
    expected_count: int | None = None,
) -> IngestionResult:
    input_path = Path(input_path)
    payload = _load_json(input_path)
    return ingest_payload_to_bronze(
        spark,
        payload,
        dataset=dataset,
        ingestion_mode=ingestion_mode,
        batch_id=batch_id,
        expected_count=expected_count,
        input_label=str(input_path),
    )



def fetch_http_api_payload(
    api_url: str,
    *,
    dataset: str,
    updated_after: str | None = None,
    api_key: str | None = None,
    page_limit: int = 500,
    timeout_seconds: int = 30,
) -> dict[str, Any]:
    """
    Fetch one logical API dataset over one or more cursor-paginated
    HTTP GET responses and return one contract-compatible payload.

    Transport/pagination belongs here.
    Bronze transformation and persistence remain in
    ingest_payload_to_bronze().
    """
    from urllib.error import HTTPError, URLError
    from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
    from urllib.request import Request, urlopen

    if not api_url:
        raise ValueError("api_url must not be empty")

    if page_limit < 1 or page_limit > 500:
        raise ValueError("page_limit must be between 1 and 500")

    if timeout_seconds < 1:
        raise ValueError("timeout_seconds must be >= 1")

    config = get_dataset_config(dataset)

    parts = urlsplit(api_url)
    base_query = dict(
        parse_qsl(
            parts.query,
            keep_blank_values=True,
        )
    )

    headers = {
        "Accept": "application/json",
    }

    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"

    all_records: list[dict[str, Any]] = []
    seen_record_ids: set[str] = set()
    seen_cursors: set[str] = set()

    first_page: dict[str, Any] | None = None
    cursor: str | None = None
    page_number = 0

    while True:
        page_number += 1

        if page_number > 10000:
            raise RuntimeError("Pagination exceeded safety limit")

        query = dict(base_query)
        query["limit"] = str(page_limit)

        if updated_after:
            query["updated_after"] = updated_after

        if cursor:
            query["cursor"] = cursor
        else:
            query.pop("cursor", None)

        request_url = urlunsplit(
            (
                parts.scheme,
                parts.netloc,
                parts.path,
                urlencode(query),
                parts.fragment,
            )
        )

        request = Request(
            request_url,
            headers=headers,
            method="GET",
        )

        try:
            with urlopen(
                request,
                timeout=timeout_seconds,
            ) as response:
                payload = json.load(response)

        except HTTPError as exc:
            try:
                body = exc.read().decode(
                    "utf-8",
                    errors="replace",
                )
            except Exception:
                body = ""

            raise RuntimeError(
                f"HTTP API returned status={exc.code}: {body}"
            ) from exc

        except URLError as exc:
            raise RuntimeError(
                f"HTTP API connection failed: {exc}"
            ) from exc

        if not isinstance(payload, dict):
            raise ValueError(
                "Top-level API response must be a JSON object"
            )

        # Validate every real API page before accepting its data.
        validate_contract(payload, config)

        if first_page is None:
            first_page = payload
        else:
            for field in (
                "source_system",
                "dataset",
                "schema_version",
            ):
                if payload.get(field) != first_page.get(field):
                    raise ValueError(
                        f"Inconsistent {field} across API pages"
                    )

        page_records = payload["data"]

        for record in page_records:
            source_record_id = record[
                config.record_id_field
            ]

            if source_record_id in seen_record_ids:
                raise ValueError(
                    "Duplicate source identifier across "
                    "API pages: "
                    f"{config.record_id_field}="
                    f"{source_record_id}"
                )

            seen_record_ids.add(
                source_record_id
            )
            all_records.append(record)

        pagination = payload["pagination"]

        print(
            "HTTP_PAGE="
            f"{page_number}"
            f"|records={len(page_records)}"
            f"|has_more={pagination['has_more']}"
        )

        if not pagination["has_more"]:
            break

        next_cursor = pagination.get("next_cursor")

        if not next_cursor:
            raise ValueError(
                "API says has_more=true but next_cursor is missing"
            )

        if next_cursor in seen_cursors:
            raise ValueError(
                "Repeated pagination cursor detected"
            )

        seen_cursors.add(next_cursor)
        cursor = next_cursor

    if first_page is None:
        raise RuntimeError("API returned no response pages")

    merged_payload = dict(first_page)

    merged_payload["data"] = all_records

    # This is now the locally assembled logical response.
    merged_payload["pagination"] = {
        "returned_records": len(all_records),
        "has_more": False,
        "next_cursor": None,
    }

    # Validate the complete logical payload as well.
    validate_contract(
        merged_payload,
        config,
    )

    print(
        "HTTP_FETCH_TOTAL="
        f"{len(all_records)}"
        f"|pages={page_number}"
    )

    return merged_payload


def ingest_http_api_to_bronze(
    spark: SparkSession,
    api_url: str,
    *,
    dataset: str,
    ingestion_mode: str = "FULL_DEMO",
    batch_id: str | None = None,
    expected_count: int | None = None,
    updated_after: str | None = None,
    api_key: str | None = None,
    page_limit: int = 500,
    timeout_seconds: int = 30,
) -> IngestionResult:
    """
    HTTP source adapter.

    Fetch HTTP payload, then delegate to the existing verified
    contract/schema/metadata/checksum/MinIO Bronze engine.
    """
    payload = fetch_http_api_payload(
        api_url,
        dataset=dataset,
        updated_after=updated_after,
        api_key=api_key,
        page_limit=page_limit,
        timeout_seconds=timeout_seconds,
    )

    return ingest_payload_to_bronze(
        spark,
        payload,
        dataset=dataset,
        ingestion_mode=ingestion_mode,
        batch_id=batch_id,
        expected_count=expected_count,
        input_label=f"HTTP GET {api_url}",
    )