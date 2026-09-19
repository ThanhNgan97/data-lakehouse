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


def canonical_source_checksum(record: dict[str, Any]) -> str:
    """SHA-256 of source/business content only, independent of ingestion metadata."""
    canonical = json.dumps(
        record,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


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
        _parse_iso_datetime(
            record["updated_at"],
            field_name=f"data[{index}].updated_at",
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
        updated_at = _parse_iso_datetime(source_record["updated_at"], field_name=f"data[{index}].updated_at")

        row = dict(source_record)
        row["updated_at"] = updated_at
        row.update({
            "_source_system": payload["source_system"],
            "_source_type": "API",
            "_dataset": payload["dataset"],
            "_schema_version": payload["schema_version"],
            "_ingestion_mode": ingestion_mode,
            "_batch_id": batch_id,
            "_source_updated_at": updated_at,
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
    sample_id = sample_source["record_id"]
    expected_checksum = canonical_source_checksum(sample_source)
    sample_after = readback.loc[readback["record_id"] == sample_id]
    if len(sample_after.index) != 1:
        raise AssertionError(f"Sample record_id={sample_id!r} not found exactly once after read-back")

    actual_checksum = str(sample_after.iloc[0]["_record_checksum"])
    if actual_checksum != expected_checksum:
        raise AssertionError(
            f"Checksum mismatch for {sample_id}: expected={expected_checksum}, actual={actual_checksum}"
        )

    # Values that must remain source-like and typed, not dashboard-formatted strings.
    sample = sample_after.iloc[0]
    comparisons = {
        "program_code": sample["program_code"] == sample_source["program_code"],
        "academic_year": sample["academic_year"] == sample_source["academic_year"],
        "semester": int(sample["semester"]) == sample_source["semester"],
        "student_count": int(sample["student_count"]) == sample_source["student_count"],
        "gpa_point_sum": abs(float(sample["gpa_point_sum"]) - float(sample_source["gpa_point_sum"])) < 1e-9,
        "is_deleted": bool(sample["is_deleted"]) is sample_source["is_deleted"],
    }
    failed_comparisons = [name for name, ok in comparisons.items() if not ok]
    if failed_comparisons:
        raise AssertionError(f"Sample value mismatch after read-back: {failed_comparisons}")

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
