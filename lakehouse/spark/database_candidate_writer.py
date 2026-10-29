"""Prepare one registered database extract as an immutable Silver candidate."""

from __future__ import annotations

import argparse
import hashlib
import importlib
import json
import os
from pathlib import Path

from pyspark.sql import functions as F

from ai_dataset_router import RoutingDecision
from api_dataset_registry import get_dataset_config
from env_config import S3_BUCKET_NAME
from gx_bronze_gate import validate_bronze_metrics
from gx_candidate_state import (
    CandidateAssetIdentity,
    CandidateState,
    candidate_state_path,
    load_candidate_state,
    retry_stable_branch_name,
    save_candidate_state,
)
from gx_universal_gate import resolve_snapshot_identity
from nessie_catalog_utils import get_nessie_reference_hash, use_main
from spark_generic_dynamic_processor import standardize_dataframe_columns


def _batch_id(dataset_id: str, run_id: str) -> str:
    digest = hashlib.sha256(f"{dataset_id}\ndatabase\n{run_id}".encode()).hexdigest()[:20]
    return f"database_{digest}"


def _load_callable(reference: str):
    module_name, separator, attribute = str(reference or "").partition(":")
    if not separator or not module_name or not attribute:
        raise RuntimeError("Processor reference must use module:function format")
    value = getattr(importlib.import_module(module_name), attribute, None)
    if not callable(value):
        raise RuntimeError(f"Configured processor is not callable: {reference}")
    return value


def prepare_database_candidate(
    *, manifest_path: str, decision_file: str, run_id: str
) -> CandidateState:
    manifest = json.loads(Path(manifest_path).read_text(encoding="utf-8"))
    decision = RoutingDecision(**json.loads(Path(decision_file).read_text(encoding="utf-8")))
    if not decision.registered_dataset_id:
        raise RuntimeError(
            "NOT_CONFIGURED: database extract is not mapped to an active dataset contract"
        )
    entities = manifest.get("entities") or {}
    if len(entities) != 1:
        raise RuntimeError(
            "NOT_CONFIGURED: governed database candidate requires exactly one mapped entity"
        )
    config = get_dataset_config(decision.registered_dataset_id)
    branch = retry_stable_branch_name("gx_database_silver", config.dataset, run_id)
    root = Path(os.getenv("GX_CANDIDATE_STATE_ROOT", "/opt/airflow/logs/gx-candidates"))
    state_file = candidate_state_path(root, config.dataset, run_id)
    if state_file.is_file():
        state = load_candidate_state(state_file)
        if state.source_kind != "database" or state.candidate_branch != branch:
            raise RuntimeError("Existing database candidate state has a different identity")
        return state
    if get_nessie_reference_hash(branch, missing_ok=True) is not None:
        raise RuntimeError("Database candidate branch exists without durable state")

    bucket = str(manifest.get("bucket") or S3_BUCKET_NAME)
    keys = next(iter(entities.values()))
    if not isinstance(keys, list) or not keys:
        raise RuntimeError("Database manifest entity has no extract objects")
    paths = [f"s3a://{bucket}/{str(key).lstrip('/')}" for key in keys]
    from spark_bronze_to_silver import get_spark_session

    spark = get_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    try:
        use_main(spark)
        raw = standardize_dataframe_columns(spark.read.parquet(*paths))
        for source, canonical in config.source_to_canonical.items():
            if source not in raw.columns and canonical in raw.columns:
                raw = raw.withColumnRenamed(canonical, source)
        missing = [name for name in config.source_fields if name not in raw.columns]
        if missing:
            raise RuntimeError(f"Database extract is missing registered source fields: {missing}")
        source = raw.select(
            *[
                F.col(field.name).cast(field.dataType).alias(field.name)
                for field in config.spark_source_schema.fields
            ]
        )
        row_count = source.count()
        if row_count < 1:
            raise RuntimeError("Database extract contains no rows")
        batch_id = _batch_id(config.dataset, run_id)
        checksum_columns = [
            F.coalesce(F.col(name).cast("string"), F.lit("<NULL>"))
            for name in config.source_fields
        ]
        bronze = (
            source
            .withColumn("_source_system", F.lit("airbyte_database"))
            .withColumn("_source_type", F.lit("database"))
            .withColumn("_dataset", F.lit(config.dataset))
            .withColumn("_schema_version", F.lit(config.schema_version))
            .withColumn("_ingestion_mode", F.lit("database_candidate"))
            .withColumn("_batch_id", F.lit(batch_id))
            .withColumn("_source_updated_at", F.col(config.source_updated_at_field))
            .withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_record_checksum", F.sha2(F.concat_ws("||", *checksum_columns), 256))
        )
        bronze_path = (
            f"s3a://{S3_BUCKET_NAME}/{config.bronze_prefix.strip('/')}"
            f"/batch_id={batch_id}/data.parquet"
        )
        bronze.write.mode("overwrite").parquet(bronze_path)
        readback = spark.read.parquet(bronze_path)
        readback_count = readback.count()
        gx_bronze = validate_bronze_metrics(
            dataset_id=config.dataset,
            source_kind="database",
            run_id=run_id,
            artifact_ref=bronze_path,
            metrics={
                "readable": True,
                "row_count": readback_count,
                "corrupt_record_ratio": 0.0,
                "metadata_complete": all(
                    name in readback.columns
                    for name in ("_dataset", "_batch_id", "_ingested_at", "_record_checksum")
                ),
                "checksum_present": readback.filter(F.col("_record_checksum").isNull()).limit(1).count() == 0,
                "duplicate_ingestion_count": 0,
                "extraction_complete": readback_count == row_count,
                "watermark_present": readback.filter(F.col("_source_updated_at").isNull()).limit(1).count() == 0,
            },
        )
        if gx_bronze.get("merge_allowed") is not True:
            raise RuntimeError("GX Bronze denied database candidate creation")

        spark.table(config.silver_table).limit(0).collect()
        spark.table(config.quarantine_table).limit(0).collect()
        base_hash = get_nessie_reference_hash("main")
        processor = _load_callable(str(config.silver_processor))
        processor(spark, batch_id, merge_to_main=False, candidate_branch=branch)
        assets = (config.silver_table, config.quarantine_table)
        identities = tuple(resolve_snapshot_identity(asset, branch) for asset in assets)
        if len({identity.commit_hash for identity in identities}) != 1:
            raise RuntimeError("Database candidate assets are not sealed at one head")
        state = CandidateState(
            dataset_id=config.dataset,
            source_kind="database",
            run_id=run_id,
            candidate_branch=branch,
            base_reference="main",
            base_hash=base_hash,
            candidate_hash=identities[0].commit_hash,
            assets=tuple(
                CandidateAssetIdentity(asset, identity.snapshot_id)
                for asset, identity in zip(assets, identities)
            ),
        )
        save_candidate_state(state, state_file)
        return state
    finally:
        try:
            use_main(spark)
        finally:
            spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--decision-file", required=True)
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args()
    state = prepare_database_candidate(
        manifest_path=args.manifest,
        decision_file=args.decision_file,
        run_id=args.run_id,
    )
    print(json.dumps(state.as_payload(), ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
