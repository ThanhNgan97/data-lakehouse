"""Prepare a registered file upload as an immutable Silver candidate."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path

from pyspark.sql import functions as F

from ai_dataset_router import RoutingDecision
from api_dataset_orchestration import _load_callable
from api_dataset_registry import get_dataset_config
from env_config import S3_BUCKET_NAME
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
from spark_bronze_to_silver import get_spark_session
from spark_generic_dynamic_processor import read_input_dataset, standardize_dataframe_columns


def _batch_id(dataset_id: str, run_id: str) -> str:
    digest = hashlib.sha256(f"{dataset_id}\n{run_id}".encode()).hexdigest()[:20]
    return f"file_upload_{digest}"


def prepare_file_candidate(
    *, input_path: str, decision_file: str, run_id: str
) -> CandidateState:
    decision = RoutingDecision(**json.loads(Path(decision_file).read_text(encoding="utf-8")))
    if decision.route_target != "registered_api" or not decision.registered_dataset_id:
        raise RuntimeError(
            "NOT_CONFIGURED: file upload is not mapped to an active registered dataset"
        )
    config = get_dataset_config(decision.registered_dataset_id)
    branch = retry_stable_branch_name("gx_file_silver", config.dataset, run_id)
    root = Path(os.getenv("GX_CANDIDATE_STATE_ROOT", "/opt/airflow/logs/gx-candidates"))
    state_file = candidate_state_path(root, config.dataset, run_id)
    if state_file.is_file():
        state = load_candidate_state(state_file)
        if state.source_kind != "file" or state.candidate_branch != branch:
            raise RuntimeError("Existing file candidate state has a different identity")
        return state
    if get_nessie_reference_hash(branch, missing_ok=True) is not None:
        raise RuntimeError("File candidate branch exists without durable state")

    spark = get_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    try:
        use_main(spark)
        raw = standardize_dataframe_columns(read_input_dataset(spark, input_path))
        for source, canonical in config.source_to_canonical.items():
            if source not in raw.columns and canonical in raw.columns:
                raw = raw.withColumnRenamed(canonical, source)
        missing = [name for name in config.source_fields if name not in raw.columns]
        if missing:
            raise RuntimeError(f"Uploaded file is missing registered source fields: {missing}")
        source = raw.select(*[
            F.col(field.name).cast(field.dataType).alias(field.name)
            for field in config.spark_source_schema.fields
        ])
        now = F.current_timestamp()
        checksum_columns = [F.coalesce(F.col(name).cast("string"), F.lit("<NULL>")) for name in config.source_fields]
        bronze = (
            source
            .withColumn("_source_system", F.lit("file_upload"))
            .withColumn("_source_type", F.lit("file"))
            .withColumn("_dataset", F.lit(config.dataset))
            .withColumn("_schema_version", F.lit(config.schema_version))
            .withColumn("_ingestion_mode", F.lit("file_upload"))
            .withColumn("_batch_id", F.lit(_batch_id(config.dataset, run_id)))
            .withColumn("_source_updated_at", F.col(config.source_updated_at_field))
            .withColumn("_ingested_at", now)
            .withColumn("_record_checksum", F.sha2(F.concat_ws("||", *checksum_columns), 256))
        )
        if bronze.limit(1).count() == 0:
            raise RuntimeError("Uploaded file contains no rows")
        batch_id = _batch_id(config.dataset, run_id)
        bronze_path = (
            f"s3a://{S3_BUCKET_NAME}/{config.bronze_prefix.strip('/')}"
            f"/batch_id={batch_id}/data.parquet"
        )
        bronze.write.mode("overwrite").parquet(bronze_path)

        spark.table(config.silver_table).limit(0).collect()
        spark.table(config.quarantine_table).limit(0).collect()
        base_hash = get_nessie_reference_hash("main")
        processor = _load_callable(str(config.silver_processor))
        processor(spark, batch_id, merge_to_main=False, candidate_branch=branch)
        identities = tuple(
            resolve_snapshot_identity(asset, branch)
            for asset in (config.silver_table, config.quarantine_table)
        )
        hashes = {identity.commit_hash for identity in identities}
        if len(hashes) != 1:
            raise RuntimeError("File candidate assets are not sealed at one head")
        state = CandidateState(
            dataset_id=config.dataset,
            source_kind="file",
            run_id=run_id,
            candidate_branch=branch,
            base_reference="main",
            base_hash=base_hash,
            candidate_hash=identities[0].commit_hash,
            assets=tuple(
                CandidateAssetIdentity(asset, identity.snapshot_id)
                for asset, identity in zip(
                    (config.silver_table, config.quarantine_table), identities
                )
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
    parser.add_argument("--input", required=True)
    parser.add_argument("--decision-file", required=True)
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args()
    state = prepare_file_candidate(
        input_path=args.input, decision_file=args.decision_file, run_id=args.run_id
    )
    print(json.dumps(state.as_payload(), ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
