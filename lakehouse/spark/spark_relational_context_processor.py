# -*- coding: utf-8 -*-
"""Generic multi-table relational-context processor.

Airbyte lands one context as ``staging/<context>/<entity>/*.parquet``. This
processor loads every entity in a manifest, maintains one Silver Iceberg table
per entity, infers a guarded relationship plan, aggregates one-to-many facts,
and publishes one joined Gold table for Superset.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Mapping, Sequence, Tuple


if sys.stdout and hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

_CURRENT_DIR = Path(__file__).resolve().parent
if str(_CURRENT_DIR) not in sys.path:
    sys.path.insert(0, str(_CURRENT_DIR))

from ai_dataset_router import RoutingDecision, to_snake_case
from env_config import MINIO_ACCESS_KEY, MINIO_BUCKET_NAME, MINIO_ENDPOINT, MINIO_SECRET_KEY
from nessie_catalog_utils import create_branch, make_branch_name, merge_branch_to_main, use_branch, use_main
from relational_context import (
    EntityProfile,
    build_context_plan,
    candidate_combinations,
    candidate_key_columns,
    is_technical_column,
)
from spark_generic_dynamic_processor import (
    apply_iceberg_schema_evolution,
    get_spark_session,
    standardize_dataframe_columns,
    table_exists,
)

from pyspark.sql import DataFrame, SparkSession, Window, functions as F
from pyspark.sql.types import NumericType, StringType


def _safe_name(value: str) -> str:
    return to_snake_case(value) or "entity"


def _quoted(value: str) -> str:
    return "`" + value.replace("`", "``") + "`"


def load_manifest(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as handle:
        manifest = json.load(handle)
    if not manifest.get("context_id") or not manifest.get("entities"):
        raise ValueError("Relational context manifest is missing context_id or entities")
    return manifest


def infer_business_key(df: DataFrame) -> Tuple[List[str], float, Dict[str, Any]]:
    """Infer a bounded single/composite key and keep measurable evidence."""
    row_count = df.count()
    candidates = candidate_key_columns(df.columns)
    evidence: Dict[str, Any] = {"row_count": row_count, "candidates": {}}
    if row_count == 0 or not candidates:
        return [], 0.0, evidence

    expressions = []
    for index, column in enumerate(candidates):
        expressions.extend(
            [
                F.countDistinct(F.col(column)).alias(f"distinct_{index}"),
                F.sum(F.when(F.col(column).isNull(), 1).otherwise(0)).alias(f"nulls_{index}"),
            ]
        )
    stats = df.agg(*expressions).first().asDict()

    best_column = None
    best_score = -1.0
    for index, column in enumerate(candidates):
        distinct = int(stats.get(f"distinct_{index}") or 0)
        nulls = int(stats.get(f"nulls_{index}") or 0)
        unique_ratio = distinct / row_count
        non_null_ratio = (row_count - nulls) / row_count
        name_rank = 1.0 - (index / max(len(candidates), 1))
        score = (0.70 * unique_ratio) + (0.20 * non_null_ratio) + (0.10 * name_rank)
        evidence["candidates"][column] = {
            "distinct": distinct,
            "unique_ratio": round(unique_ratio, 6),
            "non_null_ratio": round(non_null_ratio, 6),
            "score": round(score, 6),
        }
        if non_null_ratio == 1.0 and unique_ratio >= 0.98 and score > best_score:
            best_column = column
            best_score = score

    if best_column:
        return [best_column], min(best_score, 1.0), evidence

    # Only try a bounded set of two-column keys; this prevents combinatorial scans.
    for pair in candidate_combinations(candidates):
        non_null = df.filter(F.col(pair[0]).isNotNull() & F.col(pair[1]).isNotNull()).count()
        if non_null != row_count:
            continue
        distinct = df.agg(F.countDistinct(F.struct(*[F.col(c) for c in pair])).alias("n")).first()["n"]
        unique_ratio = int(distinct or 0) / row_count
        if unique_ratio >= 0.98:
            evidence["composite_key"] = {
                "columns": pair,
                "unique_ratio": round(unique_ratio, 6),
            }
            return list(pair), min(0.92 + (unique_ratio - 0.98), 0.99), evidence

    # A weak candidate is usable for grouping but is explicitly marked low-confidence.
    fallback = candidates[0]
    fallback_stats = evidence["candidates"].get(fallback, {})
    confidence = min(float(fallback_stats.get("score") or 0.0), 0.74)
    return [fallback], confidence, evidence


def _latest_order_columns(df: DataFrame) -> List[Any]:
    preferred = [
        "_airbyte_extracted_at",
        "airbyte_extracted_at",
        "_ab_cdc_updated_at",
        "ab_cdc_updated_at",
        "updated_at",
        "modified_at",
        "_ab_cdc_lsn",
        "ab_cdc_lsn",
    ]
    return [F.col(column).desc_nulls_last() for column in preferred if column in df.columns]


def deduplicate_latest(df: DataFrame, keys: Sequence[str]) -> DataFrame:
    if not keys:
        return df
    ordering = _latest_order_columns(df)
    if not ordering:
        stable_columns = sorted(df.columns)
        ordering = [
            F.sha2(
                F.concat_ws(
                    "||",
                    *[
                        F.coalesce(F.col(column).cast("string"), F.lit("<NULL>"))
                        for column in stable_columns
                    ],
                ),
                256,
            ).desc()
        ]
    window = Window.partitionBy(*[F.col(key) for key in keys]).orderBy(*ordering)
    return df.withColumn("_context_rank", F.row_number().over(window)).filter(
        F.col("_context_rank") == 1
    ).drop("_context_rank")


def merge_silver_entity(
    spark: SparkSession,
    df: DataFrame,
    table_name: str,
    business_keys: Sequence[str],
) -> int:
    """Maintain current entity state in Silver using an idempotent Iceberg MERGE."""
    working = df
    if not business_keys:
        value_columns = [column for column in working.columns if not is_technical_column(column)]
        working = working.withColumn(
            "_context_row_hash",
            F.sha2(
                F.concat_ws(
                    "||",
                    *[
                        F.coalesce(F.col(column).cast("string"), F.lit("<NULL>"))
                        for column in value_columns
                    ],
                ),
                256,
            ),
        )
        business_keys = ["_context_row_hash"]

    working = deduplicate_latest(working, business_keys).withColumn(
        "_context_ingested_at", F.current_timestamp()
    )
    working = working.filter(
        F.expr(" AND ".join(f"{_quoted(key)} IS NOT NULL" for key in business_keys))
    )
    record_count = working.count()

    delete_column = next(
        (
            column
            for column in ("_ab_cdc_deleted_at", "ab_cdc_deleted_at")
            if column in working.columns
        ),
        None,
    )
    if not table_exists(spark, table_name):
        initial = working.filter(F.col(delete_column).isNull()) if delete_column else working
        initial.writeTo(table_name).using("iceberg").create()
        return initial.count()

    apply_iceberg_schema_evolution(spark, table_name, working)
    view_name = "incoming_" + _safe_name(table_name.split(".")[-1])
    working.createOrReplaceTempView(view_name)

    join_condition = " AND ".join(
        f"t.{_quoted(key)} <=> s.{_quoted(key)}" for key in business_keys
    )
    assignments = ", ".join(
        f"t.{_quoted(column)} = s.{_quoted(column)}" for column in working.columns
    )
    insert_columns = ", ".join(_quoted(column) for column in working.columns)
    insert_values = ", ".join(f"s.{_quoted(column)}" for column in working.columns)

    clauses = []
    if delete_column:
        clauses.append(
            f"WHEN MATCHED AND s.{_quoted(delete_column)} IS NOT NULL THEN DELETE"
        )
    clauses.append(f"WHEN MATCHED THEN UPDATE SET {assignments}")
    not_deleted = (
        f" AND s.{_quoted(delete_column)} IS NULL" if delete_column else ""
    )
    clauses.append(
        f"WHEN NOT MATCHED{not_deleted} THEN INSERT ({insert_columns}) VALUES ({insert_values})"
    )
    spark.sql(
        f"MERGE INTO {table_name} t USING {view_name} s ON {join_condition} "
        + " ".join(clauses)
    )
    return record_count


def _prefixed_columns(df: DataFrame, entity: str, keys: Sequence[str]) -> DataFrame:
    selections = [F.col(key) for key in keys]
    selections.extend(
        F.col(column).alias(f"{entity}__{column}")
        for column in df.columns
        if column not in keys and not is_technical_column(column)
    )
    return df.select(*selections)


def aggregate_fact(df: DataFrame, entity: str, keys: Sequence[str]) -> DataFrame:
    aggregations = [F.count(F.lit(1)).alias(f"{entity}__record_count")]
    for field in df.schema.fields:
        column = field.name
        if column in keys or is_technical_column(column):
            continue
        if isinstance(field.dataType, NumericType):
            aggregations.extend(
                [
                    F.sum(F.col(column)).alias(f"{entity}__sum__{column}"),
                    F.avg(F.col(column)).alias(f"{entity}__avg__{column}"),
                ]
            )
    return df.groupBy(*[F.col(key) for key in keys]).agg(*aggregations)


def build_gold_context(
    spark: SparkSession,
    context_id: str,
    plan,
    silver_tables: Mapping[str, str],
) -> DataFrame:
    gold = spark.table(silver_tables[plan.anchor_entity])
    for relationship in plan.relationships:
        peer = spark.table(silver_tables[relationship.entity])
        keys = list(relationship.join_columns)
        if relationship.kind == "fact":
            prepared = aggregate_fact(peer, relationship.entity, keys)
        else:
            prepared = _prefixed_columns(peer, relationship.entity, keys).dropDuplicates(keys)
        gold = gold.join(prepared, on=keys, how="left")

    return gold.withColumn("_context_id", F.lit(context_id)).withColumn(
        "_gold_generated_at", F.current_timestamp()
    )


def _write_contract_to_minio(context_id: str, contract: Mapping[str, Any]) -> str:
    import boto3

    key = f"metadata/contracts/{context_id}.json"
    client = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )
    client.put_object(
        Bucket=MINIO_BUCKET_NAME,
        Key=key,
        Body=json.dumps(contract, ensure_ascii=False, indent=2).encode("utf-8"),
        ContentType="application/json",
    )
    return key


def _update_routing_decision(
    decision_path: str,
    decision: RoutingDecision,
    context_id: str,
    plan,
    gold_df: DataFrame,
    silver_tables: Mapping[str, str],
    gold_table: str,
) -> None:
    dimensions = []
    metrics = []
    anchor_keys = set(plan.anchor_key)
    for field in gold_df.schema.fields:
        if field.name.startswith("_"):
            continue
        if isinstance(field.dataType, NumericType):
            metrics.append(field.name)
        elif isinstance(field.dataType, StringType) or field.name in anchor_keys:
            dimensions.append(field.name)

    decision.dataset_entity = f"{context_id}_context"
    decision.route_target = "relational_context"
    decision.target_silver_table = silver_tables[plan.anchor_entity]
    decision.target_quarantine_table = f"lakehouse.silver.{_safe_name(context_id)}__quarantine"
    decision.target_gold_table = gold_table
    decision.business_keys = list(plan.anchor_key)
    decision.dimension_columns = dimensions[:12]
    decision.metric_columns = metrics[:20]
    decision.reasoning = (
        f"Relational context '{context_id}' joined around anchor "
        f"'{plan.anchor_entity}' using key {list(plan.anchor_key)}."
    )
    with open(decision_path, "w", encoding="utf-8") as handle:
        handle.write(decision.model_dump_json(indent=2))


def process_relational_context(
    spark: SparkSession,
    manifest: Mapping[str, Any],
    decision: RoutingDecision,
    decision_path: str,
    run_id: str,
) -> Dict[str, Any]:
    context_id = _safe_name(str(manifest["context_id"]))
    bucket = str(manifest.get("bucket") or MINIO_BUCKET_NAME)
    entities = manifest.get("entities") or {}
    if not entities:
        raise ValueError(f"Context '{context_id}' has no tabular entities")

    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.silver")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.gold")

    frames: Dict[str, DataFrame] = {}
    profiles: List[EntityProfile] = []
    profile_evidence: Dict[str, Any] = {}
    for raw_entity, keys in sorted(entities.items()):
        entity = _safe_name(raw_entity)
        paths = [f"s3a://{bucket}/{key}" for key in keys]
        frame = standardize_dataframe_columns(spark.read.parquet(*paths)).cache()
        primary_key, confidence, evidence = infer_business_key(frame)
        frames[entity] = frame
        profiles.append(
            EntityProfile(
                entity=entity,
                columns=list(frame.columns),
                primary_key=primary_key,
                key_confidence=confidence,
                row_count=int(evidence["row_count"]),
                key_candidates=[
                    [column]
                    for column, stats in evidence.get("candidates", {}).items()
                    if stats.get("unique_ratio", 0) >= 0.98
                    and stats.get("non_null_ratio", 0) == 1.0
                ],
            )
        )
        profile_evidence[entity] = evidence
        print(
            f"🔑 [Context] {entity}: key={primary_key}, "
            f"confidence={confidence:.3f}, rows={evidence['row_count']}"
        )

    plan = build_context_plan(context_id, profiles)
    if not plan.anchor_key:
        raise ValueError(f"No usable anchor key found for relational context '{context_id}'")
    if any(
        profile.entity == plan.anchor_entity and profile.key_confidence < 0.75
        for profile in profiles
    ):
        raise ValueError(
            f"Anchor key for '{plan.anchor_entity}' is below the safe confidence threshold"
        )

    clean_run_id = "".join(char if char.isalnum() else "_" for char in run_id)[:16]
    branch_name = make_branch_name(f"ctx_{context_id}_{clean_run_id}")
    create_branch(spark, branch_name, from_ref="main")
    use_branch(spark, branch_name)

    silver_tables: Dict[str, str] = {}
    processed: Dict[str, int] = {}
    gold_table = f"lakehouse.gold.{context_id}_context"
    try:
        profile_by_entity = {profile.entity: profile for profile in profiles}
        for entity, frame in frames.items():
            table_name = f"lakehouse.silver.{context_id}__{entity}"
            silver_tables[entity] = table_name
            processed[entity] = merge_silver_entity(
                spark,
                frame,
                table_name,
                profile_by_entity[entity].primary_key,
            )
            print(f"✅ [Silver Context] {entity} -> {table_name}")

        gold_df = build_gold_context(spark, context_id, plan, silver_tables)
        gold_df.writeTo(gold_table).using("iceberg").createOrReplace()
        gold_count = gold_df.count()
        print(f"✅ [Gold Context] {gold_table}: {gold_count} rows")

        merge_branch_to_main(spark, branch_name)
        use_main(spark)

        contract = {
            "contract_version": 1,
            "context_id": context_id,
            "plan": plan.to_dict(),
            "profiles": [
                {
                    "entity": profile.entity,
                    "columns": list(profile.columns),
                    "primary_key": list(profile.primary_key),
                    "key_confidence": profile.key_confidence,
                    "row_count": profile.row_count,
                    "evidence": profile_evidence[profile.entity],
                }
                for profile in profiles
            ],
            "silver_tables": silver_tables,
            "gold_table": gold_table,
        }
        contract_key = _write_contract_to_minio(context_id, contract)
        _update_routing_decision(
            decision_path,
            decision,
            context_id,
            plan,
            gold_df,
            silver_tables,
            gold_table,
        )
        return {
            "status": "SUCCESS",
            "context_id": context_id,
            "anchor_entity": plan.anchor_entity,
            "anchor_key": list(plan.anchor_key),
            "processed": processed,
            "gold_table": gold_table,
            "gold_records": gold_count,
            "contract_key": contract_key,
            "disconnected_entities": list(plan.disconnected_entities),
        }
    except Exception:
        use_main(spark)
        raise
    finally:
        for frame in frames.values():
            frame.unpersist()


def main() -> None:
    parser = argparse.ArgumentParser(description="Process one Airbyte relational context")
    parser.add_argument("--manifest", required=True, help="Context manifest JSON")
    parser.add_argument("--decision-file", required=True, help="RoutingDecision JSON")
    parser.add_argument("--run-id", default="manual_run")
    args = parser.parse_args()

    manifest = load_manifest(args.manifest)
    with open(args.decision_file, "r", encoding="utf-8") as handle:
        decision = RoutingDecision(**json.load(handle))

    spark = get_spark_session(f"Relational_Context_{manifest['context_id']}")
    try:
        result = process_relational_context(
            spark,
            manifest,
            decision,
            args.decision_file,
            args.run_id,
        )
        print(json.dumps(result, ensure_ascii=False, indent=2))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
