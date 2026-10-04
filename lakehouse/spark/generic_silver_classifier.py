# -*- coding: utf-8 -*-
"""Reusable Silver target-state classification mechanics.

This module classifies source rows against the current Silver target using
caller-supplied field/table configuration. It preserves the frozen Day 4
new/update/stale/duplicate/conflict/delete semantics for datasets that expose
a delete flag, while also supporting datasets with no source delete semantics.
"""

from __future__ import annotations

from collections.abc import Sequence

from pyspark.sql import DataFrame, SparkSession, functions as F


def classify_against_target(
    spark: SparkSession,
    df: DataFrame,
    *,
    target_table: str,
    business_key: Sequence[str],
    source_updated_field: str,
    checksum_field: str,
    delete_field: str | None,
    source_columns: Sequence[str],
    delete_without_target_reason: str = "DELETE_WITHOUT_EXISTING_TARGET",
    equal_timestamp_conflict_reason: str = (
        "EQUAL_TIMESTAMP_DIFFERENT_CHECKSUM"
    ),
    null_checksum_sentinel: str = "__NULL_CHECKSUM__",
) -> tuple[DataFrame, DataFrame, DataFrame, DataFrame]:
    """Classify source rows against a configured Silver target.

    Returns:
        mergeable,
        quarantine,
        stale,
        duplicate

    Semantics with a configured delete field:
      - new non-deleted row -> mergeable
      - existing target + newer source timestamp -> mergeable
      - existing target + older source timestamp -> stale
      - equal timestamp + same checksum -> duplicate
      - equal timestamp + different checksum -> quarantine
      - deleted row without target -> quarantine

    Semantics when ``delete_field`` is ``None``:
      - every source row is treated as active for delete handling
      - delete-without-target routing is disabled
      - timestamp/checksum classification is unchanged
    """
    if not business_key:
        raise ValueError("business_key must not be empty")

    target_key_aliases = tuple(
        f"_target_key_{index}"
        for index, _ in enumerate(business_key)
    )

    target = spark.table(target_table).select(
        *[
            F.col(field_name).alias(alias_name)
            for field_name, alias_name in zip(
                business_key,
                target_key_aliases,
            )
        ],
        F.col(source_updated_field).alias("_target_updated_at"),
        F.col(checksum_field).alias("_target_record_checksum"),
    )

    source = df.alias("s")
    target = target.alias("t")

    join_condition = None
    for source_field, target_alias in zip(
        business_key,
        target_key_aliases,
    ):
        term = (
            F.col(f"s.{source_field}")
            == F.col(f"t.{target_alias}")
        )
        join_condition = (
            term
            if join_condition is None
            else join_condition & term
        )

    joined = source.join(
        target,
        join_condition,
        "left",
    )

    # Preserve the current Spark 3.5 + Iceberg/DataSource V2 workaround:
    # materialize locally before applying classification filters.
    joined = joined.localCheckpoint(eager=True)

    target_exists = F.col(target_key_aliases[0]).isNotNull()

    source_updated_at = F.col(source_updated_field)
    target_updated_at = F.col("_target_updated_at")

    source_checksum = F.coalesce(
        F.col(checksum_field),
        F.lit(null_checksum_sentinel),
    )
    target_checksum = F.coalesce(
        F.col("_target_record_checksum"),
        F.lit(null_checksum_sentinel),
    )

    same_checksum = source_checksum == target_checksum
    different_checksum = source_checksum != target_checksum

    if delete_field is None:
        source_is_deleted = F.lit(False)
    else:
        source_is_deleted = F.coalesce(
            F.col(delete_field),
            F.lit(False),
        )

    delete_without_target_condition = (
        (~target_exists)
        & source_is_deleted
    )

    equal_timestamp_conflict_condition = (
        target_exists
        & (source_updated_at == target_updated_at)
        & different_checksum
    )

    stale_condition = (
        target_exists
        & (source_updated_at < target_updated_at)
    )

    duplicate_condition = (
        target_exists
        & (source_updated_at == target_updated_at)
        & same_checksum
    )

    mergeable_condition = (
        (
            (~target_exists)
            & (~source_is_deleted)
        )
        |
        (
            target_exists
            & (source_updated_at > target_updated_at)
        )
    )

    mergeable = (
        joined
        .filter(mergeable_condition)
        .select(*source_columns)
    )

    delete_without_target = (
        joined
        .filter(delete_without_target_condition)
        .select(*source_columns)
        .withColumn(
            "rejection_reason",
            F.lit(delete_without_target_reason),
        )
    )

    equal_timestamp_conflicts = (
        joined
        .filter(equal_timestamp_conflict_condition)
        .select(*source_columns)
        .withColumn(
            "rejection_reason",
            F.lit(equal_timestamp_conflict_reason),
        )
    )

    quarantine = delete_without_target.unionByName(
        equal_timestamp_conflicts
    )

    stale = (
        joined
        .filter(stale_condition)
        .select(*source_columns)
    )

    duplicate = (
        joined
        .filter(duplicate_condition)
        .select(*source_columns)
    )

    return mergeable, quarantine, stale, duplicate
