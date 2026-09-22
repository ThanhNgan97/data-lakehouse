# -*- coding: utf-8 -*-
"""Reusable Silver Iceberg MERGE mechanics.

The caller supplies all dataset-specific table/field identities. This module
preserves the frozen Day 4 MERGE contract:
- match on the configured business key;
- update only when the source timestamp is newer;
- insert only unmatched, non-deleted rows;
- never issue a physical DELETE.
"""

from __future__ import annotations

from collections.abc import Sequence

from pyspark.sql import DataFrame, SparkSession, functions as F


def business_key_sql(
    business_key: Sequence[str],
    *,
    source_alias: str = "s",
    target_alias: str = "t",
) -> str:
    """Build the configured business-key equality condition."""
    if not business_key:
        raise ValueError("business_key must not be empty")

    return " AND ".join(
        f"{target_alias}.{column} = {source_alias}.{column}"
        for column in business_key
    )


def build_merge_sql(
    *,
    target_table: str,
    source_view: str,
    business_key: Sequence[str],
    source_columns: Sequence[str],
    source_updated_field: str,
    delete_field: str,
    silver_updated_at_field: str = "_silver_updated_at",
) -> str:
    """Build the frozen newer-only / no-orphan-delete MERGE statement."""
    all_columns = list(source_columns) + [silver_updated_at_field]

    update_assignments = ",\n".join(
        f"t.{column} = s.{column}"
        for column in all_columns
    )

    insert_column_sql = ", ".join(all_columns)
    insert_value_sql = ", ".join(
        f"s.{column}"
        for column in all_columns
    )

    return f"""
        MERGE INTO {target_table} t
        USING {source_view} s
        ON {business_key_sql(business_key, source_alias="s", target_alias="t")}

        WHEN MATCHED
          AND s.{source_updated_field} > t.{source_updated_field}
        THEN UPDATE SET
          {update_assignments}

        WHEN NOT MATCHED
          AND s.{delete_field} = false
        THEN INSERT (
          {insert_column_sql}
        )
        VALUES (
          {insert_value_sql}
        )
    """


def merge_into_silver(
    spark: SparkSession,
    df: DataFrame,
    *,
    target_table: str,
    business_key: Sequence[str],
    source_columns: Sequence[str],
    source_updated_field: str,
    delete_field: str,
    source_view: str,
    silver_updated_at_field: str = "_silver_updated_at",
) -> int:
    """Execute the frozen Silver MERGE mechanics for configured inputs."""
    row_count = df.count()

    if row_count == 0:
        return 0

    merge_source = (
        df.withColumn(
            silver_updated_at_field,
            F.current_timestamp(),
        )
        .select(
            *source_columns,
            silver_updated_at_field,
        )
    )

    merge_source.createOrReplaceTempView(source_view)

    merge_sql = build_merge_sql(
        target_table=target_table,
        source_view=source_view,
        business_key=business_key,
        source_columns=source_columns,
        source_updated_field=source_updated_field,
        delete_field=delete_field,
        silver_updated_at_field=silver_updated_at_field,
    )

    spark.sql(merge_sql)
    return row_count
