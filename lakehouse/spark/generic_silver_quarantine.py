# -*- coding: utf-8 -*-
"""Reusable Silver quarantine routing mechanics.

This module formats configured business keys, attaches quarantine metadata,
selects the configured quarantine schema, and appends rows to a configured
target table. It intentionally owns no dataset-specific field names or rules.
"""

from __future__ import annotations

from collections.abc import Sequence

from pyspark.sql import DataFrame, functions as F


def add_quarantine_metadata(
    df: DataFrame,
    business_key: Sequence[str],
    *,
    business_key_column: str = "_business_key",
    rejected_at_column: str = "rejected_at",
    separator: str = "|",
    null_sentinel: str = "<NULL>",
) -> DataFrame:
    """Attach deterministic business-key text and rejection timestamp."""
    business_key_expr = F.concat_ws(
        separator,
        *[
            F.coalesce(
                F.col(field_name).cast("string"),
                F.lit(null_sentinel),
            )
            for field_name in business_key
        ],
    )

    return (
        df.withColumn(business_key_column, business_key_expr)
        .withColumn(rejected_at_column, F.current_timestamp())
    )


def prepare_quarantine_rows(
    df: DataFrame,
    *,
    business_key: Sequence[str],
    output_columns: Sequence[str],
    business_key_column: str = "_business_key",
    rejected_at_column: str = "rejected_at",
    separator: str = "|",
    null_sentinel: str = "<NULL>",
) -> DataFrame:
    """Prepare rejected rows using the configured quarantine output schema."""
    return add_quarantine_metadata(
        df,
        business_key,
        business_key_column=business_key_column,
        rejected_at_column=rejected_at_column,
        separator=separator,
        null_sentinel=null_sentinel,
    ).select(*output_columns)


def write_quarantine_rows(
    df: DataFrame,
    *,
    target_table: str,
    business_key: Sequence[str],
    output_columns: Sequence[str],
    business_key_column: str = "_business_key",
    rejected_at_column: str = "rejected_at",
    separator: str = "|",
    null_sentinel: str = "<NULL>",
) -> int:
    """Append configured rejected rows to a configured quarantine table."""
    prepared = prepare_quarantine_rows(
        df,
        business_key=business_key,
        output_columns=output_columns,
        business_key_column=business_key_column,
        rejected_at_column=rejected_at_column,
        separator=separator,
        null_sentinel=null_sentinel,
    )

    row_count = prepared.count()

    if row_count == 0:
        return 0

    prepared.writeTo(target_table).append()
    return row_count
