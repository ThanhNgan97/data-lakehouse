# -*- coding: utf-8 -*-
"""Reusable Silver equal-timestamp source-conflict mechanics.

This module preserves the frozen Day 4 behavior:
if one configured business key has competing checksums at the same source
timestamp, every row for that business key in the current batch is blocked.
Dataset-specific field names are supplied explicitly by the caller.
"""

from __future__ import annotations

from collections.abc import Sequence

from pyspark.sql import DataFrame, functions as F


def split_equal_timestamp_conflicts(
    df: DataFrame,
    *,
    business_key: Sequence[str],
    source_updated_field: str,
    checksum_field: str,
    rejection_reason: str = "EQUAL_TIMESTAMP_DIFFERENT_CHECKSUM",
    rejection_reason_column: str = "rejection_reason",
    null_checksum_sentinel: str = "__NULL_CHECKSUM__",
) -> tuple[DataFrame, DataFrame]:
    """Split mergeable rows from frozen whole-business-key conflicts.

    Frozen behavior:
    1. Detect groups sharing business key + source timestamp.
    2. A group conflicts when it contains more than one checksum variant.
    3. Collapse conflicts to distinct business keys.
    4. Block *all* current-batch rows for every conflicting business key.
    """
    checksum_value = F.coalesce(
        F.col(checksum_field),
        F.lit(null_checksum_sentinel),
    )

    conflict_groups = (
        df.groupBy(*business_key, source_updated_field)
        .agg(
            F.countDistinct(checksum_value).alias("_checksum_variants")
        )
        .filter(F.col("_checksum_variants") > 1)
        .select(*business_key, source_updated_field)
    )

    conflict_keys = conflict_groups.select(*business_key).distinct()

    conflict_rows = (
        df.join(
            conflict_keys,
            on=list(business_key),
            how="inner",
        )
        .withColumn(
            rejection_reason_column,
            F.lit(rejection_reason),
        )
    )

    mergeable = df.join(
        conflict_keys,
        on=list(business_key),
        how="left_anti",
    )

    return mergeable, conflict_rows
