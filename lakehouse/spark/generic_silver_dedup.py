# -*- coding: utf-8 -*-
"""Reusable deterministic Silver same-batch deduplication mechanics.

The ordering policy is preserved from the frozen Day 4 contract, while all
dataset-specific field names are supplied explicitly by the caller.
"""

from __future__ import annotations

from collections.abc import Sequence

from pyspark.sql import DataFrame, Window, functions as F


def deterministic_deduplicate(
    df: DataFrame,
    *,
    business_key: Sequence[str],
    source_updated_field: str,
    ingested_at_field: str,
    batch_id_field: str,
    checksum_field: str,
    record_id_field: str,
    row_number_column: str = "_silver_row_number",
) -> DataFrame:
    """Return one deterministic source row per configured business key.

    Frozen priority:
      1. newest source-updated timestamp
      2. newest ingestion timestamp
      3. descending batch identifier
      4. ascending checksum
      5. ascending record identifier
    """
    ordering = Window.partitionBy(*business_key).orderBy(
        F.col(source_updated_field).desc_nulls_last(),
        F.col(ingested_at_field).desc_nulls_last(),
        F.col(batch_id_field).desc_nulls_last(),
        F.col(checksum_field).asc_nulls_last(),
        F.col(record_id_field).asc_nulls_last(),
    )

    return (
        df.withColumn(
            row_number_column,
            F.row_number().over(ordering),
        )
        .filter(F.col(row_number_column) == 1)
        .drop(row_number_column)
    )
