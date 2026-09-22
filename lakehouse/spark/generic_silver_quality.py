# -*- coding: utf-8 -*-
"""Reusable Silver data-quality execution mechanics.

This module executes declarative DQ rules supplied by dataset-specific code.
It intentionally contains no Learning Outcomes field names or business rules.
"""

from __future__ import annotations

from collections.abc import Sequence

from pyspark.sql import DataFrame, functions as F


DQRule = tuple[str, str]


def add_dq_reasons(
    df: DataFrame,
    rules: Sequence[DQRule],
    *,
    reasons_column: str = "_dq_reasons",
) -> DataFrame:
    """Attach ordered reason codes for every configured rule that fails."""
    reason_columns = [
        F.when(F.expr(invalid_expression), F.lit(reason_code))
        for reason_code, invalid_expression in rules
    ]

    return df.withColumn(
        reasons_column,
        F.array_compact(F.array(*reason_columns)),
    )


def split_dq(
    df: DataFrame,
    rules: Sequence[DQRule],
    *,
    reasons_column: str = "_dq_reasons",
    rejection_reason_column: str = "rejection_reason",
    separator: str = "|",
) -> tuple[DataFrame, DataFrame]:
    """Split rows into DQ-valid and rejected outputs using configured rules."""
    evaluated = add_dq_reasons(
        df,
        rules,
        reasons_column=reasons_column,
    )

    valid = (
        evaluated
        .filter(F.size(F.col(reasons_column)) == 0)
        .drop(reasons_column)
    )

    invalid = (
        evaluated
        .filter(F.size(F.col(reasons_column)) > 0)
        .withColumn(
            rejection_reason_column,
            F.concat_ws(separator, F.col(reasons_column)),
        )
        .drop(reasons_column)
    )

    return valid, invalid
