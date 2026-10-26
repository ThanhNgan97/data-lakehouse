"""Reusable GX execution against in-memory Spark DataFrames."""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any

import great_expectations as gx
from great_expectations.core import RunIdentifier

from .exceptions import (
    GXExecutionError,
    GXProjectConfigurationError,
    GXSuiteNotFoundError,
)
from .gx_result import GXExpectationFailure, GXValidationSummary
from .validation_policy import ValidationPolicy


DEFAULT_CONTEXT_ROOT = Path("/opt/airflow/gx")


def _safe_name(value: str) -> str:
    normalized = re.sub(r"[^a-zA-Z0-9_]+", "_", value).strip("_").lower()
    if not normalized:
        raise ValueError("GX resource names must contain an alphanumeric character")
    return normalized[:120]


def resolve_context_root(context_root: str | Path | None = None) -> Path:
    configured = context_root or os.getenv("GX_CONTEXT_ROOT")
    return Path(configured) if configured else DEFAULT_CONTEXT_ROOT


def load_file_context(context_root: str | Path | None = None):
    """Load the repository File Data Context and reject implicit fallback."""
    root = resolve_context_root(context_root).resolve()
    config_file = root / "great_expectations.yml"
    if not config_file.is_file():
        raise GXProjectConfigurationError(
            f"GX configuration not found at '{config_file}'"
        )

    try:
        context = gx.get_context(
            mode="file",
            project_root_dir=str(root.parent),
        )
    except Exception as exc:
        raise GXProjectConfigurationError(
            f"Unable to load GX File Data Context from '{root}'"
        ) from exc

    loaded_root = Path(context.root_directory).resolve()
    if loaded_root != root:
        raise GXProjectConfigurationError(
            f"GX loaded unexpected context '{loaded_root}', expected '{root}'"
        )
    return context


def _get_or_add_spark_resources(context, dataset_id: str, data_layer: str):
    base_name = _safe_name(f"{data_layer}_{dataset_id}")
    source_name = f"spark_{base_name}"
    asset_name = f"asset_{base_name}"
    batch_name = f"batch_{base_name}"

    try:
        data_source = context.data_sources.get(source_name)
    except Exception:
        data_source = context.data_sources.add_spark(name=source_name)

    try:
        asset = data_source.get_asset(asset_name)
    except Exception:
        asset = data_source.add_dataframe_asset(name=asset_name)

    try:
        batch_definition = asset.get_batch_definition(batch_name)
    except Exception:
        batch_definition = asset.add_batch_definition_whole_dataframe(
            name=batch_name,
        )

    return batch_definition


def _expectation_type(config: Any) -> str:
    return str(
        getattr(config, "type", None)
        or getattr(config, "expectation_type", None)
        or config.__class__.__name__
    )


def _failure_from_result(result: Any, policy: ValidationPolicy):
    config = result.expectation_config
    kwargs = getattr(config, "kwargs", {}) or {}
    severity = policy.normalize_severity(getattr(config, "severity", None))
    return GXExpectationFailure(
        expectation_type=_expectation_type(config),
        severity=severity,
        column=kwargs.get("column"),
        observed_value=(getattr(result, "result", {}) or {}).get(
            "observed_value"
        ),
    )


def validate_dataframe(
    dataframe,
    *,
    suite_names: list[str] | tuple[str, ...],
    dataset_id: str,
    data_layer: str,
    run_id: str,
    table_name: str | None = None,
    nessie_branch: str | None = None,
    context_root: str | Path | None = None,
    policy: ValidationPolicy | None = None,
) -> GXValidationSummary:
    """Validate one Spark DataFrame with one or more persistent suites."""
    requested_suites = tuple(dict.fromkeys(suite_names))
    if not requested_suites:
        raise GXSuiteNotFoundError("At least one Expectation Suite is required")

    validation_policy = policy or ValidationPolicy()
    context = load_file_context(context_root)
    batch_definition = _get_or_add_spark_resources(
        context,
        dataset_id,
        data_layer,
    )

    validation_definitions = []
    for suite_name in requested_suites:
        try:
            suite = context.suites.get(suite_name)
        except Exception as exc:
            raise GXSuiteNotFoundError(
                f"GX suite '{suite_name}' is not registered"
            ) from exc

        definition_name = _safe_name(
            f"{data_layer}_{dataset_id}_{suite_name}_validation"
        )
        try:
            definition = context.validation_definitions.get(definition_name)
        except Exception:
            definition = context.validation_definitions.add(
                gx.ValidationDefinition(
                    name=definition_name,
                    data=batch_definition,
                    suite=suite,
                )
            )
        validation_definitions.append(definition)

    checkpoint_name = _safe_name(
        f"{data_layer}_{dataset_id}_checkpoint"
    )
    try:
        checkpoint = context.checkpoints.get(checkpoint_name)
    except Exception:
        checkpoint = context.checkpoints.add(
            gx.Checkpoint(
                name=checkpoint_name,
                validation_definitions=validation_definitions,
            )
        )

    try:
        checkpoint_result = checkpoint.run(
            batch_parameters={"dataframe": dataframe},
            run_id=RunIdentifier(run_name=run_id),
        )
    except Exception as exc:
        raise GXExecutionError(
            f"GX validation failed to execute for '{dataset_id}'"
        ) from exc

    evaluated = 0
    successful = 0
    critical_failures = []
    warning_failures = []
    result_ids = []

    for result_id, validation_result in checkpoint_result.run_results.items():
        result_ids.append(str(result_id))
        for expectation_result in validation_result.results:
            evaluated += 1
            if expectation_result.success:
                successful += 1
                continue
            failure = _failure_from_result(
                expectation_result,
                validation_policy,
            )
            if validation_policy.is_blocking(failure.severity):
                critical_failures.append(failure)
            else:
                warning_failures.append(failure)

    failed = evaluated - successful
    return GXValidationSummary(
        success=not critical_failures,
        raw_gx_success=bool(checkpoint_result.success),
        dataset_id=dataset_id,
        data_layer=data_layer,
        run_id=run_id,
        suite_names=requested_suites,
        evaluated_expectations=evaluated,
        successful_expectations=successful,
        failed_expectations=failed,
        critical_failures=tuple(critical_failures),
        warning_failures=tuple(warning_failures),
        validation_result_ids=tuple(result_ids),
        table_name=table_name,
        nessie_branch=nessie_branch,
    )
