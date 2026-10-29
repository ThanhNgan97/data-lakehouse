"""Read-only GX route discovery for Airflow-side quality gates.

The GX service remains authoritative for contract and suite validation.  This
module lets ingestion DAGs discover the same dataset/layer route without
duplicating dataset names or contract versions in Python.
"""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any


class GXRouteNotFoundError(LookupError):
    """No effective validation route covers the requested asset."""


class GXRouteConfigurationError(RuntimeError):
    """The mounted GX registries are malformed or internally inconsistent."""


@dataclass(frozen=True)
class GXRoutePlan:
    dataset_id: str
    data_layer: str
    contract_version: str
    data_asset_ref: str
    source_kind: str
    execution_engine: str
    coverage: str = "CONTRACT"
    contract_status: str = "APPROVED"


def default_config_root() -> Path:
    configured = os.getenv("GX_CONFIG_ROOT")
    if configured:
        return Path(configured)
    container_root = Path("/opt/airflow/gx-config")
    if container_root.is_dir():
        return container_root
    return Path(__file__).resolve().parents[1] / "gx"


def _load(path: Path, collection: str) -> list[dict[str, Any]]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        items = payload[collection]
        if not isinstance(items, list):
            raise TypeError(collection)
        return items
    except (OSError, json.JSONDecodeError, KeyError, TypeError) as exc:
        raise GXRouteConfigurationError(f"Unable to load GX registry {path}") from exc


def _version_key(value: str) -> tuple[tuple[int, Any], ...]:
    parts = value.replace("-", ".").split(".")
    return tuple(
        (0, int(part)) if part.isdigit() else (1, part.lower())
        for part in parts
    )


def resolve_route(
    *,
    dataset_id: str,
    data_layer: str,
    source_kind: str,
    execution_engine: str,
    config_root: str | Path | None = None,
    as_of: date | None = None,
) -> GXRoutePlan:
    root = Path(config_root) if config_root is not None else default_config_root()
    contracts = _load(root / "contract_registry.json", "contracts")
    routes = _load(root / "validation_registry.json", "routes")
    today = as_of or date.today()
    allow_pilot = os.getenv("GX_ALLOW_PILOT_CONTRACTS", "true").strip().lower() == "true"

    effective_contracts: dict[tuple[str, str], dict[str, Any]] = {}
    for contract in contracts:
        try:
            key = (str(contract["dataset_id"]), str(contract["contract_version"]))
            effective = date.fromisoformat(str(contract["effective_from"]))
            status = str(contract["status"])
            effective_to = (
                date.fromisoformat(str(contract["effective_to"]))
                if contract.get("effective_to")
                else None
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise GXRouteConfigurationError("Invalid GX contract registry entry") from exc
        if key in effective_contracts:
            raise GXRouteConfigurationError(f"Duplicate GX contract {key[0]}@{key[1]}")
        resolvable = status in {"active", "deprecated"} or (
            status == "pilot" and allow_pilot
        )
        if effective <= today and (effective_to is None or today <= effective_to) and resolvable:
            effective_contracts[key] = contract

    candidates: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str]] = set()
    for route in routes:
        try:
            key = (
                str(route["dataset_id"]),
                str(route["layer"]),
                str(route["contract_version"]),
            )
            sources = {str(value) for value in route["supported_source_kinds"]}
            engines = route["suites"]
            asset = str(route["data_asset_ref"]).strip()
        except (KeyError, TypeError) as exc:
            raise GXRouteConfigurationError("Invalid GX validation route entry") from exc
        if key in seen:
            raise GXRouteConfigurationError(
                f"Duplicate GX validation route {key[0]}/{key[1]}@{key[2]}"
            )
        seen.add(key)
        if not asset or not isinstance(engines, dict):
            raise GXRouteConfigurationError("GX validation route has no asset or suites")
        if (
            key[0] == dataset_id
            and key[1] == data_layer
            and source_kind in sources
            and execution_engine in engines
            and (key[0], key[2]) in effective_contracts
        ):
            candidates.append(route)

    if not candidates:
        raise GXRouteNotFoundError(
            f"No effective GX route for {dataset_id}/{data_layer} "
            f"source={source_kind} engine={execution_engine}"
        )
    selected = max(candidates, key=lambda item: _version_key(str(item["contract_version"])))
    return GXRoutePlan(
        dataset_id=dataset_id,
        data_layer=data_layer,
        contract_version=str(selected["contract_version"]),
        data_asset_ref=str(selected["data_asset_ref"]),
        source_kind=source_kind,
        execution_engine=execution_engine,
    )


_SILVER_ASSET_PATTERN = re.compile(
    r"^lakehouse\.silver\.[A-Za-z_][A-Za-z0-9_]*$"
)


def resolve_route_or_baseline(
    *,
    dataset_id: str | None,
    data_asset_ref: str | None,
    source_kind: str,
    execution_engine: str = "sql",
    config_root: str | Path | None = None,
    as_of: date | None = None,
) -> GXRoutePlan:
    """Resolve governed coverage, falling back to the shared Silver baseline."""
    asset = str(data_asset_ref or "")
    try:
        route = resolve_route(
            dataset_id=str(dataset_id or ""),
            data_layer="silver",
            source_kind=source_kind,
            execution_engine=execution_engine,
            config_root=config_root,
            as_of=as_of,
        )
    except GXRouteNotFoundError:
        if not _SILVER_ASSET_PATTERN.fullmatch(asset):
            raise GXRouteConfigurationError(
                f"Baseline GX requires a safe Silver asset, received {asset!r}"
            )
        fallback_id = str(dataset_id or f"dynamic.{asset.rsplit('.', 1)[-1]}")
        return GXRoutePlan(
            dataset_id=fallback_id,
            data_layer="silver",
            contract_version="baseline-1.0.0",
            data_asset_ref=asset,
            source_kind=source_kind,
            execution_engine=execution_engine,
            coverage="BASELINE",
            contract_status="MISSING",
        )
    if asset != route.data_asset_ref:
        raise GXRouteConfigurationError(
            "Routed Silver asset does not match the approved GX route: "
            f"{asset!r} != {route.data_asset_ref!r}"
        )
    return route
