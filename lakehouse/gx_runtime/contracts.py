"""Git-backed data contract loader and semantic safeguards."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any

from .errors import ValidationConfigurationError
from .models import ExecutionEngine


ALLOWED_RULE_TYPES = frozenset(
    {
        "schema",
        "freshness",
        "null_ratio",
        "duplicate_ratio",
        "volume",
        "range",
        "row_count_match",
        "key_set_match",
        "column_reconciliation",
    }
)
ALLOWED_SEVERITIES = frozenset({"critical", "warning"})
CONTRACT_STATUSES = frozenset({"draft", "review", "pilot", "active", "deprecated", "retired"})
RESOLVABLE_STATUSES = frozenset({"pilot", "active", "deprecated"})


@dataclass(frozen=True)
class ContractRule:
    rule_id: str
    rule_type: str
    severity: str
    supported_engines: frozenset[ExecutionEngine]
    config: dict[str, Any]


@dataclass(frozen=True)
class DataContract:
    dataset_id: str
    contract_version: str
    payload: dict[str, Any]
    rules: tuple[ContractRule, ...]

    def rules_for_engine(self, engine: ExecutionEngine) -> tuple[ContractRule, ...]:
        return tuple(rule for rule in self.rules if engine in rule.supported_engines)


@dataclass(frozen=True)
class ContractRegistration:
    dataset_id: str
    contract_version: str
    status: str
    owner: str
    effective_from: date
    effective_to: date | None
    definition: str
    supersedes: str | None = None
    approved_by: str | None = None
    approved_at: str | None = None

    def effective(self, as_of: date) -> bool:
        return self.effective_from <= as_of and (
            self.effective_to is None or as_of <= self.effective_to
        )


class ContractRegistry:
    """Resolve immutable contract versions without process-local registration."""

    def __init__(self, registry_path: str | Path) -> None:
        self.registry_path = Path(registry_path)
        try:
            payload = json.loads(self.registry_path.read_text(encoding="utf-8"))
            registrations = tuple(_parse_registration(item) for item in payload["contracts"])
            self._entries = {
                (item.dataset_id, item.contract_version): item
                for item in registrations
            }
        except (OSError, json.JSONDecodeError, KeyError, TypeError) as exc:
            raise ValidationConfigurationError(
                f"Unable to load contract registry {self.registry_path}"
            ) from exc
        if len(self._entries) != len(payload["contracts"]):
            raise ValidationConfigurationError(
                "Contracts must be unique by dataset and version"
            )
        self._validate_definitions_and_evolution()

    @staticmethod
    def _pilot_allowed() -> bool:
        return os.getenv("GX_ALLOW_PILOT_CONTRACTS", "true").strip().lower() == "true"

    def _resolvable(self, entry: ContractRegistration, as_of: date) -> bool:
        if not entry.effective(as_of) or entry.status not in RESOLVABLE_STATUSES:
            return False
        return entry.status != "pilot" or self._pilot_allowed()

    def _definition(self, entry: ContractRegistration) -> DataContract:
        path = (self.registry_path.parent / entry.definition).resolve()
        contract = load_contract(path)
        if (
            contract.dataset_id != entry.dataset_id
            or contract.contract_version != entry.contract_version
        ):
            raise ValidationConfigurationError(
                f"Contract registry identity does not match {path}"
            )
        if contract.payload.get("status") != entry.status:
            raise ValidationConfigurationError(
                f"Contract status does not match registry for "
                f"{entry.dataset_id}@{entry.contract_version}"
            )
        if contract.payload.get("owner") != entry.owner:
            raise ValidationConfigurationError(
                f"Contract owner does not match registry for "
                f"{entry.dataset_id}@{entry.contract_version}"
            )
        if contract.payload.get("effective_from") != entry.effective_from.isoformat():
            raise ValidationConfigurationError(
                f"Contract effective_from does not match registry for "
                f"{entry.dataset_id}@{entry.contract_version}"
            )
        return contract

    def _validate_definitions_and_evolution(self) -> None:
        by_dataset: dict[str, list[ContractRegistration]] = {}
        for entry in self._entries.values():
            by_dataset.setdefault(entry.dataset_id, []).append(entry)
            current = self._definition(entry)
            if entry.supersedes:
                previous = self._entries.get((entry.dataset_id, entry.supersedes))
                if previous is None:
                    raise ValidationConfigurationError(
                        f"Contract {entry.dataset_id}@{entry.contract_version} supersedes "
                        f"unknown version {entry.supersedes}"
                    )
                validate_contract_compatibility(self._definition(previous), current)
        for dataset_id, entries in by_dataset.items():
            ordered = sorted(entries, key=lambda item: _version_key(item.contract_version))
            for entry in ordered[1:]:
                if not entry.supersedes:
                    raise ValidationConfigurationError(
                        f"Contract {dataset_id}@{entry.contract_version} must declare supersedes"
                    )

    def effective_versions(
        self, dataset_id: str, *, as_of: date | None = None
    ) -> frozenset[str]:
        today = as_of or date.today()
        versions: set[str] = set()
        try:
            for (entry_dataset, version), entry in self._entries.items():
                if (
                    entry_dataset == dataset_id
                    and self._resolvable(entry, today)
                ):
                    versions.add(version)
        except (KeyError, TypeError, ValueError) as exc:
            raise ValidationConfigurationError(
                f"Invalid contract effective date in {self.registry_path}"
            ) from exc
        return frozenset(versions)

    def resolve(self, dataset_id: str, contract_version: str) -> DataContract:
        try:
            entry = self._entries[(dataset_id, contract_version)]
        except KeyError as exc:
            raise ValidationConfigurationError(
                f"Contract {dataset_id}@{contract_version} is not registered"
            ) from exc
        if not self._resolvable(entry, date.today()):
            raise ValidationConfigurationError(
                f"Contract {dataset_id}@{contract_version} is not active for validation "
                f"(status={entry.status})"
            )
        return self._definition(entry)


def _parse_registration(raw: dict[str, Any]) -> ContractRegistration:
    try:
        status = str(raw["status"])
        owner = str(raw["owner"]).strip()
        effective_from = date.fromisoformat(str(raw["effective_from"]))
        effective_to = (
            date.fromisoformat(str(raw["effective_to"]))
            if raw.get("effective_to")
            else None
        )
        approved_at = str(raw["approved_at"]) if raw.get("approved_at") else None
        if approved_at:
            approval_time = datetime.fromisoformat(approved_at.replace("Z", "+00:00"))
            if approval_time.tzinfo is None:
                raise ValueError("approved_at must include timezone")
        registration = ContractRegistration(
            dataset_id=str(raw["dataset_id"]),
            contract_version=str(raw["contract_version"]),
            status=status,
            owner=owner,
            effective_from=effective_from,
            effective_to=effective_to,
            definition=str(raw["definition"]),
            supersedes=str(raw["supersedes"]) if raw.get("supersedes") else None,
            approved_by=str(raw["approved_by"]).strip() if raw.get("approved_by") else None,
            approved_at=approved_at,
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise ValidationConfigurationError(f"Invalid contract registration: {raw!r}") from exc
    if status not in CONTRACT_STATUSES or not owner:
        raise ValidationConfigurationError("Contract status and owner must be governed")
    if effective_to is not None and effective_to < effective_from:
        raise ValidationConfigurationError("Contract effective_to precedes effective_from")
    if status == "active" and (not registration.approved_by or not registration.approved_at):
        raise ValidationConfigurationError(
            f"Active contract {registration.dataset_id}@{registration.contract_version} "
            "requires approved_by and approved_at"
        )
    return registration


def validate_contract_compatibility(previous: DataContract, current: DataContract) -> None:
    """Reject breaking evolution unless the new contract has a higher major version."""
    breaking: list[str] = []
    previous_layers = previous.payload.get("layers", {})
    current_layers = current.payload.get("layers", {})
    for layer, old_layer in previous_layers.items():
        new_layer = current_layers.get(layer)
        if not isinstance(new_layer, dict):
            breaking.append(f"removed layer {layer}")
            continue
        old_columns = old_layer.get("columns", {})
        new_columns = new_layer.get("columns", {})
        for column, old_definition in old_columns.items():
            new_definition = new_columns.get(column)
            if new_definition is None:
                breaking.append(f"removed column {layer}.{column}")
                continue
            if new_definition.get("type") != old_definition.get("type"):
                breaking.append(f"changed type {layer}.{column}")
            if old_definition.get("nullable") is True and new_definition.get("nullable") is False:
                breaking.append(f"made required {layer}.{column}")
    old_key = previous.payload.get("grain", {}).get("candidate_business_key")
    new_key = current.payload.get("grain", {}).get("candidate_business_key")
    if old_key != new_key:
        breaking.append("changed candidate business key")
    try:
        old_major = int(previous.contract_version.split(".", 1)[0])
        new_major = int(current.contract_version.split(".", 1)[0])
    except ValueError as exc:
        raise ValidationConfigurationError("Contract versions must begin with a numeric major") from exc
    if breaking and new_major <= old_major:
        raise ValidationConfigurationError(
            "Breaking contract changes require a new major version: " + ", ".join(breaking)
        )


def _version_key(value: str) -> tuple[tuple[int, object], ...]:
    return tuple(
        (0, int(part)) if part.isdigit() else (1, part.lower())
        for part in value.replace("-", ".").split(".")
    )


def _validate_ratio(rule: ContractRule) -> None:
    if rule.rule_type not in {"null_ratio", "duplicate_ratio"}:
        return
    ratio = rule.config.get("threshold", {}).get("max_ratio")
    if not isinstance(ratio, (int, float)) or not 0 <= ratio <= 1:
        raise ValidationConfigurationError(
            f"Rule {rule.rule_id} max_ratio must be between 0 and 1"
        )


def _parse_rule(raw: dict[str, Any]) -> ContractRule:
    try:
        rule_type = raw["type"]
        severity = raw["severity"]
        engines = frozenset(
            ExecutionEngine(value) for value in raw["supported_engines"]
        )
        rule = ContractRule(
            rule_id=raw["id"],
            rule_type=rule_type,
            severity=severity,
            supported_engines=engines,
            config={
                key: value
                for key, value in raw.items()
                if key not in {"id", "type", "severity", "supported_engines"}
            },
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise ValidationConfigurationError(f"Invalid contract rule: {raw!r}") from exc
    if rule.rule_type not in ALLOWED_RULE_TYPES:
        raise ValidationConfigurationError(
            f"Unsupported rule type {rule.rule_type!r} in {rule.rule_id}"
        )
    if rule.severity not in ALLOWED_SEVERITIES or not rule.supported_engines:
        raise ValidationConfigurationError(
            f"Invalid severity or engines in rule {rule.rule_id}"
        )
    _validate_ratio(rule)
    return rule


def load_contract(path: str | Path) -> DataContract:
    contract_path = Path(path)
    try:
        payload = json.loads(contract_path.read_text(encoding="utf-8"))
        layer_rules = [
            rule
            for layer in payload["layers"].values()
            for rule in layer["rules"]
        ]
        rules = tuple(
            _parse_rule(rule)
            for rule in layer_rules + payload.get("reconciliation_rules", [])
        )
    except (OSError, json.JSONDecodeError, KeyError, TypeError) as exc:
        raise ValidationConfigurationError(
            f"Unable to load data contract {contract_path}"
        ) from exc
    rule_ids = [rule.rule_id for rule in rules]
    if len(rule_ids) != len(set(rule_ids)):
        raise ValidationConfigurationError("Contract rule IDs must be unique")
    if payload.get("status") not in CONTRACT_STATUSES or not str(payload.get("owner") or "").strip():
        raise ValidationConfigurationError("Contract must declare a governed status and owner")
    grain = payload.get("grain", {})
    if not grain.get("business_semantics_verified", False):
        critical_uniqueness = [
            rule.rule_id
            for rule in rules
            if rule.rule_type == "duplicate_ratio" and rule.severity == "critical"
        ]
        if critical_uniqueness:
            raise ValidationConfigurationError(
                "Critical uniqueness requires business-verified grain: "
                + ", ".join(critical_uniqueness)
            )
    return DataContract(
        dataset_id=payload["dataset_id"],
        contract_version=payload["contract_version"],
        payload=payload,
        rules=rules,
    )
