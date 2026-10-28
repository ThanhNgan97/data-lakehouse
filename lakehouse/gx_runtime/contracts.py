"""Git-backed data contract loader and semantic safeguards."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date
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


class ContractRegistry:
    """Resolve immutable contract versions without process-local registration."""

    def __init__(self, registry_path: str | Path) -> None:
        self.registry_path = Path(registry_path)
        try:
            payload = json.loads(self.registry_path.read_text(encoding="utf-8"))
            self._entries = {
                (item["dataset_id"], item["contract_version"]): item
                for item in payload["contracts"]
            }
        except (OSError, json.JSONDecodeError, KeyError, TypeError) as exc:
            raise ValidationConfigurationError(
                f"Unable to load contract registry {self.registry_path}"
            ) from exc
        if len(self._entries) != len(payload["contracts"]):
            raise ValidationConfigurationError(
                "Contracts must be unique by dataset and version"
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
                    and date.fromisoformat(entry["effective_from"]) <= today
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
        path = (self.registry_path.parent / entry["definition"]).resolve()
        contract = load_contract(path)
        if (
            contract.dataset_id != dataset_id
            or contract.contract_version != contract_version
        ):
            raise ValidationConfigurationError(
                f"Contract registry identity does not match {path}"
            )
        return contract


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
