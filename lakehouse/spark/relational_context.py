# -*- coding: utf-8 -*-
"""Pure planning helpers for multi-table relational contexts landed in MinIO.

The module intentionally has no Spark/Airflow dependency so discovery and
relationship planning can be unit-tested without starting the data plane.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
from itertools import combinations
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence


SUPPORTED_TABULAR_EXTENSIONS = (".parquet", ".json", ".csv", ".tsv")
TECHNICAL_COLUMN_PREFIXES = (
    "_airbyte_",
    "airbyte_",
    "_ab_cdc_",
    "ab_cdc_",
    "_context_",
    "context_",
)

DOMAIN_KEYWORDS = {
    "education": (
        "student", "sinh_vien", "hoc_phan", "mon_hoc", "giang_day",
        "diem_danh", "hoc_ky", "nam_hoc", "giang_vien", "dao_tao",
    ),
    "finance": ("payment", "invoice", "revenue", "amount", "tuition", "hoc_phi", "thanh_toan"),
    "hr": ("employee", "staff", "nhan_vien", "can_bo", "department", "phong_ban"),
    "commerce": ("order", "product", "customer", "sales", "don_hang", "san_pham", "khach_hang"),
    "iot": ("iot", "sensor", "telemetry", "device_log", "cam_bien", "scada"),
    "healthcare": ("patient", "clinical", "hospital", "benh_nhan", "y_te"),
}

PERFORMANCE_KEYWORDS = (
    "ty_le", "rate", "score", "progress", "tien_do", "completion",
    "attendance", "diem_danh", "satisfaction", "hai_long", "risk", "sla",
)

GENUINE_TIME_KEYWORDS = (
    "event_time", "event_at", "occurred_at", "transaction_at", "paid_at",
    "created_at", "ngay_yeu_cau", "ngay_hoan_thanh", "timestamp",
)


def is_technical_column(column: str) -> bool:
    clean = column.lower()
    return clean.startswith(TECHNICAL_COLUMN_PREFIXES)


def infer_context_semantics(
    context_id: str,
    entity_names: Sequence[str],
    columns: Sequence[str],
) -> Dict[str, Any]:
    """Produce an auditable domain/template decision from business-only names."""
    business_columns = [c.lower() for c in columns if not is_technical_column(c)]
    corpus = " ".join([context_id.lower(), *[e.lower() for e in entity_names], *business_columns])
    domain_scores = {
        domain: sum(1 for keyword in keywords if keyword in corpus)
        for domain, keywords in DOMAIN_KEYWORDS.items()
    }
    best_domain, best_score = max(domain_scores.items(), key=lambda item: item[1])
    domain = best_domain if best_score else "generic"

    performance_signals = sorted({k for k in PERFORMANCE_KEYWORDS if k in corpus})
    time_signals = sorted({k for k in GENUINE_TIME_KEYWORDS if k in corpus})
    if domain == "iot" and time_signals:
        archetype = "time_series"
        signals = [f"domain:{domain}", *[f"time:{s}" for s in time_signals[:3]]]
    elif performance_signals:
        risk_signals = [k for k in ("risk", "sla", "benchmark", "target") if k in corpus]
        archetype = "performance_risk" if risk_signals else "operational_performance"
        signals = [f"domain:{domain}", *[f"performance:{s}" for s in performance_signals[:5]]]
    elif len(entity_names) > 1:
        archetype = "categorical_distribution"
        signals = [f"domain:{domain}", f"relational_entities:{len(entity_names)}"]
    else:
        archetype = "entity_catalog"
        signals = [f"domain:{domain}", "single_entity"]

    evidence_count = max(1, best_score + len(performance_signals) + len(time_signals))
    confidence = min(0.98, 0.55 + 0.05 * evidence_count)
    return {
        "domain": domain,
        "archetype": archetype,
        "confidence": round(confidence, 2),
        "signals": signals,
    }


def rank_semantic_metrics(columns: Sequence[str]) -> List[str]:
    """Rank physical Gold metric columns by business usefulness."""
    def score(column: str) -> tuple[int, str]:
        clean = column.lower()
        value = 0
        average_semantics = any(k in clean for k in ("ty_le", "rate", "score", "diem", "hai_long", "progress"))
        additive_semantics = any(k in clean for k in ("amount", "revenue", "count", "so_luot", "so_sv", "so_tiet"))
        if any(k in clean for k in PERFORMANCE_KEYWORDS):
            value += 100
        business_priority = {
            "ty_le_hien_dien": 45,
            "ty_le_dung_tien_do": 42,
            "ty_le_nhap_diem": 40,
            "diem_danh_gia": 38,
            "muc_do_hai_long": 36,
        }
        value += max((weight for keyword, weight in business_priority.items() if keyword in clean), default=0)
        if "__avg__" in clean and average_semantics:
            value += 70
        if "__sum__" in clean and additive_semantics:
            value += 60
        if clean.endswith("__record_count"):
            value += 35
        if "__sum__" in clean and average_semantics:
            value -= 50
        if "__avg__" in clean and additive_semantics:
            value -= 20
        if any(k in clean for k in ("thu_tu", "sequence", "ordinal")):
            value -= 100
        return value, clean

    return sorted(columns, key=score, reverse=True)


def parse_context_object_key(
    key: str,
    root_prefix: str = "staging/",
) -> Optional[Dict[str, str]]:
    """Parse ``staging/<context>/<entity>/<file>`` without assuming depth below entity."""
    normalized_root = root_prefix.strip("/") + "/"
    if not key.startswith(normalized_root) or key.endswith("/"):
        return None

    relative = key[len(normalized_root):]
    parts = [part for part in relative.split("/") if part]
    if len(parts) < 3 or parts[1] == "_control":
        return None
    if not parts[-1].lower().endswith(SUPPORTED_TABULAR_EXTENSIONS):
        return None

    return {
        "context_id": parts[0],
        "entity": parts[1],
        "filename": parts[-1],
        "key": key,
    }


def _serialize_last_modified(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def build_context_manifest(
    objects: Iterable[Mapping[str, Any]],
    context_id: str,
    *,
    bucket: str,
    root_prefix: str = "staging/",
    batch_id: Optional[str] = None,
) -> Dict[str, Any]:
    """Build a deterministic manifest for every supported object in one context."""
    manifest_objects: List[Dict[str, Any]] = []
    entities: Dict[str, List[str]] = {}

    for obj in objects:
        key = str(obj.get("Key") or obj.get("key") or "")
        parsed = parse_context_object_key(key, root_prefix=root_prefix)
        if not parsed or parsed["context_id"] != context_id:
            continue
        if int(obj.get("Size") or obj.get("size") or 0) <= 0:
            continue

        entry = {
            "key": key,
            "entity": parsed["entity"],
            "size": int(obj.get("Size") or obj.get("size") or 0),
            "etag": str(obj.get("ETag") or obj.get("etag") or "").strip('"'),
            "last_modified": _serialize_last_modified(
                obj.get("LastModified") or obj.get("last_modified")
            ),
        }
        manifest_objects.append(entry)
        entities.setdefault(parsed["entity"], []).append(key)

    manifest_objects.sort(key=lambda item: item["key"])
    for keys in entities.values():
        keys.sort()

    return {
        "manifest_version": 1,
        "batch_id": batch_id or f"context_{context_id}",
        "context_id": context_id,
        "bucket": bucket,
        "root_prefix": f"{root_prefix.strip('/')}/{context_id}/",
        "objects": manifest_objects,
        "entities": dict(sorted(entities.items())),
    }


@dataclass(frozen=True)
class EntityProfile:
    entity: str
    columns: Sequence[str]
    primary_key: Sequence[str]
    key_confidence: float
    row_count: int = 0
    key_candidates: Sequence[Sequence[str]] = ()


@dataclass(frozen=True)
class Relationship:
    entity: str
    kind: str
    join_columns: Sequence[str]
    references: str


@dataclass(frozen=True)
class ContextPlan:
    context_id: str
    anchor_entity: str
    anchor_key: Sequence[str]
    relationships: Sequence[Relationship]
    disconnected_entities: Sequence[str]

    def to_dict(self) -> Dict[str, Any]:
        result = asdict(self)
        result["relationships"] = [asdict(item) for item in self.relationships]
        return result


def _choose_anchor(profiles: Sequence[EntityProfile]) -> tuple[EntityProfile, List[str]]:
    """Choose an anchor and join key using cross-entity evidence.

    A table-local PK can differ from the context grain. For example, a course
    table may have both ``ma_mon_hoc`` and ``ma_lop_hp`` unique, while only the
    latter connects the rest of the context.
    """
    if not profiles:
        raise ValueError("Cannot choose an anchor from an empty context")

    choices = []
    for profile in profiles:
        candidates = list(profile.key_candidates) or [profile.primary_key]
        for candidate in candidates:
            key = set(candidate)
            if not key:
                continue
            references = sum(
                1
                for other in profiles
                if other.entity != profile.entity and key.issubset(set(other.columns))
            )
            dimension_links = sum(
                1
                for other in profiles
                if other.entity != profile.entity
                and other.primary_key
                and set(other.primary_key).issubset(set(profile.columns))
            )
            business_width = sum(
                1 for column in profile.columns if not is_technical_column(column)
            )
            choices.append(
                (
                    (references, dimension_links, business_width, profile.key_confidence, profile.entity),
                    profile,
                    list(candidate),
                )
            )

    if not choices:
        return profiles[0], list(profiles[0].primary_key)
    _, profile, key = max(choices, key=lambda item: item[0])
    return profile, key


def choose_anchor_entity(profiles: Sequence[EntityProfile]) -> EntityProfile:
    """Return the entity selected as the relational context anchor."""
    profile, _ = _choose_anchor(profiles)
    return profile


def build_context_plan(
    context_id: str,
    profiles: Sequence[EntityProfile],
) -> ContextPlan:
    """Classify peers as dimensions or facts around a single anchor entity."""
    anchor, inferred_anchor_key = _choose_anchor(profiles)
    anchor_columns = set(anchor.columns)
    anchor_key = inferred_anchor_key
    relationships: List[Relationship] = []
    disconnected: List[str] = []

    for profile in profiles:
        if profile.entity == anchor.entity:
            continue

        profile_columns = set(profile.columns)
        profile_key = list(profile.primary_key)
        if anchor_key and set(anchor_key).issubset(profile_columns):
            relationships.append(
                Relationship(
                    entity=profile.entity,
                    kind="fact",
                    join_columns=anchor_key,
                    references=anchor.entity,
                )
            )
        elif profile_key and set(profile_key).issubset(anchor_columns):
            relationships.append(
                Relationship(
                    entity=profile.entity,
                    kind="dimension",
                    join_columns=profile_key,
                    references=anchor.entity,
                )
            )
        else:
            disconnected.append(profile.entity)

    relationships.sort(key=lambda item: (item.kind, item.entity))
    disconnected.sort()
    return ContextPlan(
        context_id=context_id,
        anchor_entity=anchor.entity,
        anchor_key=anchor_key,
        relationships=relationships,
        disconnected_entities=disconnected,
    )


def candidate_key_columns(columns: Sequence[str], limit: int = 12) -> List[str]:
    """Rank likely business-key columns before expensive distinct profiling."""
    def name_score(column: str) -> tuple[int, str]:
        clean = column.lower()
        if is_technical_column(clean):
            return (-100, clean)
        score = 0
        if clean in {"id", "uuid", "key", "code", "ma"}:
            score += 100
        if clean.startswith("ma_"):
            score += 80
        if clean.endswith(("_id", "_uuid", "_key", "_code")):
            score += 70
        if "id" in clean or "code" in clean:
            score += 20
        return (score, clean)

    ranked = sorted(columns, key=name_score, reverse=True)
    return [column for column in ranked if not is_technical_column(column)][:limit]


def candidate_combinations(columns: Sequence[str], limit: int = 6) -> List[Sequence[str]]:
    """Return bounded two-column candidates for composite-key validation."""
    ranked = candidate_key_columns(columns, limit=limit)
    return [list(pair) for pair in combinations(ranked, 2)]
