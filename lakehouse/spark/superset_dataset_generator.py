# -*- coding: utf-8 -*-
"""Superset Dataset & Chart Configuration Generator.

This module converts a RoutingDecision into Apache Superset YAML metadata:
1. Generates dataset definition YAML targeting Trino catalog: lakehouse.gold
2. Configures metric expressions (SUM, AVG, COUNT)
3. Maps dimensions as groupable/filterable columns
4. Prepares dashboard import artifacts ready for Superset CLI import
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional
import uuid

# Đảm bảo console Windows in tiếng Việt UTF-8 không bị lỗi charmap
if sys.stdout and hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

_CURRENT_DIR = Path(__file__).resolve().parent
if str(_CURRENT_DIR) not in sys.path:
    sys.path.insert(0, str(_CURRENT_DIR))

from ai_dataset_router import RoutingDecision, to_snake_case


DATABASE_UUID = "41989b72-5069-4671-8a40-78245bd3bd30"  # UUID của Database CTU IOC Trino trong Superset


def generate_superset_dataset_yaml(decision: RoutingDecision) -> Dict[str, Any]:
    """Sinh cấu hình từ điển Python tương ứng với file dataset YAML của Superset."""
    table_name = decision.target_gold_table.split(".")[-1]

    columns_config = []
    metrics_config = [
        {
            "metric_name": "count",
            "verbose_name": "Tổng số bản ghi",
            "metric_type": "count",
            "expression": "COUNT(*)",
        }
    ]

    # Cấu hình Dimensions
    for dim in decision.dimension_columns:
        columns_config.append({
            "column_name": to_snake_case(dim),
            "verbose_name": dim.replace("_", " ").title(),
            "is_dttm": False,
            "is_active": True,
            "type": "VARCHAR",
            "groupby": True,
            "filterable": True,
        })

    # Cấu hình Metrics
    for metric in decision.metric_columns:
        m_clean = to_snake_case(metric)
        columns_config.append({
            "column_name": f"sum_{m_clean}",
            "verbose_name": f"Tổng {m_clean}",
            "is_dttm": False,
            "is_active": True,
            "type": "DOUBLE",
            "groupby": False,
            "filterable": True,
        })
        metrics_config.append({
            "metric_name": f"total_{m_clean}",
            "verbose_name": f"Tổng {m_clean}",
            "metric_type": "sum",
            "expression": f"SUM(sum_{m_clean})",
        })
        metrics_config.append({
            "metric_name": f"average_{m_clean}",
            "verbose_name": f"Trung bình {m_clean}",
            "metric_type": "avg",
            "expression": f"AVG(avg_{m_clean})",
        })

    dataset_dict = {
        "table_name": table_name,
        "main_dttm_col": None,
        "description": f"AI Generated Data Mart: {decision.dataset_entity} ({decision.dataset_domain})",
        "default_endpoint": None,
        "offset": 0,
        "cache_timeout": None,
        "schema": "gold",
        "sql": None,
        "params": None,
        "template_params": None,
        "filter_select_enabled": True,
        "fetch_values_predicate": None,
        "extra": None,
        "uuid": str(uuid.uuid4()),
        "columns": columns_config,
        "metrics": metrics_config,
        "database_uuid": DATABASE_UUID,
    }

    return dataset_dict


def export_superset_dataset_to_file(decision: RoutingDecision, output_dir: Optional[Path] = None) -> Path:
    """Ghi cấu hình dataset thành file JSON/YAML sẵn sàng cho việc đăng ký hoặc API."""
    if output_dir is None:
        output_dir = Path(__file__).resolve().parent.parent.parent / "superset_exports" / "generated"

    output_dir.mkdir(parents=True, exist_ok=True)
    table_name = decision.target_gold_table.split(".")[-1]
    output_file = output_dir / f"{table_name}_dataset.json"

    dataset_content = generate_superset_dataset_yaml(decision)

    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(dataset_content, f, indent=2, ensure_ascii=False)

    print(f"📊 [Superset Generator] Đã sinh cấu hình dataset thành công: {output_file}")
    return output_file


def main():
    parser = argparse.ArgumentParser(description="Superset Dataset Generator from Routing Decision")
    parser.add_argument("--decision-file", "-d", required=True, help="Đường dẫn file RoutingDecision JSON")
    parser.add_argument("--output-dir", "-o", help="Thư mục xuất file cấu hình Superset")
    args = parser.parse_args()

    with open(args.decision_file, "r", encoding="utf-8") as f:
        decision = RoutingDecision(**json.load(f))

    out_dir = Path(args.output_dir) if args.output_dir else None
    export_superset_dataset_to_file(decision, out_dir)


if __name__ == "__main__":
    main()
