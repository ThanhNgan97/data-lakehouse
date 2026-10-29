from __future__ import annotations

import json
import unittest
from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[3]


class MonitoringAssetsTest(unittest.TestCase):
    def test_prometheus_scrapes_gx_and_loads_alerts(self):
        config = yaml.safe_load((ROOT / "monitoring/gx/prometheus.yml").read_text())
        self.assertEqual(config["scrape_configs"][0]["metrics_path"], "/metrics")
        self.assertIn("gx-validator:8090", config["scrape_configs"][0]["static_configs"][0]["targets"])
        alerts = yaml.safe_load((ROOT / "monitoring/gx/gx-alerts.yml").read_text())
        names = {rule["alert"] for group in alerts["groups"] for rule in group["rules"]}
        self.assertIn("GXValidatorUnavailable", names)
        self.assertIn("GXNotConfiguredDatasets", names)
        self.assertIn("GXAuthenticationFailures", names)

    def test_grafana_dashboard_is_valid_and_covers_core_signals(self):
        dashboard = json.loads(
            (ROOT / "monitoring/gx/grafana/dashboards/gx-overview.json").read_text()
        )
        expressions = " ".join(
            target["expr"]
            for panel in dashboard["panels"]
            for target in panel.get("targets", [])
        )
        self.assertIn("gx_active_validations", expressions)
        self.assertIn("gx_not_configured_total", expressions)
        self.assertIn("gx_quarantine_total", expressions)


if __name__ == "__main__":
    unittest.main()
