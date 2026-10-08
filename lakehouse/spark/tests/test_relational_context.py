import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path


SPARK_DIR = Path(__file__).resolve().parent.parent
if str(SPARK_DIR) not in sys.path:
    sys.path.insert(0, str(SPARK_DIR))

from relational_context import (
    EntityProfile,
    build_context_manifest,
    build_context_plan,
    candidate_key_columns,
    infer_context_semantics,
    parse_context_object_key,
    rank_semantic_metrics,
)


class RelationalContextPlanningTest(unittest.TestCase):
    def test_nested_airbyte_key_is_parsed(self):
        parsed = parse_context_object_key(
            "staging/ctu_ioc_test/diem_danh_lop_hp/year=2026/part_0.parquet"
        )
        self.assertEqual(parsed["context_id"], "ctu_ioc_test")
        self.assertEqual(parsed["entity"], "diem_danh_lop_hp")

    def test_manual_upload_is_not_parsed_as_relational_context(self):
        parsed = parse_context_object_key(
            "staging/manual/a510b1109595490399276402b899622e/train.csv"
        )
        self.assertIsNone(parsed)

    def test_manifest_keeps_all_entities_in_one_context(self):
        objects = [
            {
                "Key": "staging/ctu_ioc_test/lop_hoc_phan/part_0.parquet",
                "Size": 100,
                "ETag": '"anchor"',
                "LastModified": datetime(2026, 10, 8, tzinfo=timezone.utc),
            },
            {
                "Key": "staging/ctu_ioc_test/diem_danh_lop_hp/part_0.parquet",
                "Size": 200,
                "ETag": '"fact"',
                "LastModified": datetime(2026, 10, 8, tzinfo=timezone.utc),
            },
            {
                "Key": "staging/other/table/part_0.parquet",
                "Size": 300,
            },
        ]

        manifest = build_context_manifest(
            objects,
            "ctu_ioc_test",
            bucket="university-lakehouse",
            batch_id="airbyte_job_65",
        )

        self.assertEqual(len(manifest["objects"]), 2)
        self.assertEqual(
            set(manifest["entities"]), {"lop_hoc_phan", "diem_danh_lop_hp"}
        )

    def test_anchor_and_relationships_are_inferred_from_shared_keys(self):
        profiles = [
            EntityProfile(
                entity="lop_hoc_phan",
                columns=["ma_lop_hp", "ma_don_vi", "ten_mon_hoc"],
                primary_key=["ma_lop_hp"],
                key_confidence=1.0,
                row_count=10,
                key_candidates=[["ma_lop_hp"], ["ma_mon_hoc"]],
            ),
            EntityProfile(
                entity="diem_danh_lop_hp",
                columns=["ma_lop_hp", "tong_luot_diem_danh"],
                primary_key=["ma_lop_hp", "ngay"],
                key_confidence=0.95,
                row_count=100,
            ),
            EntityProfile(
                entity="dm_don_vi_dao_tao",
                columns=["ma_don_vi", "ten_don_vi"],
                primary_key=["ma_don_vi"],
                key_confidence=1.0,
                row_count=5,
            ),
        ]

        plan = build_context_plan("ctu_ioc_test", profiles)

        self.assertEqual(plan.anchor_entity, "lop_hoc_phan")
        relations = {(r.entity, r.kind) for r in plan.relationships}
        self.assertIn(("diem_danh_lop_hp", "fact"), relations)
        self.assertIn(("dm_don_vi_dao_tao", "dimension"), relations)

    def test_key_candidates_prioritize_business_identifiers(self):
        ranked = candidate_key_columns(
            [
                "ten_mon_hoc",
                "_airbyte_extracted_at",
                "airbyte_raw_id",
                "ma_lop_hp",
                "so_tin_chi",
            ]
        )
        self.assertEqual(ranked[0], "ma_lop_hp")
        self.assertNotIn("_airbyte_extracted_at", ranked)
        self.assertNotIn("airbyte_raw_id", ranked)

    def test_semantic_router_ignores_airbyte_time_and_detects_education_performance(self):
        result = infer_context_semantics(
            "ctu_ioc_test",
            ["lop_hoc_phan", "diem_danh_lop_hp", "tien_do_giang_day"],
            ["airbyte_updated_at", "ma_lop_hp", "ty_le_hien_dien", "ty_le_dung_tien_do"],
        )
        self.assertEqual(result["domain"], "education")
        self.assertEqual(result["archetype"], "operational_performance")
        self.assertFalse(any("airbyte" in signal for signal in result["signals"]))

    def test_semantic_metric_ranking_prefers_average_rate_over_sum(self):
        ranked = rank_semantic_metrics([
            "diem_danh__sum__ty_le_hien_dien",
            "diem_danh__avg__ty_le_hien_dien",
            "dm_don_vi__thu_tu_hien_thi",
        ])
        self.assertEqual(ranked[0], "diem_danh__avg__ty_le_hien_dien")
        self.assertEqual(ranked[-1], "dm_don_vi__thu_tu_hien_thi")


if __name__ == "__main__":
    unittest.main(verbosity=2)
