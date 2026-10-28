import copy
import io
import json
import os
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

import dbt_gold_runtime as runtime
from dbt_gold_contract import identity, outputs, SQL_VALIDATION_CONTRACT, require_config
from gx_candidate_binding import dbt_gold_candidate_binding


CONFIG = SimpleNamespace(dataset="education.learning_outcomes", dbt_selector="agg_learning_outcomes_by_period",
                         silver_table="lakehouse.silver.learning_outcomes", gold_table="lakehouse.gold.learning_outcomes_metrics")


def state(phase="validated"):
    job, ref, alias = identity(CONFIG.dataset, "unit-run")
    entries = outputs(CONFIG.dataset, "unit-run")
    candidates = {name: {"type": "ICEBERG_TABLE", "metadataLocation": "s3a://bucket/" + name,
                         "snapshotId": 1, "schemaId": 0, "specId": 0, "sortOrderId": 0}
                  for name in entries}
    return {"dataset": CONFIG.dataset, "run_id": "unit-run", "job": job, "ref": ref, "alias": alias,
            "target_ref": "main", "base_hash": "base", "validated_hash": "valid", "phase": phase,
            "artifact_sha256": "sealed", "validation_contract": SQL_VALIDATION_CONTRACT,
            "source_content": {"type": "ICEBERG_TABLE", "snapshotId": 5},
            "outputs": {name: {"alias": alias, "original_content": None} for name, alias in entries.items()},
            "candidate_contents": candidates}


def worker(s):
    return {"artifact_sha256": s["artifact_sha256"], "validation_contract": SQL_VALIDATION_CONTRACT,
            "dataset": s["dataset"], "build_ref_hash": "built",
            "candidate_contents": s["candidate_contents"], "source_content": s["source_content"]}


def approvals(s):
    binding = dbt_gold_candidate_binding(s["dataset"], s["run_id"], CONFIG.gold_table)
    output_name = CONFIG.gold_table.rsplit(".", 1)[-1]
    snapshot_id = s["candidate_contents"][output_name]["snapshotId"]
    results = []
    for layer in ("gold", "reconciliation"):
        validation_id = f"{layer}-validation"
        results.append({
            "data_layer": layer,
            "data_asset_ref": CONFIG.gold_table,
            "physical_data_asset_ref": binding.physical_asset_ref,
            "candidate_branch": s["ref"],
            "candidate_hash": s["validated_hash"],
            "validation_id": validation_id,
            "merge_allowed": True,
            "approval": {
                "validation_id": validation_id,
                "merge_allowed": True,
                "data_asset_ref": binding.physical_asset_ref,
                "validated_branch": s["ref"],
                "validated_snapshot_id": str(snapshot_id),
                "validated_commit_hash": s["validated_hash"],
            },
        })
    return tuple(results)


class GoldRuntimeTest(unittest.TestCase):
    def test_build_submits_pinned_head_and_retry_does_not_resubmit(self):
        s = state("prepared")
        response = Mock()
        response.json.return_value = {"status": "passed", "artifact_sha256": "new_seal"}
        with patch.object(runtime, "load", return_value=s), patch.object(runtime.requests, "post") as post, patch.object(runtime.requests, "get", return_value=response), patch.object(runtime, "save"), patch.object(runtime, "worker_result", return_value=worker(s)):
            runtime.build(CONFIG, "unit-run")
            self.assertEqual(post.call_args.kwargs["json"]["expected_head"], "base")
            self.assertEqual(s["phase"], "built")
            self.assertEqual(s["artifact_sha256"], "new_seal")
            runtime.build(CONFIG, "unit-run")
            post.assert_called_once()

    def test_worker_receipt_identity_or_cleanup_mismatch_is_refused(self):
        s = state()
        baseline = dict(worker(s), status="passed", ref=s["ref"], job_id=s["job"],
                        run_id=s["run_id"], expected_head=s["base_hash"])
        for field, value in [("ref", "other"), ("job_id", "other"), ("run_id", "other"),
                             ("expected_head", "other"), ("catalog_cleanup_failed", True)]:
            response = Mock()
            response.json.return_value = dict(baseline, **{field: value})
            with self.subTest(field=field), patch.object(runtime.requests, "get", return_value=response), self.assertRaises(RuntimeError):
                runtime.worker_result(s)

    def test_iceberg_pointer_disagreement_is_refused(self):
        candidate = state()["candidate_contents"]["learning_outcomes_metrics"]
        metadata = {"current-snapshot-id": 999, "current-schema-id": 0,
                    "default-spec-id": 0, "default-sort-order-id": 0}
        store = Mock()
        store.get_object.return_value = {"Body": io.BytesIO(json.dumps(metadata).encode())}
        with patch.object(runtime, "store", return_value=store), patch.object(runtime, "S3_BUCKET_NAME", "bucket"), self.assertRaises(RuntimeError):
            runtime.metadata_document(candidate)

    def test_wrong_dataset_selector_has_no_fallback(self):
        bad = copy.copy(CONFIG)
        bad.dbt_selector = "pyspark"
        with self.assertRaises(ValueError):
            require_config(bad)

    def test_prepare_retry_reuses_state_without_new_branch(self):
        s = state("prepared")
        with patch.object(runtime, "load", return_value=s), patch.object(runtime, "api") as api:
            self.assertIs(runtime.prepare(CONFIG, "unit-run"), s)
            api.assert_not_called()

    def test_prepare_without_source_never_creates_branch(self):
        with patch.object(runtime, "load", return_value=None), patch.object(runtime, "head", return_value="base"), patch.object(runtime, "content", return_value=None), patch.object(runtime, "api") as api:
            with self.assertRaises(RuntimeError):
                runtime.prepare(CONFIG, "unit-run")
            api.assert_not_called()

    def test_load_refuses_tampered_output_and_target(self):
        for change in ["alias", "target"]:
            s = state()
            if change == "alias":
                s["outputs"]["learning_outcomes_metrics"]["alias"] = "another_job"
            else:
                s["target_ref"] = "dbt_gold_test_changed"
            store = Mock()
            store.get_object.return_value = {"Body": io.BytesIO(json.dumps(s).encode())}
            with patch.object(runtime, "store", return_value=store), patch.dict(os.environ, {"GOLD_TARGET_REF": "main"}), self.assertRaises(RuntimeError):
                runtime.load(CONFIG, "unit-run")

    def test_validation_rejects_changed_artifact_head_or_contract(self):
        for kind in ["artifact", "head", "contract"]:
            s = state("built")
            result = worker(s)
            if kind == "artifact": result["artifact_sha256"] = "changed"
            if kind == "contract": result["validation_contract"] = "legacy"
            with patch.object(runtime, "load", return_value=s), patch.object(runtime, "worker_result", return_value=result), patch.object(runtime, "head", return_value="changed" if kind == "head" else "built"), self.assertRaises(RuntimeError):
                runtime.validate(CONFIG, "unit-run")

    def test_validation_rejects_source_change_before_guard(self):
        s = state("built")
        with patch.object(runtime, "load", return_value=s), patch.object(runtime, "worker_result", return_value=worker(s)), patch.object(runtime, "head", return_value="built"), patch.object(runtime, "content", return_value={"snapshotId": 999}), self.assertRaises(RuntimeError):
            runtime.validate(CONFIG, "unit-run")

    def test_validation_rejects_missing_or_matching_invalid_state_contract(self):
        # Even matching state/receipt values must not authorize an unknown contract.
        for value in (None, "legacy", require_config):
            with self.subTest(value=value):
                s = state("built")
                result = worker(s)
                if value is None:
                    s.pop("validation_contract")
                else:
                    s["validation_contract"] = value
                    result["validation_contract"] = value
                with patch.object(runtime, "load", return_value=s), patch.object(runtime, "worker_result", return_value=result), patch.object(runtime, "head") as head, patch.object(runtime, "save") as save:
                    with self.assertRaisesRegex(RuntimeError, "mandatory SQL quality contract"):
                        runtime.validate(CONFIG, "unit-run")
                    head.assert_not_called()
                    save.assert_not_called()

    def test_complete_sql_validation_freezes_guarded_bundle(self):
        s = state("built")
        def content(ref, key, hash_value=None):
            if key.startswith("silver."): return s["source_content"]
            name = next(n for n, entry in s["outputs"].items() if key == "gold." + entry["alias"])
            return s["candidate_contents"][name]
        properties = {"gc.enabled": "false", "write.metadata.delete-after-commit.enabled": "false"}
        with patch.object(runtime, "load", return_value=s), patch.object(runtime, "worker_result", return_value=worker(s)), patch.object(runtime, "head", side_effect=["built", "guarded", "guarded"]), patch.object(runtime, "content", side_effect=content), patch.object(runtime, "metadata_document", return_value={"properties": properties}), patch("dbt_gold_metadata.guard", return_value={"spark_session": False}), patch.object(runtime, "save") as save:
            runtime.validate(CONFIG, "unit-run")
            self.assertEqual(s["phase"], "validated")
            self.assertEqual(s["validated_hash"], "guarded")
            self.assertEqual(len(s["candidate_contents"]), 2)
            save.assert_called_once()

    def test_unvalidated_or_conflicting_target_never_commits(self):
        for phase, target_hash in [("built", "base"), ("validated", "newer")]:
            s = state(phase)
            with patch.object(runtime, "load", return_value=s), patch.object(runtime, "worker_result", return_value=worker(s)), patch.object(runtime, "head", side_effect=lambda ref: "valid" if ref == s["ref"] else target_hash), patch.object(runtime, "content", return_value=None), patch.object(runtime, "api") as api:
                with self.assertRaises(RuntimeError): runtime.publish(CONFIG, "unit-run", *approvals(s))
                api.assert_not_called()

    def test_missing_or_stale_gx_approval_never_commits(self):
        s = state()
        gold, reconciliation = approvals(s)
        stale = copy.deepcopy(reconciliation)
        stale["approval"]["validated_commit_hash"] = "stale"
        for receipts in ((None, None), (gold, stale)):
            with self.subTest(receipts=receipts), patch.object(runtime, "load", return_value=s), patch.object(runtime, "api") as api:
                with self.assertRaisesRegex(RuntimeError, "GX .* approval"):
                    runtime.publish(CONFIG, "unit-run", *receipts)
                api.assert_not_called()

    def test_fact_mart_publish_is_one_cas_commit(self):
        s = state()
        def content(ref, key, hash_value=None):
            return s["candidate_contents"][key.split(".")[-1]] if hash_value == "committed" else None
        with patch.object(runtime, "load", return_value=s), patch.object(runtime, "worker_result", return_value=worker(s)), patch.object(runtime, "head", side_effect=lambda ref: "valid" if ref == s["ref"] else "base"), patch.object(runtime, "content", side_effect=content), patch.object(runtime, "api", return_value={"hash": "committed"}) as api, patch.object(runtime, "save"):
            runtime.publish(CONFIG, "unit-run", *approvals(s))
            self.assertEqual(s["phase"], "published")
            self.assertEqual(
                s["gx_approvals"],
                {"gold": "gold-validation", "reconciliation": "reconciliation-validation"},
            )
            api.assert_called_once()
            self.assertEqual(api.call_args.kwargs["params"], {"expectedHash": "base"})
            self.assertEqual(len(api.call_args.kwargs["json"]["operations"]), 2)

    def test_published_retry_does_not_commit_again(self):
        with patch.object(runtime, "load", return_value=state("published")), patch.object(runtime, "api") as api:
            runtime.publish(CONFIG, "unit-run")
            api.assert_not_called()

    def test_successful_commit_with_unsaved_state_recovers_whole_bundle(self):
        s = state()
        with patch.object(runtime, "load", return_value=s), patch.object(runtime, "worker_result", return_value=worker(s)), patch.object(runtime, "head", side_effect=lambda ref: "valid" if ref == s["ref"] else "already_committed"), patch.object(runtime, "content", side_effect=lambda ref, key, hash_value=None: s["candidate_contents"][key.split(".")[-1]]), patch.object(runtime, "api") as api, patch.object(runtime, "save"):
            runtime.publish(CONFIG, "unit-run", *approvals(s))
            self.assertEqual(s["phase"], "published")
            api.assert_not_called()

    def test_partial_bundle_is_not_mistaken_for_successful_publication(self):
        s = state()
        def content(ref, key, hash_value=None):
            return s["candidate_contents"]["learning_outcomes_metrics"] if key.endswith("learning_outcomes_metrics") else None
        with patch.object(runtime, "load", return_value=s), patch.object(runtime, "worker_result", return_value=worker(s)), patch.object(runtime, "head", side_effect=lambda ref: "valid" if ref == s["ref"] else "partial_commit"), patch.object(runtime, "content", side_effect=content), patch.object(runtime, "api") as api, self.assertRaises(RuntimeError):
            runtime.publish(CONFIG, "unit-run", *approvals(s))
        api.assert_not_called()
