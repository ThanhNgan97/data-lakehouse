from __future__ import annotations

import unittest

from gx_runtime.errors import ValidationConfigurationError
from gx_runtime.onboarding import OnboardingProposal


def proposal():
    contract = {
        "dataset_id": "finance.transactions",
        "contract_version": "1.0.0",
        "status": "draft",
        "owner": "finance-data",
        "effective_from": "2026-11-01",
        "grain": {"business_semantics_verified": True},
        "layers": {
            "silver": {
                "data_asset_ref": "lakehouse.silver.finance_transactions",
                "columns": {},
                "rules": [{
                    "id": "silver.schema", "type": "schema", "severity": "critical",
                    "supported_engines": ["sql"],
                }],
            },
            "gold": {
                "data_asset_ref": "lakehouse.gold.finance_transaction_metrics",
                "columns": {},
                "rules": [{
                    "id": "gold.schema", "type": "schema", "severity": "critical",
                    "supported_engines": ["sql"],
                }],
            },
        },
    }
    return {
        "dataset_id": contract["dataset_id"],
        "contract_version": contract["contract_version"],
        "owner": contract["owner"],
        "source_kinds": ["database", "file"],
        "silver_asset_ref": contract["layers"]["silver"]["data_asset_ref"],
        "gold_asset_ref": contract["layers"]["gold"]["data_asset_ref"],
        "contract": contract,
    }


class OnboardingProposalTest(unittest.TestCase):
    def test_proposal_is_deterministic_and_requires_review(self):
        first = OnboardingProposal.from_mapping(proposal(), submitted_by="data-admin")
        second = OnboardingProposal.from_mapping(proposal(), submitted_by="other-admin")
        self.assertEqual(first.proposal_id, second.proposal_id)
        self.assertEqual(first.status, "REVIEW_REQUIRED")
        self.assertEqual(first.source_kinds, ("database", "file"))

    def test_active_contract_and_asset_mismatch_are_rejected(self):
        active = proposal()
        active["contract"]["status"] = "active"
        with self.assertRaisesRegex(ValidationConfigurationError, "draft or review"):
            OnboardingProposal.from_mapping(active, submitted_by="admin")
        mismatch = proposal()
        mismatch["silver_asset_ref"] = "lakehouse.silver.other"
        with self.assertRaisesRegex(ValidationConfigurationError, "do not match"):
            OnboardingProposal.from_mapping(mismatch, submitted_by="admin")


if __name__ == "__main__":
    unittest.main()
