"""Governed dataset-onboarding proposals; runtime registries remain Git-backed."""

from __future__ import annotations

import hashlib
import json
import os
import re
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any, Mapping

from .errors import ValidationConfigurationError, ValidationPersistenceError
from .contracts import validate_contract_payload


_DATASET = re.compile(r"^[a-z][a-z0-9_]*(?:\.[a-z][a-z0-9_]*)+$")
_ASSET = re.compile(r"^lakehouse\.(?:silver|gold)\.[a-z_][a-z0-9_]*$")
_SOURCES = frozenset({"file", "api", "database"})


@dataclass(frozen=True)
class OnboardingProposal:
    proposal_id: str
    dataset_id: str
    contract_version: str
    owner: str
    source_kinds: tuple[str, ...]
    silver_asset_ref: str
    gold_asset_ref: str
    contract: dict[str, Any]
    submitted_by: str
    submitted_at: str
    status: str = "REVIEW_REQUIRED"

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_mapping(
        cls, value: Mapping[str, Any], *, submitted_by: str
    ) -> "OnboardingProposal":
        try:
            dataset_id = str(value["dataset_id"]).strip()
            version = str(value["contract_version"]).strip()
            owner = str(value["owner"]).strip()
            sources = tuple(sorted({str(item) for item in value["source_kinds"]}))
            silver = str(value["silver_asset_ref"]).strip()
            gold = str(value["gold_asset_ref"]).strip()
            contract = dict(value["contract"])
        except (KeyError, TypeError, ValueError) as exc:
            raise ValidationConfigurationError("Invalid onboarding proposal") from exc
        if not _DATASET.fullmatch(dataset_id) or not owner or not version:
            raise ValidationConfigurationError("Invalid dataset, owner, or contract version")
        if not sources or not set(sources) <= _SOURCES:
            raise ValidationConfigurationError("Onboarding source_kinds are invalid")
        if not _ASSET.fullmatch(silver) or not _ASSET.fullmatch(gold):
            raise ValidationConfigurationError("Onboarding canonical assets are invalid")
        if (
            contract.get("dataset_id") != dataset_id
            or contract.get("contract_version") != version
            or contract.get("owner") != owner
            or contract.get("status") not in {"draft", "review"}
        ):
            raise ValidationConfigurationError(
                "Proposal identity/owner must match a draft or review contract"
            )
        layers = contract.get("layers") or {}
        if (
            (layers.get("silver") or {}).get("data_asset_ref") != silver
            or (layers.get("gold") or {}).get("data_asset_ref") != gold
        ):
            raise ValidationConfigurationError("Proposal assets do not match contract layers")
        validate_contract_payload(contract, source="onboarding proposal")
        canonical = json.dumps(
            {
                "dataset_id": dataset_id,
                "contract_version": version,
                "owner": owner,
                "source_kinds": sources,
                "silver_asset_ref": silver,
                "gold_asset_ref": gold,
                "contract": contract,
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        proposal_id = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
        return cls(
            proposal_id=proposal_id,
            dataset_id=dataset_id,
            contract_version=version,
            owner=owner,
            source_kinds=sources,
            silver_asset_ref=silver,
            gold_asset_ref=gold,
            contract=contract,
            submitted_by=submitted_by,
            submitted_at=datetime.now(timezone.utc).isoformat(),
        )


class S3OnboardingStore:
    def __init__(self, bucket: str, prefix: str = "gx/onboarding-proposals") -> None:
        self.bucket = bucket
        self.prefix = prefix.strip("/")

    def _client(self):
        import boto3
        return boto3.client(
            "s3",
            endpoint_url=os.getenv("S3_ENDPOINT") or None,
            region_name=os.getenv("S3_REGION") or None,
            aws_access_key_id=os.getenv("S3_ACCESS_KEY") or None,
            aws_secret_access_key=os.getenv("S3_SECRET_KEY") or None,
        )

    def save(self, proposal: OnboardingProposal) -> str:
        key = f"{self.prefix}/{proposal.proposal_id}.json"
        try:
            self._client().put_object(
                Bucket=self.bucket,
                Key=key,
                Body=json.dumps(proposal.as_dict(), sort_keys=True).encode("utf-8"),
                ContentType="application/json",
                Tagging="gx-artifact=onboarding-proposal&gx-status=review-required",
                IfNoneMatch="*",
            )
        except Exception as exc:
            # An identical deterministic proposal is idempotent.
            response = getattr(exc, "response", {})
            status = (response.get("ResponseMetadata") or {}).get("HTTPStatusCode")
            if status != 412:
                raise ValidationPersistenceError("Unable to persist onboarding proposal") from exc
        return f"s3://{self.bucket}/{key}"

    def load(self, proposal_id: str) -> OnboardingProposal:
        if not re.fullmatch(r"[a-f0-9]{64}", proposal_id):
            raise ValidationConfigurationError("Invalid onboarding proposal_id")
        try:
            response = self._client().get_object(
                Bucket=self.bucket, Key=f"{self.prefix}/{proposal_id}.json"
            )
            return OnboardingProposal(**json.loads(response["Body"].read()))
        except Exception as exc:
            raise ValidationPersistenceError("Unable to load onboarding proposal") from exc


def onboarding_store_from_env() -> S3OnboardingStore:
    bucket = os.getenv("GX_RESULT_STORE_BUCKET", "").strip()
    if not bucket:
        raise ValidationConfigurationError("GX_RESULT_STORE_BUCKET is required")
    return S3OnboardingStore(bucket, os.getenv("GX_ONBOARDING_PREFIX", "gx/onboarding-proposals"))
