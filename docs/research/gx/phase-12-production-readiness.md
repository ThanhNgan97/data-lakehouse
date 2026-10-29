# Phase 12: GX production readiness

Date: 2026-10-27

## Enforced runtime policy

When `GX_ENVIRONMENT=production`, startup and request handling fail closed unless:

- every validation uses `execution_mode=enforce`;
- `GX_SUITE_REVISION` identifies the reviewed Git revision packaged in the image;
- `GX_REDACT_OBSERVED_VALUES=true`;
- persistent S3-compatible result storage is configured.

Validation results carry `suite_revision`. S3 objects also carry revision and
retention metadata. This makes a retry, audit, or rollback traceable to the exact
Git version of the suites. Rollback means reverting/pinning the reviewed suite
and validation registry revision, building a new immutable image, and setting
`GX_SUITE_REVISION` to that revision; mutable in-place rule edits are prohibited.

## Rule governance

Repository administrators must protect the production branch and configure
CODEOWNERS/reviewer groups for:

```text
lakehouse/gx/suites/**
lakehouse/gx/suite_registry.json
lakehouse/gx/contract_registry.json
lakehouse/gx/validation_registry.json
```

Changes to critical rules require approval from both the data owner and data
platform/quality owner. CI must run the complete GX suite matrix before merge.
Git history is the authoritative rule-change audit log; the deployed commit is
recorded as `GX_SUITE_REVISION` in every validation result.

## Privacy and retention

Observed failure values are redacted from persisted summaries by default and
cannot be enabled in production. OpenMetadata events already exclude row data.
Production Data Docs publishers must use similarly redacted summaries and must
not publish unexpected rows, samples, query results, credentials, or PII.

`GX_RESULT_RETENTION_DAYS` defaults to 90 and is restricted to 1–3650 days.
Result objects receive `gx-artifact=validation-result` and
`gx-retention-days=<days>` tags. The infrastructure team must configure the
object-store lifecycle policy matching these tags; tagging alone does not delete
objects. Revocation audit records should follow the organization's audit
retention policy rather than the short result-retention window.

## Unavailability and revocation

- Readiness false or shutdown returns retryable `GX_UNAVAILABLE`.
- Infrastructure/persistence errors remain retryable `GX_EXECUTION_ERROR`.
- Airflow must not merge while GX is unavailable; there is no production bypass.
- Data-quality failure is not retried automatically.
- Snapshot mismatch writes a durable, deterministic revocation marker under
  `gx/revocations/`, returns the current identity, and requires validation again.
- Only the validation matching the current branch, snapshot, and commit may be
  used for merge approval.

## Production activation

Minimum environment additions:

```text
GX_ENVIRONMENT=production
GX_SUITE_REVISION=<reviewed-git-commit>
GX_REDACT_OBSERVED_VALUES=true
GX_RESULT_RETENTION_DAYS=90
GX_REVOCATION_PREFIX=gx/revocations
```

Secrets remain injected by the deployment platform. Production images should be
addressed by immutable digest, and promotion/rollback must never reuse a mutable
image tag.
