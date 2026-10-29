# Phase 7: multi-dataset and multi-source routing

Date: 2026-10-27

## Outcome

GX routing is now organized by:

```text
dataset_id + layer + contract_version + execution_engine
```

It is not organized by ingestion technology. `file`, `api`, and `database`
identify provenance and compatibility, but all three resolve to the exact same
contract, asset, and suite version for a given dataset/layer. This prevents
business rules from drifting merely because transport changed.

`DatasetValidationRegistry` verifies the complete route before creating a
request:

1. the exact dataset/layer/contract route exists;
2. the source kind is declared compatible;
3. the selected engine has a suite implementation;
4. the contract exists in the contract registry;
5. every suite exists, supports the engine, and implements the same contract
   version.

All failures are fail-closed configuration errors. The resolved
`ValidationPlan` can create a `ValidationRequest`, and `source_kind` is carried
into the validation summary and deterministic validation identity for audit.

## Registered datasets

| Dataset | Layers | Sources | Engines |
|---|---|---|---|
| `education.teaching_progress` | Silver, Gold, reconciliation | File, API, Database | Spark/SQL for Silver; SQL for Gold/reconciliation |
| `education.learning_outcomes` | Silver, Gold, reconciliation | File, API, Database | SQL |

The second dataset is a real onboarding, not a synthetic registry fixture.
`education.learning_outcomes@1.0.0` contains schema, completeness, candidate-key,
volume, freshness, range, and Silver/Gold reconciliation rules. Candidate-key
uniqueness remains warning severity because the source owner has not yet
confirmed the business grain.

## Learning outcomes evidence

Read-only profiling and suite execution on the current demo state produced:

| Check | Silver | Gold |
|---|---:|---:|
| Rows | 31 | 31 |
| Distinct `(ma_chuong_trinh, nam_hoc, hoc_ky)` | 31 | 31 |
| Required-value violations | 0 | 0 |
| Range violations | 0 | 0 |
| Snapshot ID | `444583211869213712` | `5362565669828490177` |
| Core suite | 6/6 passed | 6/6 passed |

Silver/Gold reconciliation passed 3/3 expectations. Routing the Silver, Gold,
and reconciliation layers with each of File, API, and Database returned the
same respective suite references.

## Streaming decision

`streaming` is a recognized source kind but is deliberately absent from every
current route. Requests fail with a specific “streaming validation is not
enabled” configuration error. Enabling it later requires a contract extension
that defines, at minimum:

- micro-batch or event-time window identity;
- watermark and allowed lateness;
- replay/idempotency semantics;
- how a window maps to an Iceberg snapshot and Nessie commit;
- window-level thresholds and publish policy.

This keeps batch rules reusable without pretending they are sufficient for
streaming semantics.

## Verification

Automated tests cover two dataset identities, source-independent suite reuse,
source-kind traceability, disabled streaming, unknown contract versions, wrong
engine selection, and independent pass/fail execution for every SQL
expectation. The live Trino/Nessie execution was read-only and did not create or
merge branches.
