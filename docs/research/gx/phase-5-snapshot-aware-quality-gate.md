# Phase 5: snapshot-aware quality gate

Date: 2026-10-27  
Pilot dataset: `education.teaching_progress`

## Outcome

The validation framework can now authorize publication only for the exact
Nessie branch, Iceberg snapshot, Nessie commit, and data asset that GX checked.
The gate is fail-closed: an unexecuted validation, a blocking quality result,
missing identity, or any identity mismatch refuses publication.

The durable validation summary exposes the phase-5 contract fields:

```json
{
  "status": "PASSED",
  "dataset_id": "education.teaching_progress",
  "layer": "silver",
  "suite_versions": {"teaching_progress_silver_core": "1.0.0"},
  "validated_branch": "candidate_abc",
  "validated_snapshot_id": "123456",
  "validated_commit_hash": "abc123",
  "critical_failure_count": 0,
  "merge_allowed": true
}
```

The original `nessie_branch`, `snapshot_id`, `nessie_commit_hash`, and
`data_layer` keys remain serialized for backward compatibility with the phase-2
framework.

## Gate protocol

1. The writer finishes all Spark/dbt writes on the candidate branch.
2. Orchestration resolves the candidate branch head and the table snapshot at
   that immutable commit.
3. GX validates that identity and persists its summary.
4. Immediately before publication, `SnapshotAwareQualityGate` resolves the
   current identity again.
5. Exact equality is required for data asset, branch, snapshot ID, and commit
   hash. Otherwise the gate raises `SnapshotMismatchError` with category
   `SNAPSHOT_MISMATCH`; orchestration must run GX again.
6. A successful check returns a small `QualityGateApproval` receipt containing
   the validation ID and validated identity.

The approval receipt is authorization evidence, not a merge implementation.
The orchestration-owned Nessie merge must use
`approval.validated_commit_hash` as its compare-and-swap/expected-hash
precondition. Without that atomic precondition, another write could occur
between verification and merge (a TOCTOU race).

## Nessie resolver behavior

`NessieSnapshotResolver` performs read-only calls:

- resolve the requested ref and capture its commit hash;
- resolve `schema.table` content using both `ref` and `hashOnRef`;
- require content type `ICEBERG_TABLE` and a non-null `snapshotId`.

Resolving content at the captured hash prevents the resolver itself from
combining a branch head from one commit with a table pointer from another.

## Verification

Automated coverage verifies:

- exact identity returns an approval receipt;
- changed snapshot invalidates the result;
- changed commit invalidates the result even if the table snapshot is unchanged;
- changed branch or data asset invalidates the result;
- `off` mode cannot authorize publication because GX did not execute;
- blocking quality results cannot authorize publication;
- summary JSON contains the explicit validated identity contract;
- Nessie content is read at the captured branch hash.

The live read-only pilot on the running local stack passed:

| Field | Observed value |
|---|---|
| Asset | `lakehouse.silver.teaching_progress` |
| Branch | `main` |
| Snapshot | `4366971137478837827` |
| Commit | `07224a10eca333523113fff048aed0cdf329c466140e877f1c174aff3bf58fc8` |
| SQL expectations | `6/6` passed |
| Gate | Approval receipt issued |

The live `main` check proves resolver and gate compatibility. Production usage
must validate a candidate branch and consume the receipt in the pre-merge
orchestration introduced with the Airflow integration phase.
