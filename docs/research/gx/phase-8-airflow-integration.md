# Phase 8: Airflow integration

Date: 2026-10-27

## Outcome

Airflow can now invoke GX through a stable HTTP boundary without installing GX,
Spark validation packages, suites, or contracts in an Airflow worker. The GX
container is a long-running, health-checked service; validation results remain
in Ozone S3 and XCom contains only a small durable reference and snapshot
approval receipt.

The manually triggered `gx_snapshot_quality_gate` DAG accepts one immutable
validation request and executes a snapshot-aware table gate. It is deliberately
separate from the existing API pipeline until that pipeline stops auto-merging
Silver and returns a durable candidate identity between tasks.

## Request and XCom contracts

Required request fields:

```text
dataset_id, data_layer, contract_version, source_kind, run_id,
nessie_branch, snapshot_id, nessie_commit_hash
```

Optional fields select `execution_engine` (SQL by default) and
`execution_mode` (`enforce` by default). The service resolves suites from the
phase-7 dataset registry rather than accepting arbitrary suite names.

The successful XCom is capped at 4 KiB and contains only:

```json
{
  "status": "PASSED",
  "validation_id": "...",
  "validation_uri": "s3://.../result.json",
  "merge_allowed": true,
  "approval": {
    "validated_branch": "candidate_abc",
    "validated_snapshot_id": "123",
    "validated_commit_hash": "abc123"
  }
}
```

Expectation results and failure details stay in the durable summary referenced
by `validation_uri`; they are not copied into Airflow metadata.

## Error and retry policy

| Category | Airflow behavior |
|---|---|
| `DATA_QUALITY_FAILED` | Fail immediately; no automatic retry |
| `GX_CONFIGURATION_ERROR` | Fail immediately; no automatic retry |
| `GX_EXECUTION_ERROR` | Retry with exponential backoff |
| `VALIDATION_TIMEOUT` | Retry with exponential backoff |
| `SNAPSHOT_MISMATCH` | Refresh identity and rerun GX once; task retry if instability continues |

The DAG allows two infrastructure retries, starts at 30 seconds, caps backoff
at five minutes, and has a ten-minute execution timeout. Quality failures do
not consume this retry budget.

## Snapshot safety

The service resolves the requested identity before validation and the phase-5
gate resolves it again afterward. A mismatch invalidates the run. The returned
approval commit must still be the expected-hash/CAS precondition of the
orchestration-owned Nessie merge.

The current Trino catalog mounts one Nessie ref. The service therefore rejects
a request whose branch differs from `GX_TRINO_NESSIE_BRANCH`. To validate a
candidate with SQL, deploy/configure the GX service and Trino catalog for that
candidate ref. This explicit rejection prevents accidentally validating `main`
while labeling the result as a candidate validation.

## Existing pipeline cutover

`api_dataset_pipeline` currently calls Silver processing with
`merge_to_main=True`; its following validation is post-publication. It has not
been silently rewired to the new gate because no durable Silver candidate state
is handed from the writer to Airflow yet. Safe cutover requires the writer team
to provide:

1. candidate branch, snapshot ID, and commit hash;
2. processing with auto-merge disabled;
3. GX gate execution using that identity;
4. merge using the approval commit as expected hash;
5. retry-stable candidate state and cleanup.

Gold already has candidate lifecycle state, but its existing dbt quality guard
must likewise pass the exact output identity to GX before replacing or
composing its publish gate.

## Verification

- GX API contract and error policy tests passed.
- Airflow adapter tests covered bounded XCom, non-retryable quality/config
  failures, retryable timeout, and snapshot refresh/revalidation.
- Airflow reported no DAG import errors and loaded
  `gx_snapshot_quality_gate`.
- The GX service health check reached `healthy`.
- An end-to-end request from the Airflow scheduler to the external GX service
  validated `education.teaching_progress` Silver (`6/6`), passed the snapshot
  gate, and persisted:

```text
s3://university-lakehouse/gx/validation-results/
education.teaching_progress/silver/phase8-live/
512e223a-44da-5a80-8095-17fee8a174cc.json
```

This live request used `main` solely to verify service wiring. It does not claim
that the existing post-merge API pipeline has become a candidate-branch gate.
