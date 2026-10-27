# GX acceptance UI and pre-publication cutover plan

## Implemented acceptance view

The API Portal presents the Airflow DAG as eight explicit stages: ingestion,
Silver processing, GX Silver, dbt Gold build, Nessie publish, GX Gold,
Silver-Gold reconciliation, and Trino readiness. It reads bounded XCom evidence
for the three GX tasks and displays validation ID, snapshot ID, Nessie commit,
asset, and `ALLOWED`/`BLOCKED` decision.

The UI labels Airflow as the orchestrator rather than a data-processing stage.
It also states that the current gates are post-write/post-publication controls,
so the visual does not claim a pre-Nessie guarantee that the implementation
does not yet provide.

## Candidate-branch cutover

### 1. Silver writer contract

- Make Silver processing produce retry-stable candidate state containing branch,
  snapshot ID, commit hash, table and base hash.
- Disable automatic merge to `main`.
- Persist state in Ozone with dataset/run identity and cleanup status.

### 2. Silver GX and publication

- Mount or safely expose the candidate branch to GX.
- Validate the candidate identity, retain the approval commit, and publish only
  with expected-hash/CAS semantics.
- Reject missing, stale, revoked or mismatched approval evidence.

### 3. dbt Gold candidate support

- Bind deterministic dbt aliases to reviewed canonical GX routes without
  accepting arbitrary catalog/table identifiers.
- Run GX Gold and reconciliation on the dbt candidate branch.
- Move `publish_gold` after both GX gates and use their approved candidate hash
  as a CAS precondition.

### 4. File and Airbyte adoption

- Reuse the same candidate-state and approval contract for registered uploads.
- Split the relational processor's combined Silver/Gold/merge operation.
- Keep dynamic datasets visibly `NOT_CONFIGURED` until a reviewed contract and
  canonical mapping exist.

### 5. Acceptance criteria

- Invalid data never changes `main`.
- A concurrent candidate or target change invalidates approval.
- Retry reuses the same candidate identity and never double-publishes.
- Portal evidence identifies candidate branch, snapshot, commit, validation and
  final publication commit.
- Valid and deliberately invalid fixtures prove allow and block behavior for
  each supported source.
