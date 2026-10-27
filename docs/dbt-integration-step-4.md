# dbt integration - Step 4: bounded Ozone/Nessie sandbox acceptance

Status: **PASS - bounded sandbox acceptance (30 rows per source)**.

Approved new files:

| File | Purpose |
|---|---|
| `scripts/verify_dbt_step4.ps1` | Pin Git baseline, validate runtime ownership, dispatch acceptance and retain report |
| `lakehouse/spark/tests/run_dbt_gold_sandbox.py` | Live isolated Gold lifecycle, parity, retry, candidate-change refusal and owned cleanup |
| This document | Scope, invocation, evidence and limits |

## Run

From D:\lakehouse, with newdirection and its dbt-worker running:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/verify_dbt_step4.ps1
```

This creates a unique report directory below `.runtime/dbt-integration/step4/`.
It extracts only the two former Gold processor Git blobs at fixed commit
`b3fd26a9c4abc45fa3004008be5e660c47f04798` into that runtime directory and /tmp.
It does not restore them to application source or run their publication functions.

## Scope and ownership

- Pin one main head and the two Silver contents. Both source tables and Gold
  namespace must exist. Read counts first; default maximum 10,000 rows/source.
  Stop rather than increase data/resources. This is not the 1.8M benchmark.
- Create unique dbt_gold_test_step4_* targets/baseline from the pinned head;
  deterministic work refs must be absent before prepare and registered as owned.
- Two positive builds: Learning 13 tests + Teaching 15 tests; SQL validation,
  Java metadata guard and atomic publication go to sandbox targets only.
- Replay each successful lifecycle with the same ID; assert target/artifact
  unchanged. One additional Learning build tests candidate-head mutation after
  validation and requires publish refusal. At most three builds, no auto retry.
- PySpark is a temporary independent Fact reference, not a production fallback.
  Read/write through explicitly sandbox-bound Spark catalogs; never invoke old
  processor run()/merge/read-main functions. Full Fact schema/order/types, row
  keys and values must match; generated _gold_updated_at value is excluded.
- Mart is new relative to the former DAG. Verify it with an independent Spark
  numerator/denominator aggregation, not a claimed old deployed Mart.
- Float comparison: abs_tol=1e-9, rel_tol=1e-12; all non-float values, NULLs and
  lineage must match. No raw business rows are written to the report.
- Require main hash unchanged. Cleanup only known owned refs/catalogs, with
  expected-hash deletion. If job status is unknown/active, retain refs instead
  of deleting them under a running build. No warehouse object deletion, VACUUM,
  volume cleanup, Docker recreation or full-DAG trigger.

Sandbox Iceberg data/metadata files and runtime state/artifacts are retained on
Ozone for audit; dropping refs is not physical S3 object reclamation. Docker
containers must retain the same IDs throughout the acceptance run.

## Results - first attempt a1d8ca92f0

Evidence: `.runtime/dbt-integration/step4/a1d8ca92f0/report.json` and the two
worker job directories below `.runtime/dbt-integration/jobs/`.

- Source snapshot: 30 Learning rows and 30 Teaching rows, same pinned main head.
- Learning: two models +13 tests passed, Java validation and sandbox publication
  succeeded; repeated lifecycle left target/artifact unchanged.
- Teaching: one model +15 tests passed in dbt, but the worker result gate refused
  the artifact. Nine generic not-null tests have a real dbt unique_id suffix
  `.HASH` (10 hexadecimal characters). Step 3's gate incorrectly expected
  three-segment IDs without that suffix. This is a gate implementation error,
  not a failed formula/data test. Both receipts remain available for review.
- Fact/Mart parity phase and changed-candidate negative case were NOT reached.
- Main head unchanged; all five owned refs and two owned catalogs removed;
  both worker jobs terminal. Ozone artifact files/state remain for audit.
- No automatic retry or change to formulas, resources or acceptance criteria.

Approved follow-up applied: fixed `lakehouse/dbt/service/result_gate.py` to
recognize valid generic-test suffixes while retaining exact logical test sets,
duplicate rejection and status/failure checks. Update the two receipt-fixture
test files (`test_dbt_result_gate.py`, `test_teaching_progress_gold.py`) to use
realistic IDs and test malformed/duplicate cases. The user separately approved these three gate/test files. No SQL formulas or
business test rules changed. All 60 unit tests passed after the fix; both
original receipts now pass the corrected gate offline.

No production/publication or performance acceptance is implied by this partial
run. The failed attempt is retained rather than replaced by a later result.


## Results - successful fresh attempt 192e219d3f

Evidence: [report](../.runtime/dbt-integration/step4/192e219d3f/report.json).
Gate fix evidence: [report](../.runtime/dbt-integration/step4/gate-fix/report.json).
The first failed attempt remains retained separately.

| Check | Result |
|---|---|
| Learning Outcomes | 2 models, 13 tests PASS |
| Learning Fact parity | 30 rows, 29 compared columns; schema/order/types and values match |
| Learning Mart parity | 3 rows, 12 compared columns; schema/order/types and values match |
| Teaching Progress | 1 model, 15 tests PASS |
| Teaching Fact parity | 30 rows, 11 compared columns; schema/order/types and values match |
| Retry both positive lifecycles | PASS; target head and sealed artifact unchanged |
| Candidate changed after validation | Publication refused as required; sandbox target unchanged |
| Owned cleanup | 3 catalogs and 7 refs removed or verified absent; all jobs terminal |
| Nessie main | Before/after hash identical |
| Docker containers | Same IDs; no recreation or volume changes |

The negative case includes another Learning build with 13 tests. Thus this
attempt executed 41 data tests across three builds: 28 mandatory positive test
nodes plus 13 repeated for the negative case, not 41 distinct business rules.
The worker was restarted only to load the approved gate fix before this attempt.
Baseline reference sources come from the pinned Git commit described above.

This verifies the Gold lifecycle and correctness on the bounded snapshot only.
No full DAG/UI run was triggered, no test output was published to main, and no
performance or production-readiness claim follows from this acceptance.
