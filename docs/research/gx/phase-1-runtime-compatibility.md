# Great Expectations Phase 1 Runtime Compatibility

## Outcome

Great Expectations Core `1.24.0` is compatible with the current research runtime for the scope tested in Phase 1.

GX can validate an in-memory PySpark `3.5.0` DataFrame, correctly passes valid data, correctly fails invalid data, and does not break the selected existing Airflow, Spark, quality, orchestration, or DAG tests.

Phase 1 does not create a persistent GX project, integrate GX into an Airflow task, or change Nessie publication behavior.

## Runtime under test

| Component | Version or value |
|---|---|
| Base image | `apache/airflow:2.9.2-python3.10` |
| Airflow | `2.9.2` |
| Python | `3.10` |
| PySpark | `3.5.0` |
| Java | OpenJDK `17.0.20.1` |
| GX Core | `1.24.0` |
| Pydantic after rebuild | `2.14.1` |
| HTTPX after rebuild | `0.28.1` |
| Research image size | `1,235,310,771` bytes |

## Implementation change

The Airflow image now installs a pinned GX version:

```text
great_expectations==1.24.0
```

Pinning prevents an unreviewed GX API upgrade during later image rebuilds.

## Image build result

| Check | Result |
|---|---|
| Docker build | Pass |
| Build exit code | `0` |
| Clean build wall time observed | `371.821` seconds |
| Spark dependency resolution | Pass |
| Final image created | Pass |

The build log briefly reported Spark heartbeat registration errors while the dependency-resolution helper was copying a large AWS SDK JAR. Spark recovered, registered the BlockManager, stopped successfully, and the image build completed with exit code zero. This should be watched on future clean builds but did not invalidate the produced image.

## Dependency compatibility checks

| Check | Result |
|---|---|
| `pip check` | No broken requirements found |
| Import Airflow | Pass |
| Import GX | Pass |
| Import PySpark | Pass |
| Import `psycopg2` | Pass |
| Airflow metadata migration | Pass |

The existing Dockerfile installs several packages without version pins. Rebuilding the shared install layer upgraded packages including Google GenAI, Pydantic, HTTPX, Google Auth, and document-processing libraries. Selected regression tests pass, but a future production feature should either pin the complete application dependency set or use an Airflow-compatible constraints strategy so a GX rebuild cannot silently upgrade unrelated libraries.

## GX Spark smoke test

The smoke test uses:

- An ephemeral GX Data Context.
- A GX Spark Data Source.
- A whole-dataframe Batch Definition.
- An Expectation Suite with two Expectations.
- A Validation Definition.
- A Checkpoint.

Expectations:

1. `id` values must not be null.
2. `progress` values must be between 0 and 100 inclusive.

Results:

| Scenario | Expected | Actual |
|---|---|---|
| Rows with progress 80 and 100 | Pass | Pass |
| A row with progress 101 | Fail | Fail |

- Tests run: `2`
- Passed: `2`
- Failed: `0`
- Test-runner time: `12.690` seconds
- Host-observed container wall time: `22.617` seconds

The smoke test intentionally uses ephemeral configuration. Its purpose is API and execution-engine compatibility; persistent Stores and repository structure belong to Phase 2.

## Regression result

The baseline tests and GX smoke tests were run together after rebuilding the image:

- Existing baseline tests: `52`
- GX smoke tests: `2`
- Total: `54`
- Passed: `54`
- Failed: `0`
- Combined test-runner time: `33.459` seconds
- DAG import errors: `0`

## Observed warnings

The following warnings did not fail tests:

- Spark could not load a native Hadoop library and used built-in Java implementations.
- The Airflow image does not contain the `ps` utility used by a Spark environment helper.
- Existing Spark tests emitted unclosed socket `ResourceWarning` messages.
- Airflow emitted existing `schedule_interval` deprecation warnings.
- Google Auth warned about a future minimum `grpcio` version for post-quantum cryptography support.
- GX emits extensive registration logs when imported under Airflow logging.

The GX registration log volume should be reduced or redirected before production integration so Airflow task logs remain readable.

## Phase 1 acceptance criteria

| Criterion | Status |
|---|---|
| Image builds with pinned GX | Pass |
| GX imports in Airflow image | Pass |
| GX validates Spark DataFrame | Pass |
| Valid input passes | Pass |
| Invalid input fails | Pass |
| Airflow metadata migration works | Pass |
| Existing selected tests remain green | Pass |
| DAGs import without errors | Pass |

## Decision

Phase 1 result: `PASS`.

The project can proceed to Phase 2: create the reusable GX framework and persistent project structure. Phase 2 should preserve the working smoke test and address dependency reproducibility and GX log verbosity without yet changing pipeline publication behavior.

