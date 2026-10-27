# dbt integration - Step 3: dbt-only API Gold orchestration

Status: code integrated; unit tests and worker readiness passed. This is NOT
an end-to-end data or production acceptance result. No business job, DAG run,
catalog creation, metadata guard invocation or publication was executed here.

## Resulting DAG

```text
api_preflight -> ingest_bronze -> validate_bronze
-> process_silver -> validate_silver
-> prepare_gold -> process_gold -> validate_gold -> publish_gold
-> trino_smoke_test
```

API/Bronze/Silver retain their existing Spark implementations. Both registered
datasets route Gold directly to dbt; there is no Gold engine switch or automatic
Spark fallback. Learning produces Fact + period Mart in one output bundle;
Teaching produces its existing Fact. Final smoke checks cover every output.

The DAG remains manual (`schedule_interval=None`, `max_active_runs=1`).
Airflow loads these files from its existing bind mounts. Users can inspect the
10-task graph; triggering it is not part of this checkpoint and would write
Bronze/Silver and eventually publish Gold to the default Nessie main target.

## Approved file changes (18)

| Action | Path | Purpose |
|---|---|---|
| Modify | `lakehouse/dags/api_dataset_pipeline.py` | Add prepare/publish Gold tasks and dependencies |
| Modify | `lakehouse/spark/api_dataset_registry.py` | Replace Gold processor route with validated dbt selector |
| Modify | `lakehouse/spark/api_dataset_orchestration.py` | Dispatch dbt lifecycle; remove Spark Gold processing/validation; smoke all outputs |
| Modify | `docker-compose.dbt.yml` | Run private worker API; bind project/contract/service read-only; persist job artifacts |
| Add | `lakehouse/dbt/service/worker.py` | Allowlisted serial build worker; candidate catalog lifecycle and sealed receipt |
| Add | `lakehouse/dbt/service/result_gate.py` | Exact required models/tests, build command/version, statuses and SHA256 verification |
| Add | `lakehouse/spark/dbt_gold_contract.py` | Deterministic dataset/run identity, selectors and output bundle |
| Add | `lakehouse/spark/dbt_gold_runtime.py` | Ozone state, prepared source, candidate validation, CAS publication |
| Add | `lakehouse/spark/dbt_gold_metadata.py` | Compile/invoke metadata-only Java helper without SparkSession |
| Add | `lakehouse/spark/java/GoldMetadataGuard.java` | Set/read back Iceberg metadata retention properties on isolated work refs |
| Modify | `lakehouse/spark/tests/test_api_dataset_orchestration.py` | dbt routing and 10-command regression checks |
| Modify | `lakehouse/spark/tests/test_api_dataset_pipeline_dag.py` | DAG import and exact 10-task dependency graph |
| Modify | `lakehouse/spark/tests/test_teaching_progress_gold.py` | Replace old Spark-specific harness with mandatory Teaching receipt contract checks |
| Add | `lakehouse/spark/tests/test_dbt_gold_runtime.py` | Source/artifact/head guards, retry, conflicts, atomic bundle publication |
| Add | `lakehouse/spark/tests/test_dbt_result_gate.py` | Missing/duplicate/failed/skipped tests; reject compile artifact; reject changed seal |
| Add | `docs/dbt-integration-step-3.md` | This implementation and verification record |
| Delete | `lakehouse/spark/spark_learning_outcomes_to_gold.py` | Remove replaced API Learning Gold processor |
| Delete | `lakehouse/spark/spark_teaching_progress_to_gold.py` | Remove replaced API Teaching Gold processor |

The two deleted processors were verified present in origin/develop before
removal. No remaining Python source references them. Historical audit documents
and the frozen research snapshot are retained. Generic PDF/tabular Gold and
the Bronze/Silver processors are not deleted or changed.

## Lifecycle and execution ownership

1. `prepare_gold`: freeze target head and Silver content, require an existing
   Gold namespace, create deterministic `dbt_gold_<job>` work ref, persist state
   under `orchestration/dbt_gold/<job>.json` in the configured Ozone bucket.
2. `process_gold`: submit dataset/run ID/expected head to the worker. It creates
   a catalog bound to that ref, enables native S3 and disables Hadoop fallback
   for Trino, then runs `dbt build --select +<selector>` with per-run aliases.
   SQL models/macros/tests remain byte-identical to Step 2/P8.
3. Worker success requires the complete bundle and mandatory tests: Learning
   2 physical models +13 tests; Teaching 1 physical model +15 tests. Missing,
   duplicate, failed, warned or skipped nodes fail closed. `compile`, `run`,
   `test` receipts cannot replace a `build` receipt. Catalog cleanup must pass.
4. `validate_gold`: verify receipt SHA256/identity/contract, frozen Silver and
   candidate head. Java guards candidate metadata (no SparkSession); runtime
   verifies unchanged snapshot/schema/spec/sort IDs and metadata properties.
5. `publish_gold`: compare target against prepare-time head/content, then PUT
   the entire output bundle in ONE Nessie expected-hash commit. Never merge the
   whole work branch or overwrite a changed target. Read back committed pointers.
6. `trino_smoke_test`: read the published output names using the application
   main catalog. This smoke is for the normal DAG; sandbox verification must
   explicitly query the sandbox catalog or compare its contents separately.

All configuration uses S3_* / NESSIE_API_URL. Trino credentials are environment
references, not literal values embedded in catalog SQL. The Java metadata-only
helper uses the existing Airflow Iceberg/Hadoop classpath and S3A to read/write
metadata; that is separate from Trino's disabled Hadoop fallback.

## Runtime and retry boundaries

Only `newdirection-dbt-worker-1` is recreated. Its existing image/dependency
lock and 512 MiB/cpuset 0,1 limits are retained. The image's sleep command is
overridden by Compose to `python /service/worker.py`. There is no host port;
Airflow reaches `http://dbt-worker:8080` on the existing private network.

Job status, build log and dbt target/log artifacts persist below
`.runtime/dbt-integration/jobs/<job>/`. The worker never runs a queued job merely
because it starts: interrupted queued/running records are marked failed, and
require a new run ID. Duplicate requests do not submit another build. A failed
build is not automatically retried or switched to Spark.

Published retry is a no-op. If a commit succeeded but saving state failed, retry
recognizes only the WHOLE promoted bundle; a partial bundle is not accepted.
If candidate/target state changes or an interrupted metadata guard cannot be
proven complete, stop and use a new run. Not every interruption is resumable.
Job/artifact retention and orphan branch cleanup remain explicit operational
work; this step does not introduce automatic garbage collection.

GOLD_TARGET_REF defaults to main and only allows main or dbt_gold_test_*.
Sandbox use is a later controlled CLI/harness step, not a new public UI input.
Old/unknown state validation contracts are refused. Fresh deployment requires
explicit Gold namespace initialization; prepare does not silently create it.

## Verification

- 57 unit tests passed in the Airflow environment. External Gold HTTP/S3/Nessie
  operations and Java guard execution are mocked in runtime tests.
- DagBag imports and exact task graph tested; API DAG had no active runs before
  editing. No test invokes a DAG task against business data.
- Java guard javac PASS against current Airflow dependencies; main not invoked.
- Gate accepted 16 archived P8 Learning build receipts (measured + warmups);
  no old workload rerun. It rejected Step 2's real compile run_results artifact.
- Only worker container recreated. Other container IDs and volume set unchanged.
- Worker /health PASS, no job submitted and no job status created.

Evidence is in `.runtime/dbt-integration/step3/`: unit-tests.log,
offline-gate-check.json, java-compile.log, worker-readiness.json and report.json.
No timing/performance claim is made from unit tests or archived receipts.

Pre-existing Airflow schedule_interval deprecation and Compose network ownership
warnings are not caused or repaired by the Gold integration.

## Next acceptance gate

Step 4 must execute controlled Ozone/Nessie sandbox builds for BOTH datasets,
verify actual Trino catalog support/cleanup, schema/lineage/formulas, Java guard,
same-snapshot parity and retry/failure behavior before a real DAG publication.
Passing unit tests does not establish those properties on live data. Do not
trigger the end-to-end UI workflow as a substitute for sandbox acceptance.
