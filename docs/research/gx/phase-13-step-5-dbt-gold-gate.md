# Phase 13 - Step 5: dbt Gold GX gate

`api_dataset_pipeline` now executes `gx_validate_gold` after dbt atomically
publishes its validated bundle and before the final Trino consumer smoke test.
The task resolves the published Gold table's exact Nessie commit and Iceberg
snapshot, then invokes the standalone GX service with the registered Gold suite
and `execution_mode=enforce`.

The dependency is:

```text
validate_gold -> publish_gold -> gx_validate_gold -> trino_smoke_test
```

Quality/configuration failures fail immediately. Retryable GX infrastructure or
snapshot instability receives one Airflow retry. The bounded validation summary
is retained in task XCom.

## Candidate limitation

dbt builds per-run alias tables on a candidate Nessie branch. The current GX
service deliberately accepts only the branch mounted by its Trino catalog
(`main` by default), and its versioned Gold suites target canonical table names.
Validating before `publish_gold` would therefore either inspect the old main
table or require unsafe relabeling. This integration validates the exact new
snapshot immediately after dbt's CAS publication and blocks downstream
consumers, but cannot roll back the already completed CAS commit.

A future pre-publication cutover requires a reviewed dynamic-catalog mechanism
in GX that binds the candidate branch and deterministic dbt alias to the
registered canonical asset without accepting arbitrary SQL identifiers.
