# Phase 13 step 1: universal file-upload GX integration

Date: 2026-10-27

## Integration point

`universal_lakehouse_pipeline` now joins every processing branch at
`gx_validate_file_upload` before the Trino smoke/archive task and dynamic
Superset publication. For a registered dataset, the task:

1. consumes the dataset and Silver asset chosen by `ai_semantic_profiler`;
2. resolves the current Nessie branch hash and Iceberg snapshot ID;
3. sends an `enforce` request with `source_kind=file` to the standalone GX API;
4. returns the bounded GX summary through Airflow XCom;
5. prevents downstream tasks when GX returns a quality/configuration error.

The supported file routes are currently `education.teaching_progress` and
`education.learning_outcomes`, matching the GX registry. An arbitrary dynamic
upload has no data contract, so its XCom result is explicitly
`status=NOT_CONFIGURED` and `merge_allowed=null`; it is never presented as a GX
pass.

## Current boundary

This step integrates GX into the existing universal DAG without rewriting its
four transformation branches. Some legacy/generic processors currently write
their Iceberg outputs before the join. Therefore the new task is already a hard
gate for downstream archive/smoke/dashboard publication, but those processors
must be migrated to candidate branches before this can be called a universal
pre-write or pre-merge gate. Registered candidate-based pipelines remain the
target architecture.

User Portal rendering is deliberately step 2. It will read the
`gx_validate_file_upload` XCom and distinguish `PASSED`, `FAILED`,
`NOT_CONFIGURED`, execution errors, and snapshot mismatch.

## Verification

- unit tests cover exact Nessie snapshot identity, registered GX execution, and
  honest `NOT_CONFIGURED` reporting;
- Airflow imports the DAG without errors;
- the task tree places `gx_validate_file_upload` after each processing branch
  and before `join_and_smoke_test` and `auto_provision_superset`.
