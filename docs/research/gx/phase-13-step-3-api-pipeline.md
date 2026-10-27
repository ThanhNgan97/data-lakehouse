# Phase 13 - Step 3: API dataset pipeline GX integration

`api_dataset_pipeline` now runs `gx_validate_silver` after the existing Silver
validation and before any Gold preparation or processing. The gate resolves the
current Nessie/Iceberg identity of the registered Silver table and sends that
exact branch, snapshot ID, and commit hash to the standalone GX service with
`source_kind=api` and `execution_mode=enforce`.

Both registered API datasets declare their GX contract version in the dataset
registry. A GX quality/configuration failure stops the DAG, while retryable GX
infrastructure and snapshot-mismatch errors receive one Airflow retry. The task
returns the bounded GX summary in XCom for operational evidence.

## Dependency

```text
process_silver -> validate_silver -> gx_validate_silver
               -> prepare_gold -> process_gold -> validate_gold
               -> publish_gold -> trino_smoke_test
```

## Current publication boundary

The existing Silver processors still merge their output to `main` before the
GX task. This integration therefore gates Gold processing and publication, but
does not retroactively prevent the Silver write. A true pre-merge Silver gate
requires the writer to expose durable candidate branch/snapshot state and is a
separate cutover from this integration.
