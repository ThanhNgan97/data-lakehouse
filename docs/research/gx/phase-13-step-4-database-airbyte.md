# Phase 13 - Step 4: Database/Airbyte GX integration

The relational branch of `universal_lakehouse_pipeline` now has its own
`gx_validate_database` task. The file and database branches converge only after
their respective GX boundary, and the database portal displays the real GX
status returned for the exact Airflow run.

For a registered dataset, the gate requires the processor's final Silver table
to exactly match the canonical asset in the local registry. It then resolves
the current Nessie branch, Iceberg snapshot ID, and Nessie commit and validates
with `source_kind=database`. Quality/configuration errors fail immediately;
retryable infrastructure errors receive one Airflow retry.

Dynamic relational contexts currently create tenant/context-specific tables.
When no reviewed mapping to a registered GX contract exists, the gate returns
`NOT_CONFIGURED` with `merge_allowed=null`. The portal explicitly states that
this is not a GX pass.

## Current boundary limitation

`spark_relational_context_processor.py` currently creates Silver and Gold and
merges its candidate branch inside one Spark task. Therefore this step adds an
auditable post-processing GX boundary that blocks the downstream Trino smoke
test and Superset provisioning, but it cannot undo that earlier merge. A true
pre-merge database gate requires splitting the processor into durable candidate
creation and CAS publication tasks.
