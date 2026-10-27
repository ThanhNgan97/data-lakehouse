# Phase 13 - Step 6: Silver-Gold reconciliation

`api_dataset_pipeline` now runs `gx_reconcile_silver_gold` after the standalone
Gold validation and before the final Trino consumer smoke test:

```text
publish_gold -> gx_validate_gold -> gx_reconcile_silver_gold
             -> trino_smoke_test
```

The task routes to the versioned `reconciliation` suite for the selected
dataset. Those suites verify row-count equality, key-set equality, and lineage
or business-value agreement between the registered Silver and Gold tables.

Gold is the reconciliation identity anchor. The request carries its exact
Iceberg snapshot ID and the shared Nessie commit. GX resolves that identity
before validation and verifies it again afterward, so a concurrent commit
invalidates the evidence instead of allowing results from mixed revisions.

Quality/configuration failures fail immediately. Retryable infrastructure or
snapshot-instability errors receive one Airflow retry. A successful bounded
summary is stored in XCom with both Silver and Gold asset references.

Like the preceding Gold gate, reconciliation occurs immediately after dbt CAS
publication because the current GX Trino catalog only mounts `main`. It blocks
downstream consumers but does not roll back the published commit.
