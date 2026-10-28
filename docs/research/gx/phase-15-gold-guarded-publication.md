# Phase 15 — GX-guarded Gold publication

The registered API DAG now validates the sealed dbt Gold candidate and
reconciles it with Silver before publication:

```text
validate_gold
  -> gx_validate_gold
  -> gx_reconcile_silver_gold
  -> publish_gold
  -> trino_smoke_test
```

The Gold publisher fails closed unless both GX results carry approval receipts
for the expected canonical route, deterministic physical alias, candidate
branch, snapshot and sealed commit hash. The same checks live inside
`dbt_gold_runtime.publish`, so invoking the legacy CLI command without receipts
cannot bypass GX.

Successful publication records both validation IDs in durable dbt Gold state.
