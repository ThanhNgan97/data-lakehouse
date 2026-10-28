# Phase 14 — dbt Gold candidate binding

## Outcome

GX can validate a sealed dbt Gold candidate before publication without adding
run-specific table names to the contract registry.

The registry remains the authority for the canonical logical asset, for example
`lakehouse.gold.teaching_progress_metrics`. The physical candidate alias and
Nessie branch are derived deterministically from `dataset_id` and `run_id` by
both the Airflow-side gate and the GX service. A caller therefore cannot select
an arbitrary table while reusing an approved contract.

## Gates

- `validate_dbt_gold_candidate` validates the physical Gold candidate against
  the canonical Gold contract.
- `reconcile_dbt_gold_candidate` runs reconciliation with both Gold and Silver
  reads pinned to the same request-scoped Nessie branch.
- The state must already be sealed by dbt validation, and its snapshot and
  commit hash must still match Nessie before GX is called.

## Deliberate boundary

This phase introduces the safe binding and candidate gates. It does not yet
rewire the production DAG or change the Gold publisher. Until the publisher is
changed to require both GX approvals, the existing DAG still validates Gold
after publication. The next cutover must place both candidate gates before
publish and make publication consume their immutable approval receipts.
