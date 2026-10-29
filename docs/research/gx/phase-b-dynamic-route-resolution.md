# GX remediation phase B: dynamic route resolution

Date: 2026-10-28

## Outcome

Airflow quality gates no longer maintain a Python whitelist or copy GX
contract versions from the API ingestion registry. File, API, database, Gold,
and reconciliation gates resolve the latest effective route from the mounted
GX contract and validation registries.

The GX HTTP service accepts an omitted `contract_version` and resolves the
latest effective route itself. An explicitly requested version remains
supported for replay and compatibility.

## Configuration boundary

Airflow mounts `lakehouse/gx` read-only at `/opt/airflow/gx-config`. Route
discovery filters by dataset, layer, source kind, execution engine, and
`effective_from`. The GX service remains authoritative: it loads the contract,
verifies suite compatibility, resolves the canonical asset, and executes the
quality policy.

Adding a contract-backed dataset no longer requires editing a GX gate. It does
require declarative contract, suite, and validation route entries. Baseline
coverage for datasets without those entries is intentionally deferred to phase
C; until then they continue to report `NOT_CONFIGURED`.
