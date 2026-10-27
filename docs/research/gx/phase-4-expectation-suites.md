# Phase 4: versioned expectation suites

Date: 2026-10-27  
Dataset: `education.teaching_progress`  
Contract: `1.0.0`

## Outcome

Phase 4 turns the phase-3 contract into immutable, Git-backed suite definitions.
Routing is explicit by suite version and execution engine; runtime code does not
select a latest version implicitly.

| Suite | Version | Engine | Expectations | Definition |
|---|---:|---|---:|---|
| `teaching_progress_silver_core` | `1.0.0` | Spark | 11 | Native GX JSON |
| `teaching_progress_silver_core` | `1.0.0` | SQL/Trino | 6 | `lakehouse-sql-suite/v1` |
| `teaching_progress_gold_core` | `1.0.0` | SQL/Trino | 5 | `lakehouse-sql-suite/v1` |
| `teaching_progress_silver_gold_reconciliation` | `1.0.0` | SQL/Trino | 3 | `lakehouse-sql-suite/v1` |

Every registry entry declares `name`, `version`, `supported_engines`,
`contract_version`, `effective_from`, and an engine-specific definition. SQL
suite files repeat the identity metadata; the native GX suite carries the
contract version in `meta` while its routing metadata remains authoritative in
the registry.

## Engine compatibility decision

Spark and SQL are intentionally not treated as equivalent:

- Spark uses native GX expectations for required columns, seven independent
  not-null checks, candidate-key uniqueness, non-empty volume, and progress
  range.
- Trino uses one read-only scalar query per contract rule. It additionally owns
  source freshness because the threshold depends on `current_timestamp`.
- Cross-table reconciliation is SQL-only because Silver and Gold must be read
  from the same Nessie-visible catalog state. The phase-3 contract compatibility
  declarations were narrowed accordingly.
- Candidate-key uniqueness remains a warning until the source owner verifies
  the business grain. All other data-integrity failures remain critical.

## Independent expectation verification

The automated tests inject a failure into each SQL expectation while keeping
all sibling expectations passing, then verify the reported type and severity.
Spark fixtures independently exercise schema, every required column, duplicate
candidate keys, empty volume, and out-of-range progress. A clean Spark fixture
must pass all 11 expectations.

The final local verification used image `lakehouse-gx-validator:1.24.0` and the
running demo Trino service. Read-only execution against Nessie `main` commit
`07224a10eca333523113fff048aed0cdf329c466140e877f1c174aff3bf58fc8`
produced:

| Execution | Snapshot | Result |
|---|---:|---|
| Silver core | `4366971137478837827` | 6/6 passed |
| Gold core | `5052954023966271510` | 5/5 passed |
| Silver/Gold reconciliation | Silver + Gold current snapshots | 3/3 passed |

These results are pilot evidence for the demo snapshot, not a replacement for
production source-owner confirmation or an approved freshness SLA.
