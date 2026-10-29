# GX remediation phase C: baseline coverage for dynamic Silver data

Date: 2026-10-28

## Outcome

Dynamic file uploads and Airbyte/database contexts no longer exit with
`NOT_CONFIGURED`. When no effective approved contract route exists, Airflow
builds a safe baseline plan for the routed `lakehouse.silver.*` asset and calls
the same snapshot-aware GX service used by contract validation.

## Baseline suite

The versioned `lakehouse_baseline_silver@1.0.0` SQL suite checks that:

1. the Silver asset exists and exposes columns;
2. it contains at least one row;
3. it has no exact duplicate rows (warning severity).

These are technical invariants, not inferred business semantics. A baseline
pass is returned with `coverage=BASELINE` and `contract_status=MISSING` and must
not be presented as an approved business-contract pass.

## Safety boundary

Baseline assets must match `lakehouse.silver.<safe_identifier>`. Governed routes
still require the processor's asset to match the canonical registry asset; a
mismatch is a configuration error and never silently falls back to baseline.

## Remaining work

This phase does not move writers to Nessie candidate branches. Baseline and
contract checks still observe the currently configured branch (`main` by
default) until the candidate/publication phases are completed. Dynamic API
sources also remain constrained by the existing API ingestion registry; their
generic ingestion adapter is separate from GX baseline execution.
