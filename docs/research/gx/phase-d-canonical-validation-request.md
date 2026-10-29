# Phase D - Canonical GX validation request

Phase D replaces the request dictionaries previously assembled independently by
each Airflow gate with one dependency-free protocol:
`GXTableValidationRequest`.

## Boundary

Every table gate sends the same fields to the GX service:

- logical identity: `dataset_id`, `data_layer`, `source_kind`, `run_id`;
- physical identity: `data_asset_ref`;
- immutable Nessie identity: `nessie_branch`, `snapshot_id`,
  `nessie_commit_hash`;
- validation policy: `contract_version`, `execution_engine`,
  `execution_mode`, `coverage`.

The protocol validates required values, supported enums, and safe Iceberg asset
names before the HTTP call. The service parses the same protocol again at its
trust boundary. For contract-backed validation it also requires
`data_asset_ref` to match the canonical route resolved from the registry. A
caller therefore cannot select a different table while claiming a registered
dataset.

## Coverage behavior

- `CONTRACT` resolves an approved registry route and its versioned suites.
- `BASELINE` is limited to Silver and requires contract version
  `baseline-1.0.0`.

File, API, database, Gold, and reconciliation gates all use this protocol.
Dataset-specific mappings remain configuration in the registries; they are not
embedded in the request class or gate code.

## Out of scope

This phase does not create a Nessie candidate branch and does not perform a
merge. It makes the validation target unambiguous so the later candidate-state
and pre-merge phases can enforce that the exact validated snapshot is the one
being promoted.
