# GX suite registry

Versioned Expectation Suite definitions live in this directory and are built
into the standalone GX image. Runtime processes treat them as read-only.

Each `suite_registry.json` entry must declare:

- `name`
- `version`
- `supported_engines` (`spark` and/or `sql`)
- `contract_version`
- `effective_from`
- `definitions` (engine-to-path mapping relative to this directory)

Spark definitions use native GX JSON. SQL definitions use the local
`lakehouse-sql-suite/v1` format and execute one scalar query per expectation so
engine behavior stays explicit and independently testable. Validation results and
Data Docs must be written to the configured object store, never this directory.

Versioned data contracts are registered separately in `contract_registry.json`.
Contracts define layer schemas, evolution policy, rule severity, thresholds,
and engine compatibility. Expectation suites in phase 4 implement those rules;
they do not redefine the contract.

`validation_registry.json` is the phase-7 routing layer. Its immutable key is
`dataset_id + layer + contract_version`; engine selects an implementation, while
`file`, `api`, and `database` are compatibility metadata and never choose a
different business suite. Streaming is disabled until a dataset declares an
explicit micro-batch/window policy.
