# GX suite registry

Versioned Expectation Suite definitions live in this directory and are built
into the standalone GX image. Runtime processes treat them as read-only.

Each `suite_registry.json` entry must declare:

- `name`
- `version`
- `supported_engines` (`spark` and/or `sql`)
- `contract_version`
- `effective_from`
- `definition` (path relative to this directory)

Business suites are intentionally added in phases 3–4. Validation results and
Data Docs must be written to the configured object store, never this directory.
