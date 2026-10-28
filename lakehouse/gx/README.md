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

## Contract lifecycle

Every registry entry and contract definition must agree on `dataset_id`,
`contract_version`, `status`, `owner`, and `effective_from`. Supported states are:

`draft -> review -> pilot -> active -> deprecated -> retired`

Only `active` and unexpired `deprecated` contracts are production-routable.
`pilot` is allowed only when `GX_ALLOW_PILOT_CONTRACTS=true`, which the GX
runtime refuses in production. Activating a contract requires immutable
`approved_by` and timezone-aware `approved_at` evidence in the registry.

Every version after the first declares `supersedes`. Breaking schema or grain
changes require a higher major version; compatible additions may use a minor or
patch version. `effective_to` can close the routing window without deleting
historical contract evidence.

## Onboarding and candidate quarantine

`POST /v1/onboarding/proposals` accepts a complete draft/review contract plus
canonical Silver/Gold assets and allowed source kinds. GX validates contract
semantics and stores an immutable, content-addressed proposal in object storage.
It never edits the mounted Git-backed registries; activation still requires a
reviewed repository change and lifecycle approval evidence.

Failed contract validations and `NOT_CONFIGURED` baseline observations create a
candidate-level quarantine manifest containing dataset, run, contract, branch,
snapshot, commit, validation result, and failure reason. Governance callers can
inspect manifests and request these transitions:

`QUARANTINED -> REVALIDATION_REQUESTED -> APPROVED_FOR_RELEASE`

Release requires a fresh signed GX receipt bound to the exact candidate.
`APPROVED_FOR_RELEASE` does not publish data itself; the guarded publisher
remains the only component allowed to merge the candidate. A rejected candidate
may instead transition to `DISCARDED`. Updates use the object ETag as an
optimistic concurrency guard.

## Publication receipt keys

GX publication approvals use Ed25519 (`gx-approval/v2`). Only the standalone
GX validator receives `GX_APPROVAL_PRIVATE_KEY`; Airflow publishers receive the
JSON public-key ring in `GX_APPROVAL_PUBLIC_KEYS`. Every receipt carries a
`key_id`, algorithm, issue/expiry timestamps, and the exact candidate identity.

Rotate keys by adding the new public key to the key ring, switching
`GX_APPROVAL_SIGNING_KEY_ID` and the validator private key, waiting for the old
receipt TTL, then removing the old public key. Emergency revocation uses
`GX_APPROVAL_REVOKED_KEY_IDS`; revoked keys cannot sign or authorize publish.

## Production operations

Start the optional monitoring profile with:

`docker compose --profile gx-observability up -d`

Prometheus scrapes the GX runtime and loads `monitoring/gx/gx-alerts.yml`;
Grafana provisions the `GX Production Overview` dashboard. Production must
replace the example Grafana password before exposing the port.

Object retention is deliberately dry-run by default. Review the JSON report:

`docker compose --profile gx run --rm --entrypoint python gx-validator -m gx_runtime.retention`

Only after confirming the configured `gx/` prefixes should an operator set
`GX_RETENTION_DRY_RUN=false`. Each execution is capped by
`GX_RETENTION_MAX_DELETE`; scheduling belongs to the platform scheduler so the
cleanup policy remains observable and separately authorized.

`validation_registry.json` is the phase-7 routing layer. Its immutable key is
`dataset_id + layer + contract_version`; engine selects an implementation, while
`file`, `api`, and `database` are compatibility metadata and never choose a
different business suite. Streaming is disabled until a dataset declares an
explicit micro-batch/window policy.
