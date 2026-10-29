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

The `revalidate` transition executes GX again against the branch, snapshot and
commit stored in the manifest; callers cannot replace that identity in the
request body. A passing revalidation stores the fresh receipt and moves the
manifest to `APPROVED_FOR_RELEASE`. A failed attempt returns it to
`QUARANTINED`. Initial manifests use S3 `If-None-Match: *`, while transitions
use `If-Match`, so neither creation nor concurrent updates silently overwrite
governance evidence.

After a guarded publisher observes the exact snapshots on `main`, it writes an
immutable receipt-consumption record below `gx/receipt-consumption` and deletes
the published candidate branch at its expected hash. A retry remains
idempotent: it recognizes the snapshots already on `main`, recreates no data,
and treats an existing audit object as the same consumption event. Active
quarantines use `GX_QUARANTINE_ACTIVE_RETENTION_DAYS`, which must be no shorter
than ordinary result retention.

## Publication receipt keys

GX publication approvals use Ed25519 (`gx-approval/v2`). Only the standalone
GX validator receives `GX_APPROVAL_PRIVATE_KEY`; Airflow publishers receive the
JSON public-key ring in `GX_APPROVAL_PUBLIC_KEYS`. Every receipt carries a
`key_id`, algorithm, issue/expiry timestamps, and the exact candidate identity.

Rotate keys by adding the new public key to the key ring, switching
`GX_APPROVAL_SIGNING_KEY_ID` and the validator private key, waiting for the old
receipt TTL, then removing the old public key. Emergency revocation uses
`GX_APPROVAL_REVOKED_KEY_IDS`; revoked keys cannot sign or authorize publish.

## Service caller identities

The GX HTTP boundary authenticates the caller from its bearer credential, not
from the user-controlled `X-GX-Caller` header. The validator receives a
server-side JSON key ring in `GX_CALLER_CREDENTIALS`; each key is a caller ID
and each value is either one credential or an array during rotation. A
credential can be a development token string or an object containing `token`
and a timezone-aware `expires_at`. Production requires expiry metadata.
Tokens must contain at least 32 characters and cannot be shared by callers.

Airflow receives only its own token as `GX_CALLER_TOKEN` (populated from
`GX_AIRFLOW_CALLER_TOKEN` by Compose) plus `GX_CALLER_ID=airflow`. If the
optional caller header disagrees with the identity bound to the token, the
request is rejected. Only identities listed in `GX_GOVERNANCE_CALLERS` may
manage onboarding and quarantine resources. Production rejects the legacy
shared `GX_SERVICE_TOKEN` configuration.

Rotate a caller credential by temporarily configuring an array containing the
old and new tokens, deploying the new client token, then removing the old token
from the server key ring. Inject production values through service-specific
secret management; never commit them to this repository.

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
