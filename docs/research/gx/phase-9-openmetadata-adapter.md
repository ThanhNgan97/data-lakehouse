# Phase 9: OpenMetadata integration contract and adapter

Date: 2026-10-27

## Outcome

GX now exposes a small, version-independent metadata event and an optional
OpenMetadata HTTP adapter. This phase does not install or operate an
OpenMetadata server and does not add the heavy `openmetadata-ingestion` package
to the GX runtime.

One event is emitted per suite because a single validation can execute multiple
suite versions. The event contract is:

```text
dataset
layer
run_id
suite_name
suite_version
status
branch
snapshot_id
commit_hash
data_docs_url
validation_timestamp
validation_id
validation_uri
```

Snapshot identity and the durable result URI are mandatory. Export is rejected
if they are absent.

## Privacy boundary

The event intentionally excludes:

- source rows and failed-row samples;
- expectation `observed_value` payloads;
- critical/warning failure messages;
- credentials and source connection details.

OpenMetadata receives traceability and status. Full GX evidence remains in the
protected validation-result store referenced by `validation_uri`. Data Docs is
only linked when the summary already provides an approved URL.

## OpenMetadata mapping

The adapter writes a test result for a pre-provisioned OpenMetadata TestCase.
Its generated FQN is deterministic from service, layer, dataset, suite, and
suite version. The TestCase/TestDefinition creation lifecycle remains owned by
OpenMetadata administration; publishing a result does not silently create or
alter governance entities.

| GX status | OpenMetadata status |
|---|---|
| `PASSED` | `Success` |
| `FAILED` | `Failed` |
| `WARNING` | `Aborted` |

OpenMetadata has no warning result state. `Aborted` is used as a neutral state,
while the original `gx_status=WARNING` remains in `testResultValue`.

The default `current` API style uses:

```text
POST /api/v1/dataQuality/testCases/testCaseResults/{fqn}
```

`GX_OPENMETADATA_API_STYLE=legacy` is available for deployments that expose the
older `PUT .../{fqn}/testCaseResult` endpoint. The deployment must select the
style matching its server version.

## Runtime policy

Configuration is environment based:

```text
GX_OPENMETADATA_ENABLED=false
GX_OPENMETADATA_REQUIRED=false
OPENMETADATA_HOST_PORT=http://openmetadata_server:8585
OPENMETADATA_JWT_TOKEN=<secret>
OPENMETADATA_SERVICE_NAME=lakehouse-trino
GX_OPENMETADATA_API_STYLE=current
GX_OPENMETADATA_TIMEOUT_SECONDS=10
```

- Disabled is the default because no OpenMetadata server is in GX scope.
- Best-effort is the default when enabled. Export failure is counted and logged
  but cannot reverse a valid snapshot quality decision.
- Required mode converts export failure to retryable `GX_EXECUTION_ERROR`. This
  is suitable only when metadata publication is an explicit operational SLA.

The adapter runs after the validation summary has been persisted and after the
snapshot-aware gate passes. The GX HTTP response returns only aggregate export
counts; the Airflow XCom allowlist continues to exclude metadata details.

## Verification

Automated tests verify:

- one event per suite and complete required fields;
- absence of raw failures and observed values;
- mandatory snapshot identity and result URI;
- JWT header, request method, endpoint, timeout, and deterministic FQN;
- status mapping, including neutral warning handling;
- current and explicitly configured legacy API styles;
- best-effort versus required failure behavior;
- disabled-by-default configuration and missing-secret rejection.

No live OpenMetadata write was attempted because this repository does not run
an OpenMetadata server. HTTP behavior is verified using the same request object
and serialized payload that production will send.
