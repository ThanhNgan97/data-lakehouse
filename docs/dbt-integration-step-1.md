# dbt integration — Step 1: runtime on newdirection

Status: **PASS**, verified on 2026-10-26 against the live `newdirection` runtime.

Scope: dedicated Python 3.11 runtime with dbt-core 1.9.8 / dbt-trino 1.9.3;
existing Trino 435, Ozone and Nessie. No business SQL, DAG change, Nessie branch,
table creation, Gold write, metadata commit or pipeline execution.

## Files

| File | Purpose |
|---|---|
| `lakehouse/dbt/runtime/Dockerfile` | Build isolated dbt runtime; currently sleeps, no job API |
| `lakehouse/dbt/runtime/requirements.txt` | Direct dependency versions |
| `lakehouse/dbt/runtime/requirements.lock.txt` | Full pinned Python environment |
| `lakehouse/dbt/runtime/preflight.py` | Versions, Trino SELECT 1, Nessie GET config, Ozone HEAD bucket |
| `lakehouse/trino/config.properties` | Preserve existing single-node settings and enable dynamic catalogs |
| `docker-compose.dbt.yml` | Opt-in runtime service and Trino config mount |
| This document | Operation, evidence and rollback |

The lock starts from the preserved P8 Linux Python 3.11 dependency list, plus
pinned boto3 dependencies verified in the local backend. Build runs `pip check`;
the live environment must be verified against the lock before accepting it.
This is not an assertion that all previous package provenance gaps are closed.

## Operation

Supply `S3_ACCESS_KEY` and `S3_SECRET_KEY` from the existing local Ozone runtime
in the calling environment; never put values in this document or Git. No `.env`
file is modified. Docker-to-Docker endpoints use `trino`, `nessie`, `s3g`.

Use this Compose prefix from the repository root:

```powershell
docker compose -p newdirection -f docker-compose.yml -f docker-compose.mock-api.yml -f docker-compose.dbt.yml
```

Append `build dbt-worker`, then `up -d --no-deps trino dbt-worker` after confirming
there are no active queries. Applying the new config recreates only Trino;
the worker is new. Do not run a whole-stack `up`, `down`, volume removal or prune.

Readiness (no business project required):

```powershell
docker compose -p newdirection -f docker-compose.yml -f docker-compose.mock-api.yml -f docker-compose.dbt.yml exec -T dbt-worker python /runtime/preflight.py
```

For an already-created container, this equivalent command does not require
Compose interpolation or credentials in the host terminal:

```powershell
docker exec newdirection-dbt-worker-1 python /runtime/preflight.py
```

`--local` checks package versions only and is the container healthcheck. Healthy
therefore does not mean Ozone/Nessie/Trino connectivity was proven; the full
preflight report is required. No HTTP job API exists until a later approved step.

Java/javac and Iceberg/Nessie/Hadoop classes in Airflow are inspected separately.
No Java guard is compiled or invoked against tables in Step 1.

## Rollback

Stop/remove only the new dbt-worker container (no volume flags). Recreate only
Trino using the original two Compose files, omitting `docker-compose.dbt.yml`.
That removes the added config mount and restores the image's original static
catalog configuration. This rollback interrupts queries; check activity first.
Do not remove existing Ozone, PostgreSQL, Nessie or Research resources.

No JVM, CPU/RAM configuration of Trino, Writer Scaling threshold or catalog SQL
is changed. Dynamic catalog enablement alone does not create any catalog.

## Results

| Check | Observed result |
|---|---|
| Python / dbt-core / dbt-trino | 3.11.14 / 1.9.8 / 1.9.3 |
| Dependency integrity | All 61 lock entries match installed versions; `pip check` PASS |
| dbt executable | `dbt --version` PASS |
| Existing Trino | Version 435, ready; only its container recreated |
| Dynamic catalogs | Mounted config read back and matched; `catalog.management=dynamic` |
| Trino read-only query | `SELECT 1` PASS, query `20261026_070549_00000_5z8m2` |
| Nessie | GET config PASS |
| Ozone | HEAD bucket HTTP 200 |
| Airflow Java | Java/Javac 17.0.20.1; NessieCatalog, HadoopFileIO, S3AFileSystem classes available |
| Shared services | Airflow, Nessie, Superset, Mock API HTTP 200 |
| Protection | All other existing container IDs unchanged; volume and network sets unchanged |

New container: `newdirection-dbt-worker-1` (512 MiB, cpuset `0,1`, no swap).
It currently sleeps and runs a package-only healthcheck; it does not accept jobs.
Full connection readiness is a separate explicit preflight.

Runtime evidence: [report](../.runtime/dbt-integration/step1/2709c7dd52/report.json).
The same directory retains build output, original Trino config, CLI version,
dependency and Java-readiness results. No credentials are written to reports.

Compose reported an existing network ownership-label warning for
`lakehouse-network`. That network predates this step and was reused unchanged;
its ownership was not migrated. There was no network recreation.

Limits of this PASS: no CREATE CATALOG test, business dbt project, models/tests,
Java metadata commit, Nessie branch or publication was executed. Java class
availability does not prove the later guard's Ozone end-to-end compatibility.
Writer Scaling remains at its existing settings; no SHOW SESSION receipt or
performance benchmark was collected in this step.
