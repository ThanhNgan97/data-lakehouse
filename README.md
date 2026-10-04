<!-- DAY10_TEAM_ENTRYPOINT -->

# CTU IOC Data Lakehouse

## Purpose

This repository contains the reproducible development/demo environment for the CTU IOC data lakehouse.

Supported demo datasets:

- `education.learning_outcomes`
- `education.teaching_progress`

The clean-machine objective is to regenerate demo state from tracked source artifacts rather than copying runtime volumes or metadata from the main machine.

## High-level architecture

CTU IOC Mock API -> Airflow api_dataset_pipeline -> Bronze -> Silver -> Gold -> Iceberg / MinIO / Nessie -> Trino -> Superset

PostgreSQL provides metadata databases used by platform services.

## Required tools

The verified development workflow currently requires:

- Git
- Docker Engine
- Docker Compose v2
- Windows PowerShell 5.1 or a compatible PowerShell environment

WSL is not currently proven to be mandatory.

## Environment setup

Create the local environment file from the tracked contract:

```powershell
Copy-Item .env.example .env
```

Then replace required `CHANGE_ME` values with local credentials.

`.env` is local runtime configuration and must not be committed.

Historical Git exposure is not erased by removing a secret from the current working tree. Previously committed credentials require a separate rotation assessment.

## Compose invocation

Use project name `newdirection` and both Compose files:

```powershell
docker compose `
    -p newdirection `
    -f docker-compose.yml `
    -f docker-compose.mock-api.yml `
    up -d
```

Do not start a competing legacy Compose project for the same services and ports.

## Current host ports

| Service | Host port | Container port |
|---|---:|---:|
| Airflow webserver | 8080 | 8080 |
| Trino | 8081 | 8080 |
| Superset | 8088 | 8088 |
| CTU IOC Mock API | 8090 | 8000 |
| Nessie | 19120 | 19120 |
| MinIO API | 9000 | 9000 |
| MinIO Console | 9001 | 9001 |
| PostgreSQL | 5433 | 5432 |
| Kafka | 9094 | configured broker port |

## Host versus container endpoints

Host/browser access uses `localhost:<host-port>`.

Container-to-container communication uses Docker service DNS, for example:

- `minio:9000`
- `nessie:19120`
- `trino:8080`
- `ctu-ioc-mock-api:8000`

Do not replace container DNS addresses with `localhost`.
Do not replace browser-facing localhost URLs with Docker service names.

## Readiness contract

Day 11 automation should wait for:

- PostgreSQL: `pg_isready`
- Airflow scheduler: `airflow jobs check --job-type SchedulerJob`
- Airflow webserver: `GET http://localhost:8080/health`
- MinIO: `GET http://localhost:9000/minio/health/live`
- Nessie: `GET http://localhost:19120/api/v2/config`
- Trino: `GET http://localhost:8081/v1/info`
- Superset: `GET http://localhost:8088/health`
- Mock API: `GET http://localhost:8090/health`

## Superset reconstruction artifacts

Day 10 created secret-safe reconstruction candidates:

- `superset_exports/ctu_ioc_datasources.zip`
- `superset_exports/ctu_ioc_dashboards.zip`

Expected import ordering:

1. datasource/database metadata
2. dashboard bundle

Clean import rehearsal belongs to Day 11.

## Runtime state intentionally regenerated

Do not copy:

- PostgreSQL Docker volume data
- MinIO Docker volume/object state
- Iceberg runtime state
- Nessie branch/history state
- Superset `superset_home` or `superset.db`
- Airflow historical DAG-run metadata

## Day 11 automation target

Day 11 is expected to implement:

- `scripts/bootstrap_team.ps1`
- `scripts/seed_demo.ps1`
- `scripts/verify_team_environment.ps1`
- `scripts/reset_demo.ps1`

## Hardware observations

### MEASURED ON MAIN MACHINE

During the Day 9 audit, the main development machine exposed approximately:

- 23.8 GB visible RAM
- approximately 25.2 GB free on the Windows system drive at the time of measurement

These are observations, not minimum requirements.

### MINIMUM REQUIRED

`TO BE MEASURED ON DAY 12`

## Security rules

Never commit:

- `.env`
- real passwords
- API keys
- access keys
- tokens
- generated credential files
- runtime Docker volume contents

`.env.example` is the canonical variable-name contract and must contain only empty values or placeholders such as `CHANGE_ME`.

## Reproducibility principle

Target clean-machine flow:

clone -> create local .env -> start platform -> wait for readiness -> seed Learning + Teaching -> reconstruct Bronze/Silver/Gold -> import Superset artifacts -> verify environment

No existing MinIO, Iceberg, Nessie, PostgreSQL, or Superset runtime state should be copied from the main machine.

<!-- DAY10_TEAM_REPRO_START -->

## Team Reproducibility

This repository contains the reproducible local Data Lakehouse demo workflow prepared for team handoff.

### Architecture

The demo stack uses Docker Compose with PostgreSQL, MinIO, Nessie, Airflow, Trino, Superset, and the CTU IOC Mock API.
Demo state is reconstructed through source-controlled configuration and pipelines rather than by copying runtime volumes.

### Demo datasets

- `education.learning_outcomes`
- `education.teaching_progress`

Both datasets are triggered through the shared Airflow DAG `api_dataset_pipeline`.

### Required software

- Git
- Docker Desktop / Docker Engine with Compose v2
- Windows PowerShell 5.1 or newer

### Environment setup

Do not share another machine's `.env` file.

```powershell
Copy-Item .env.example .env
# Fill explicitly required local values in .env
```

The real `.env` is local-only and must not be committed.

### First run

```powershell
git clone <repo>
cd lakehouse
Copy-Item .env.example .env
# Fill required local values
.\scripts\bootstrap_team.ps1
.\scripts\seed_demo.ps1
.\scripts\verify_team_environment.ps1
```

### Bootstrap

```powershell
.\scripts\bootstrap_team.ps1
```

Bootstrap validates required tools and Compose, starts the stack, waits for readiness, checks Airflow DAG imports, and imports the tracked Superset assets.

### Seed

```powershell
.\scripts\seed_demo.ps1
```

The seed script triggers exactly Learning Outcomes and Teaching Progress through `api_dataset_pipeline` and verifies the expected eight tasks.

### Verify

```powershell
.\scripts\verify_team_environment.ps1
```

Verification is read-only. It checks services, Airflow, Learning and Teaching data through Trino, and the required Superset inventory.

### Reset / retry

```powershell
.\scripts\reset_demo.ps1 -ShowPlan
```

`reset_demo.ps1` is intentionally guarded on Day 10 and does not prune Docker, delete volumes, delete unrelated projects, or touch protected source.

### Superset

Tracked reconstruction source is stored under `superset_exports/day10/`.

Expected demo state:

- 2 dashboards
- 2 datasets
- 18 charts
- 7 native filters
- 1 Trino database connection

### Service ports

| Service | Container port |
| --- | ---: |
| PostgreSQL | 5432 |
| MinIO API | 9000 |
| MinIO Console | 9001 |
| Nessie | 19120 |
| Airflow Webserver | 8080 |
| Trino | 8080 |
| Superset | 8088 |

Published host ports remain defined by the Compose files.

### Known limitations

- Day 10 validates reproducibility on the existing development machine.
- A genuinely empty clean-clone environment is validated on Day 11.
- Destructive in-place reset is intentionally not automated on Day 10.
- Production monitoring, production secret management, OAuth/mTLS, and unrelated architecture changes are outside this scope.

### Runtime-state rule

**Do not copy Docker volumes or runtime state between machines.**

Do not copy `postgres_data`, `minio_data`, `superset_home`, MinIO objects, Nessie history, Airflow metadata, or existing Iceberg state.

**CLEAN-MACHINE ACCEPTANCE = DAY 11**

<!-- DAY10_TEAM_REPRO_END -->
