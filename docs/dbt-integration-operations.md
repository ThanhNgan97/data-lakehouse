# dbt + Trino integration: operations

## Scope and accepted behavior

The API dataset DAG uses Spark for Bronze/Silver and dbt + Trino for Gold.
There is no automatic PySpark Gold fallback. Learning Outcomes publishes Fact
and period Mart together; Teaching Progress publishes its Fact. Publication
defaults to Nessie `main` and writes data when a DAG is triggered.

References: [sandbox acceptance](dbt-integration-step-4.md) and
[portal acceptance](dbt-integration-step-5.md). These establish functional
acceptance on demo data, not production load or performance acceptance.

## Prerequisites

- Use the existing `newdirection` project in the repository root. Confirm its
  containers, mounts, network and volumes before changing runtime services.
- Ozone S3 Gateway, Nessie, Trino 435 and Airflow must be available on the
  existing network. Internal endpoints are `s3g:9878`, `nessie:19120` and
  `trino:8080`; host mappings differ (Trino uses 8081 on the local host).
- Supply existing `S3_ACCESS_KEY` and `S3_SECRET_KEY` through the calling
  environment or existing local configuration; never commit their values.
- The configured bucket, Silver tables and Gold namespace must already exist.
  Namespace initialization is an explicit deployment prerequisite.
- Airflow needs a JDK (`java` and `javac`) and the tested Iceberg/Hadoop/S3A
  libraries for the metadata helper. The helper does not start SparkSession.
- The worker needs writable `.runtime/dbt-integration/jobs`; project, service
  and contract mounts are read-only. Retain artifacts needed by active/retried
  runs. This directory is ignored by Git, not backed up automatically by Git.

## Start or update only the integration services

These are operator commands, not automatically executed by this document.
Ensure no active DAG runs or queries before updating/recreating services.
Review the effective service names without printing interpolated credentials:

```powershell
docker compose -p newdirection -f docker-compose.yml -f docker-compose.mock-api.yml -f docker-compose.dbt.yml --profile dbt config --quiet
docker compose -p newdirection -f docker-compose.yml -f docker-compose.mock-api.yml -f docker-compose.dbt.yml --profile dbt config --services
```

Build the pinned runtime image, then apply only the integration services:

```powershell
docker compose -p newdirection -f docker-compose.yml -f docker-compose.mock-api.yml -f docker-compose.dbt.yml --profile dbt build dbt-worker
docker compose -p newdirection -f docker-compose.yml -f docker-compose.mock-api.yml -f docker-compose.dbt.yml --profile dbt up -d --no-deps trino dbt-worker
```

This can recreate services if their configuration changed. Do not run a
whole-stack down/up, prune, volume removal or warehouse cleanup as a routine
integration update. The worker image defaults to sleep when run alone; the
Compose overlay supplies the worker command and mounts.

Python source is bind-mounted. A running worker must be restarted during an
idle maintenance window to load changes to worker/service code. Do not restart
it during a build. Airflow tasks are separate Python processes and load runtime
source on their next invocation. No service restart was performed as part of
the final source review changes.

## Configuration contract

| Variable | Consumer / behavior |
|---|---|
| `DBT_TRINO_HOST` | Worker and dbt profile; default `trino` |
| `DBT_TRINO_PORT` | Worker and dbt profile; default `8080` |
| `S3_REGION` | Worker candidate catalog and runtime S3 client; default `us-east-1` |
| `S3_ENDPOINT`, `S3_BUCKET_NAME` | Existing Ozone endpoint and warehouse bucket |
| `S3_ACCESS_KEY`, `S3_SECRET_KEY` | Existing Ozone credentials; Trino must have matching environment credentials |
| `NESSIE_API_URL` | Existing Nessie API, normally `http://nessie:19120/api/v1` |
| `DBT_GOLD_WORKER_URL` | Airflow runtime worker endpoint; default `http://dbt-worker:8080` |
| `GOLD_TARGET_REF` | Default `main`; only `main` or `dbt_gold_test_*` accepted |
| `DBT_ICEBERG_CLASSPATH` | Optional override for the tested metadata helper library set |

The overlay currently pins internal Trino port 8080. A nondefault port requires
consistent Trino listener, Compose environment and dbt profile configuration;
changing the worker variable alone does not reconfigure Trino. The same applies
to region and storage endpoint settings across services. Threads remain 1;
SQL models, memory limits, JVM and writer settings are unchanged by this review.

## Readiness and manual UI run

Readiness performs SELECT 1, HTTP reads and bucket HEAD, not business writes:

```powershell
docker exec newdirection-dbt-worker-1 python /runtime/preflight.py
docker exec newdirection-dbt-worker-1 python -c "import urllib.request; print(urllib.request.urlopen('http://127.0.0.1:8080/health', timeout=3).read().decode())"
```

Worker `/health` confirms its HTTP listener; it is not evidence of a completed
dbt build. Check Airflow DAG imports and its 10-task graph before triggering.
On the portal, configure and save the source profile, verify API authentication,
then run sync. A static mock API key is accepted as the Bearer token value;
do not enter the assignment prefix or an additional `Bearer ` prefix in the
token field. Never include credentials in screenshots or diagnostic reports.

`NO_CHANGE` at ingestion intentionally exits 99 and skips downstream tasks.
For a new end-to-end demo test, use approved new source records with valid
keys and timestamps above the watermark. Do not reset checkpoints or mark
skipped tasks successful to force Gold processing. Portal success switches to
the report tab; chart data and Superset cache should be checked separately.

## Failure and retry boundaries

1. Inspect the failed task and `.runtime/dbt-integration/jobs/<job>/status.json`,
   `build.log`, `target/run_results.json` and dbt logs. Runtime state is stored
   in Ozone under `orchestration/dbt_gold/<job>.json`. Keep these for diagnosis.
2. Retrying with the same run ID reuses deterministic job/ref/state. A completed
   sealed build can be reused; published lifecycle retry does not republish.
   Successful commit followed by failed state save can recover only if the
   complete output bundle already points to the validated metadata.
3. A failed worker job is not resubmitted by retrying `process_gold`. Worker
   restart marks interrupted queued/running jobs failed. This implementation
   does not provide automatic recovery/resume for interrupted builds.
4. **A new DAG run is not sufficient recovery by itself.** The ingestion
   watermark advances after Bronze succeeds, before downstream Gold completes.
   With no newer API records, a new run can skip ingestion and all downstream
   tasks. Retain the committed batch and state; assess which stage can safely
   resume on the original run. A failed worker build or changed candidate needs
   a separately reviewed replay/recovery procedure, not checkpoint deletion,
   artificial source updates or success-state overrides. No generic replay or
   `gold_only` mode is implemented here.
5. Candidate/source/artifact changes and target CAS conflicts fail closed.
   Do not edit status/state files, bypass tests or merge a whole candidate
   branch to force publication. Resolve the conflict and plan a controlled
   rebuild against the intended snapshot.

## Retention, cleanup and handover

Worker drops its own temporary Trino catalog after a build; cleanup failure
makes the job fail. Normal DAG runs retain candidate Nessie branches, Ozone
metadata/data and job evidence. No automatic retention/GC policy is implemented.
Do not delete active-run artifacts or expire Iceberg snapshots without checking
published pointer reachability. Dropping a branch is not physical object cleanup.

Include integration source, dependency lock, Compose overlay, tests and docs in
the Feature review. Exclude `.runtime`, `.env`, credentials, generated targets,
JFR/benchmark archives and preserved Research assets. Step 4/5 relative links to
local runtime evidence will not resolve in a fresh Git checkout unless evidence
is separately supplied. The demo source JSON additions are intentional test
data and should be explicitly reviewed as part of the change set.

Functional tests do not establish HA, multi-worker job coordination, recovery
from every interruption or production load readiness. The private worker has
no host port or application authentication; retain its private network scope.
