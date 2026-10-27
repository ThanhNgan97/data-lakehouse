# dbt integration - Step 5: Portal acceptance record

Date: 2026-10-26.

**PASS: both portal-triggered DAG runs completed all 10 tasks. User reported successful UI testing.**

## Scope

Portal API Source Profile -> backend sync -> Airflow api_dataset_pipeline ->
API -> Bronze -> Silver -> dbt/Trino build and tests -> runtime validation ->
Gold publication -> Trino smoke test -> portal Superset report.
Runtime: newdirection Compose project, Apache Ozone storage.
This documentation step only read existing Airflow metadata and worker receipts.
No DAG trigger, data insertion, resource change or workload rerun was performed.

## Verified portal runs

Times below are UTC on 2026-10-26, read from Airflow metadata.

| Dataset | Run ID | Start | End | Result |
|---|---|---|---|---|
| Learning Outcomes | `portal_api__learning_outcomes__c9c9403444ca` | 13:40:36.964828 | 13:43:25.086302 | success, 10/10 tasks |
| Teaching Progress | `portal_api__teaching_progress__f39af5acda59` | 13:45:22.948017 | 13:47:28.990189 | success, 10/10 tasks |

Both runs have success for api_preflight, ingest_bronze, validate_bronze,
process_silver, validate_silver, prepare_gold, process_gold, validate_gold,
publish_gold and trino_smoke_test. No task was skipped. This matters because
an earlier no-change DAG run also had overall success while downstream tasks
were skipped; overall DAG status alone is insufficient evidence.

## Worker receipts

| Dataset | Job | dbt result |
|---|---|---|
| Learning Outcomes | `c126f88933ae5993147d3df1` | passed; 2 models, 13 tests |
| Teaching Progress | `97915d789a8bf7b5bc2c65bc` | passed; 1 model, 15 tests |

Original receipts match the portal run IDs above:

- [Learning status](../.runtime/dbt-integration/jobs/c126f88933ae5993147d3df1/status.json)
- [Teaching status](../.runtime/dbt-integration/jobs/97915d789a8bf7b5bc2c65bc/status.json)

No formulas or business tests were removed for this UI exercise. Runtime
validation and publication completed according to task states. This step did
not repeat an independent full-output comparison; bounded sandbox parity is
recorded in [Step 4](dbt-integration-step-4.md).

## Source records prepared for the UI exercise

Two new records per dataset were added before the user triggered these runs.
Source totals became 34 records per dataset. A read-only incremental fetch
returned exactly two new records per dataset before the runs; checkpoints
were not reset.

| Dataset | New identifiers | Period and expected values |
|---|---|---|
| Learning | DEMO-P014, DEMO-P015 | 2025-2026 semester 2; GPA 3.10 and 3.30 |
| Teaching | DEMO-TP-U01-C05, DEMO-TP-U02-C05 | 2025-2026 semester 1; progress 88% and 92% |

[UI seed manifest](../.runtime/dbt-integration/ui-seed-round2/report.json)
contains complete record IDs and timestamps. Source files changed for this
exercise were ctu_ioc_mock_api/app/data/learning_outcomes.json and
ctu_ioc_mock_api/app/data/teaching_progress.json. Demo records remain available;
no automatic deletion was performed after publication.

## UI/dashboard evidence boundary

The user explicitly reported successful UI testing. Existing code in
frontend/src/pages/ApiConnection.jsx polls the run state, increments dashboard
revision and switches to the report tab on success; the iframe uses the saved
profile dashboard slug. Backend sync is in backend/api/routes/api_sources.py.
Neither file was modified for this exercise.

UI success is recorded as user-confirmed. No dashboard screenshot, chart query
or per-chart result was independently captured in this step. Therefore this
record does not claim independent verification of every new row/value on every
chart, or complete Superset cache invalidation. For chart-specific acceptance,
check the periods/identifiers above and retain corresponding screenshots.

## Conclusion and limits

Both datasets completed the portal-triggered DAG, dbt builds/tests, runtime
validation, Gold publication and Trino smoke test on local Ozone. This is
functional integration acceptance on demo data. It is not a PySpark performance
comparison, a 1.8-million-row load test or production readiness certification.
No commit, push or deployment to another environment was performed.
