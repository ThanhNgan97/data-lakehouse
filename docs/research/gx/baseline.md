# Great Expectations Phase 0 Baseline

## Purpose

This baseline records the current data-quality behavior before Great Expectations is added. It is the comparison point for the research branch and must not be interpreted as a GX result.

## Research scope

- Branch: `research/great-expectations-poc`
- Base commit: `318677a239c8298c9ecaa4bd99a71e3324dd26d8`
- Base relationship: local `develop` and `origin/develop` pointed to the same commit when Phase 0 started.
- Initial dataset: `education.teaching_progress`
- Initial layer: Silver
- Orchestrator: `api_dataset_pipeline`
- Execution engine: Spark DataFrame
- Out of scope: installing GX, changing a pipeline, adding Bronze or Gold GX suites, dbt, OpenMetadata, frontend, and backend changes.

## Current quality architecture

The current API pipeline has separate orchestration commands for Bronze, Silver, and Gold validation, but processing and publication are not fully separated.

```text
ingest_bronze
-> validate_bronze
-> process_silver
-> validate_silver
-> process_gold
-> validate_gold
-> trino_smoke_test
```

### Bronze validation

The current Bronze command checks:

- The latest committed batch exists and contains at least one row.
- All technical metadata columns exist.
- `_dataset` contains only the configured dataset identity.
- `_batch_id` contains only the expected batch identity.
- `_record_checksum` contains no null values.

### Teaching Progress Silver row classification

The current Silver transformation uses seven demo rules:

1. `MISSING_RECORD_ID`
2. `MISSING_UNIT_CODE`
3. `MISSING_COURSE_SECTION_CODE`
4. `MISSING_ACADEMIC_YEAR`
5. `INVALID_SEMESTER`
6. `INVALID_TEACHING_PROGRESS_PERCENT`
7. `MISSING_SOURCE_UPDATED_AT`

Rows that violate these rules are routed through the generic quarantine mechanism. The processor also performs deterministic deduplication and conflict classification before merging records.

### Silver validation command

The separate `validate-silver` command currently reads the Silver and quarantine tables from Nessie `main` and checks:

- Silver row count is greater than zero.
- No duplicate business-key groups exist.
- Row count, distinct key count, duplicate group count, and quarantine count are printed to the Airflow log.

Important baseline finding: `process-silver` calls the dataset processor with `merge_to_main=True`. Therefore, the later `validate-silver` task validates data after it has already been merged into `main`. It is an audit check, not a pre-publication quality gate.

### Teaching Progress Gold validation

The Gold quality contract checks:

- Gold row count matches the expected Silver row count.
- Distinct Gold keys match the expected row count.
- No duplicate Gold keys exist.
- Business keys are not null.
- Teaching progress is present, finite, and between 0 and 100.
- Monitor count is exactly one per Gold row.
- Lineage fields are not null.
- Gold lineage matches its Silver source record.

Important baseline finding: `process-gold` also runs with `merge_to_main=True`, while `validate-gold` runs afterward. Gold already contains an internal quality check before merge, but the separate Airflow validation task remains post-publication.

## Baseline test inventory

Existing automated coverage includes:

- Generic ordered DQ reasons and valid/invalid row splitting.
- Teaching Progress source-to-canonical mapping.
- Required Bronze metadata fields.
- Valid and invalid Teaching Progress row routing.
- Reuse of generic quality, quarantine, deduplication, merge, and Nessie helpers.
- Gold row preservation, grain, percentage validation, and lineage reconciliation.
- API dataset registry and orchestration routing.
- Eight-task Airflow DAG structure and dependency order.

## Phase 0 execution results

Date: 2026-10-15, Asia/Bangkok.

### Repository state

| Check | Result |
|---|---|
| Working tree before Phase 0 | Clean |
| Active branch | `research/great-expectations-poc` |
| Research base equals local `develop` | Yes |
| Research base equals `origin/develop` | Yes |
| Ahead/behind between local and remote develop | `0/0` |

### Host test attempt 1

Command scope: six relevant test modules, without an explicit Spark module path.

Result:

- Pytest exit code: `2`
- Collection completed in approximately `10.77` seconds.
- Total measured command time: approximately `25.958` seconds.
- Cause: repository Spark modules were not importable because `lakehouse/spark` was not on `PYTHONPATH`; Airflow was also unavailable on the host.

### Host test attempt 2

Command scope: five non-DAG modules with `lakehouse/spark` added to `PYTHONPATH`.

Result:

- Pytest exit code: `2`
- Collection completed in approximately `11.62` seconds.
- Total measured command time: approximately `22.03` seconds.
- Cause: host Python did not provide `psycopg2`, which is required indirectly by Gold and orchestration modules.

### Host Spark-only attempt

Command scope:

- `test_generic_silver_quality.py`
- `test_teaching_progress_silver_foundation.py`
- `test_teaching_progress_silver_orchestration.py`

Result:

- Spark/JVM initialization did not produce a test result within approximately 90 seconds.
- The run was interrupted to avoid treating an unbounded host-environment wait as a valid benchmark.
- No pass or failure conclusion was recorded.

### Authoritative container runtime

The existing Airflow image was built without changing its dependency set. Runtime versions were:

- Airflow `2.9.2`
- PySpark `3.5.0`
- OpenJDK `17.0.20.1`
- `psycopg2` available

The image does not include `pytest`, so the repository's `unittest`-based tests were executed with Python's standard test runner. This preserved the current image as the baseline instead of installing a test dependency during measurement.

### Core quality and orchestration baseline

The following modules were executed in the Airflow image:

- `test_generic_silver_quality`
- `test_teaching_progress_silver_foundation`
- `test_teaching_progress_silver_orchestration`
- `test_teaching_progress_gold`
- `test_api_dataset_orchestration`

Result:

- Tests run: `38`
- Passed: `38`
- Failed: `0`
- Test-runner time: `25.247` seconds
- Host-observed container wall time: `29.014` seconds

The output included Spark warnings about the unavailable native Hadoop library, a missing `ps` utility in the image, and unclosed test socket resource warnings. None caused a test failure, but they should remain visible during later GX comparison runs.

### Airflow DAG baseline

The DAG test requires an initialized Airflow metadata database. A disposable SQLite metadata database was migrated inside the temporary container before running `test_api_dataset_pipeline_dag`.

Result:

- Tests run: `14`
- Passed: `14`
- Failed: `0`
- Test-runner time: `0.490` seconds
- Host-observed container wall time including DB initialization: `9.938` seconds
- DAG import errors: `0`

Airflow emitted deprecation warnings for `schedule_interval`; these warnings predate GX and are not part of the GX research scope.

### Silver test baseline measurement

After the earlier warm-up run, the three Silver-focused modules were executed five times in the same Airflow container:

| Measured run | Result | Duration |
|---:|---|---:|
| 1 | Pass | 19.747 s |
| 2 | Pass | 20.926 s |
| 3 | Pass | 17.389 s |
| 4 | Pass | 17.253 s |
| 5 | Pass | 17.228 s |

- Median: `17.389` seconds
- Minimum: `17.228` seconds
- Maximum: `20.926` seconds
- Successful measured runs: `5/5`

This measures the current Silver unit/contract test bundle, including Spark startup and test actions. It is the repeatable pre-GX test baseline, not a production-volume pipeline benchmark.

## Baseline limitations

- No production-size row-volume timing is available in Phase 0 because the stack services and representative benchmark datasets were not started.
- Host Python is not the authoritative Airflow runtime and is missing required dependencies; only the container results above are accepted as the functional baseline.
- The `17.389` second median includes Spark startup and must be compared with the same test bundle after GX is added.
- Row-volume benchmarking at 1,000 through 1,000,000 rows remains part of the later GX performance phase.

## Measurements reserved for the GX comparison phase

When the GX implementation exists, run both the pre-GX commit and GX commit against the same generated datasets and record:

1. Median validation time at each row-volume tier.
2. Silver row counts and quarantine counts.
3. Nessie `main` hash before and after valid and invalid cases.
4. Container peak memory and approximate CPU utilization.
5. Validation-result storage growth.

## Phase 0 conclusions

1. The repository already has substantial row-level quarantine and dataset-specific quality logic; GX should standardize validation evidence rather than replace transformation and quarantine mechanics wholesale.
2. Silver is the correct first GX integration point.
3. The current separate Silver validation task is post-merge, so a later phase must move the GX gate before the Nessie merge.
4. Teaching Progress is suitable for the first comparison because it has explicit DQ rules, Gold reconciliation, fixtures, and automated tests.
5. The current baseline is green: `52/52` selected tests passed in the authoritative container runtime.
6. The repeatable Silver-focused baseline median is `17.389` seconds; production-volume conclusions remain intentionally deferred until an equivalent GX implementation exists.

