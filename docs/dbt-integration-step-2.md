# dbt integration - Step 2: project source and offline compile

Status: **PASS**, offline parse/compile verified on 2026-10-26.

Scope: business project only. No dbt run/build/test, catalog/table creation,
Nessie branch, metadata commit, publication, DAG change or worker job API.
Step 1 runtime and running newdirection containers are not recreated.

## Source and configuration

31 files are byte-for-byte copies verified against the Candidate B P8 handover
manifest. This includes all models, macros, SQL tests, schema/sources YAML and
dbt_project.yml. profiles.yml is deliberately adapted for this integration:
target integration, user dbt_api_gold, default host trino, port 8080,
DBT_TARGET_CATALOG default lakehouse, DBT_TARGET_SCHEMA default gold, threads=1.
It contains no storage credential. DBT_PROBE_SCHEMA from research is not used.

Ozone is selected by the existing Trino catalog, not by model SQL. Future
isolated catalog selection must set DBT_TARGET_CATALOG consistently for both
source and target. Current catalog defaults do not authorize writing to main.

No packages.yml is required: project macros use the pinned dbt/adapter builtins.
The Python result gates, worker job API and Gold orchestration will be reviewed
in a later approved step; they are not included in this SQL-only step.

## Business behavior retained

Learning: active Silver rows -> three derived metrics -> previous AVAILABLE
period per program (not calendar-gap filling) -> Fact deltas -> period Mart.
Narrow Window input columns: ma_chuong_trinh, nam_bat_dau, hoc_ky, nam_hoc,
ty_le_qua_hoc_phan, gpa_trung_binh, ty_le_dung_tien_do. Five LAG expressions
share program partition and start-year/semester ordering. Restore JOIN uses
ma_chuong_trinh, nam_bat_dau, hoc_ky; grain tests are retained.

Pass/on-track percentages use numerator *100 / denominator; GPA uses total
points / eligible students. Nonpositive denominators yield NULL. Mart sums
numerators/denominators before division; it does not average program ratios.
Teaching carries source progress unchanged and sets so_ban_ghi_theo_doi=1.
Lineage and output aliases are retained. Staging/intermediate are ephemeral,
Fact/Mart are PARQUET tables. Fact pre/post hooks disable/reset extended
statistics on write. Writer Scaling settings are not part of this change.

## Files added (32 project files + this document = 33)

| Repository path | Purpose | Exact P8 copy |
|---|---|---|
| `lakehouse/dbt/api_gold/dbt_project.yml` | Materialization, PARQUET tables, Fact NDV disable/reset hooks | Yes |
| `lakehouse/dbt/api_gold/macros/assert_output_schema.sql` | SQL-generating macro (no run_query) | Yes |
| `lakehouse/dbt/api_gold/macros/batched_row_checks.sql` | SQL-generating macro (no run_query) | Yes |
| `lakehouse/dbt/api_gold/models/intermediate/int_learning_outcomes_metrics.sql` | Extract academic start year; calculate pass rate, GPA, on-track rate; denominator<=0 yields NULL. | Yes |
| `lakehouse/dbt/api_gold/models/intermediate/int_learning_outcomes_periods.sql` | Narrow Window: 7 input columns, 5 LAGs per program; JOIN BACK by program/start year/semester. | Yes |
| `lakehouse/dbt/api_gold/models/marts/agg_learning_outcomes_by_period.sql` | Aggregate numerator/denominator by year/semester; recompute weighted rates/GPA. | Yes |
| `lakehouse/dbt/api_gold/models/marts/fct_learning_outcomes.sql` | Select Fact columns; calculate current minus previous-period metrics; output lineage and timestamp. | Yes |
| `lakehouse/dbt/api_gold/models/marts/fct_teaching_progress.sql` | Preserve source progress; assign monitored-record count=1 and Gold timestamp. | Yes |
| `lakehouse/dbt/api_gold/models/schema.yml` | Model descriptions and Teaching not-null tests | Yes |
| `lakehouse/dbt/api_gold/models/sources.yml` | Silver source declarations; catalog from DBT_TARGET_CATALOG | Yes |
| `lakehouse/dbt/api_gold/models/staging/stg_learning_outcomes.sql` | Filter da_xoa=false; preserve grain and rename lineage. | Yes |
| `lakehouse/dbt/api_gold/models/staging/stg_teaching_progress.sql` | Read Teaching Silver and rename lineage. | Yes |
| `lakehouse/dbt/api_gold/profiles.yml` | New integration profile; Trino host/port/catalog/schema from environment, threads=1 | No: integration profile |
| `lakehouse/dbt/api_gold/tests/learning_outcomes_grain.sql` | Data quality SQL: learning_outcomes_grain | Yes |
| `lakehouse/dbt/api_gold/tests/learning_outcomes_lineage_part_0.sql` | Data quality SQL: learning_outcomes_lineage_part_0 | Yes |
| `lakehouse/dbt/api_gold/tests/learning_outcomes_lineage_part_1.sql` | Data quality SQL: learning_outcomes_lineage_part_1 | Yes |
| `lakehouse/dbt/api_gold/tests/learning_outcomes_lineage_part_2.sql` | Data quality SQL: learning_outcomes_lineage_part_2 | Yes |
| `lakehouse/dbt/api_gold/tests/learning_outcomes_lineage_part_3.sql` | Data quality SQL: learning_outcomes_lineage_part_3 | Yes |
| `lakehouse/dbt/api_gold/tests/learning_outcomes_periods.sql` | Data quality SQL: learning_outcomes_periods | Yes |
| `lakehouse/dbt/api_gold/tests/learning_outcomes_reconciliation.sql` | Data quality SQL: learning_outcomes_reconciliation | Yes |
| `lakehouse/dbt/api_gold/tests/learning_outcomes_row_checks.sql` | Data quality SQL: learning_outcomes_row_checks | Yes |
| `lakehouse/dbt/api_gold/tests/learning_outcomes_schema.sql` | Data quality SQL: learning_outcomes_schema | Yes |
| `lakehouse/dbt/api_gold/tests/learning_period_grain.sql` | Data quality SQL: learning_period_grain | Yes |
| `lakehouse/dbt/api_gold/tests/learning_period_reconciliation.sql` | Data quality SQL: learning_period_reconciliation | Yes |
| `lakehouse/dbt/api_gold/tests/learning_period_row_checks.sql` | Data quality SQL: learning_period_row_checks | Yes |
| `lakehouse/dbt/api_gold/tests/learning_period_schema.sql` | Data quality SQL: learning_period_schema | Yes |
| `lakehouse/dbt/api_gold/tests/teaching_progress_grain.sql` | Data quality SQL: teaching_progress_grain | Yes |
| `lakehouse/dbt/api_gold/tests/teaching_progress_lineage.sql` | Data quality SQL: teaching_progress_lineage | Yes |
| `lakehouse/dbt/api_gold/tests/teaching_progress_monitor_count.sql` | Data quality SQL: teaching_progress_monitor_count | Yes |
| `lakehouse/dbt/api_gold/tests/teaching_progress_range.sql` | Data quality SQL: teaching_progress_range | Yes |
| `lakehouse/dbt/api_gold/tests/teaching_progress_reconciliation.sql` | Data quality SQL: teaching_progress_reconciliation | Yes |
| `lakehouse/dbt/api_gold/tests/teaching_progress_schema.sql` | Data quality SQL: teaching_progress_schema | Yes |
| `docs/dbt-integration-step-2.md` | File inventory, formulas, execution boundaries and results | New document |

## Offline verification method

Use the Step 1 image in a one-off container with --network none, a read-only
/project bind mount, and a writable /artifacts bind mount below .runtime.
No Ozone credentials or Docker socket are passed. Even an accidental adapter
connection cannot reach Trino. Inspect project macros for run_query/statement;
all supplied macros only generate SQL. Compile uses --no-introspect and
--no-populate-cache. Parsing/compiling hooks records SQL but does not execute it.
This isolation is specific to this project and command; dbt compile in general
may query metadata or execute run_query macros when permitted.

The equivalent commands INSIDE that isolated container are:

```text
dbt parse --project-dir /project --profiles-dir /project --no-partial-parse --target-path /artifacts/parse --log-path /artifacts/parse-logs
dbt compile --project-dir /project --profiles-dir /project --no-partial-parse --no-introspect --no-populate-cache --target-path /artifacts/compile --log-path /artifacts/compile-logs
```

## Results

| Check | Result |
|---|---|
| P8 preservation | 31 exact file copies; source SHA256 verified before/after compile |
| Integration changes | profiles.yml only; no business formula/model/test changes |
| dbt parse | Exit 0, network disabled |
| dbt compile | Exit 0, network disabled, introspection and cache population disabled |
| Models / Silver sources | 7 / 2 |
| Compiled test nodes | 28 = Learning Outcomes 13 + Teaching Progress 15 |
| Test definitions | 19 singular SQL files + 9 generic Teaching not-null tests |
| Custom macro files | 2 (the larger dbt macro count also includes adapter/core builtins) |
| Lineage | All four Learning parts 0..3 present |
| Compiled Learning Fact | No unresolved Jinja; staging/intermediate CTEs inlined; five LAG expressions retained |
| Step 1 | All seven source/config/document fingerprints unchanged |

Evidence and per-file source/hash inventory:
[report](../.runtime/dbt-integration/step2/4cc227c3a9/report.json).
The same folder contains both manifests, command logs and compiled SQL.
[Compiled Learning Fact](../.runtime/dbt-integration/step2/4cc227c3a9/compile/compiled/api_gold/models/marts/fct_learning_outcomes.sql).
No target or logs were generated inside the source project.

Three ignored __pycache__ files predated this step. They were not copied from
the handover, are not project sources, and were neither modified nor deleted.
The adapter's optional keyring notice does not block this method=none profile;
OAuth was not enabled or tested.

Parse/compile success does not establish Trino SQL execution/type correctness,
data correctness, performance or publication safety. Data tests are compiled,
not run. Compile run_results.json is not a build/test receipt and must not be
used to authorize validation/publication. Business validation and sandbox
execution belong to a later approved step.
