# DAY 7 — TEACHING GOLD + SUPERSET

Overall Status: PASS

Starting Commit: `13fa87e4e81b27c60063d765faed0a2224126116`

Day 7 Code Commit: `2fea1b7228d63de781a219e7124b23dc1598ac93`

Source Status: `MOCK / NOT VERIFIED CTU IOC PRODUCTION CONTRACT`

## 1. Executive Result

Day 7 completed the business-serving path for the second dataset,
`education.teaching_progress`, without changing the platform architecture or
copying Learning Outcomes business formulas.

The final verified path is:

```text
Teaching Mock API
    -> generic HTTP/API ingestion
    -> source-native Bronze
    -> Teaching-specific canonical Silver
    -> Teaching-specific Gold
    -> Iceberg / Nessie
    -> Trino
    -> Superset dataset
    -> separate Teaching Progress dashboard
```

The implementation demonstrates two datasets using one Lakehouse platform while
preserving separate business models.

Final Teaching reconciliation:

```text
Silver rows                = 30
Silver distinct keys       = 30
Silver quarantine rows     = 0
Gold rows                  = 30
Gold distinct keys         = 30
Gold duplicate keys        = 0
SUM(so_ban_ghi_theo_doi)   = 30
invalid Gold progress      = 0
semantic mismatch          = 0
lineage mismatch           = 0
```

Learning Outcomes regression remained:

```text
Silver rows       = 30
active rows       = 30
deleted rows      = 0
quarantine rows   = 0
Gold rows         = 30
```

## 2. Starting Git State

Day 7 began from:

```text
branch = feature/new-direction
HEAD   = 13fa87e4e81b27c60063d765faed0a2224126116
```

Tracked worktree was clean before implementation.

Day 7 code closed at:

```text
2fea1b7228d63de781a219e7124b23dc1598ac93
feat: add Teaching Progress Gold business mart
```

## 3. Day 6 Baseline Verification

The Day 6 Teaching baseline was reproduced before Gold design:

```text
Source rows             = 30
Bronze baseline rows    = 30
Silver rows             = 30
Silver distinct keys    = 30
duplicate key groups    = 0
quarantine rows         = 0
invalid progress rows   = 0
```

The existing Learning Silver and Gold states were also revalidated before
continuing.

## 4. Teaching Silver Contract

Teaching source/Silver remains a demo contract and is not asserted to be the CTU
IOC production contract.

Canonical business key:

```text
(ma_lop_hoc_phan, nam_hoc, hoc_ky)
```

Canonical business fields used by Gold:

```text
ma_ban_ghi
ma_don_vi
ten_don_vi
ma_lop_hoc_phan
nam_hoc
hoc_ky
ty_le_tien_do_giang_day
thoi_gian_cap_nhat_nguon
```

Relevant technical lineage:

```text
_batch_id
_record_checksum
```

Delete support remains:

```text
DELETE_SUPPORTED = NO
```

## 5. Business Questions

Supported:

1. What source-reported teaching-progress percentage belongs to each monitored
   course section for a selected academic year and semester?
2. Which monitored course sections have the highest or lowest reported progress
   percentage within selected filters?
3. How many course-section-period Gold-grain records are being monitored?
4. Which organizational unit belongs to each course section?
5. Can every Gold row be traced back to canonical Silver/source evidence?

Explicitly unsupported / not verified:

```text
on-schedule / behind-schedule status
late / on-time status
warning / risk classification
completion / remaining counts
weighted progress
expected-vs-actual variance
Teaching trend KPI
Learning Outcomes KPI formulas
```

These outputs would require source facts or business policy that the current
contract does not provide.

## 6. Gold Grain

Teaching Gold grain is:

```text
one monitored course section
x one academic year
x one semester
```

Business key:

```text
(ma_lop_hoc_phan, nam_hoc, hoc_ky)
```

Gold deliberately remains at the canonical Silver business grain. No unit-level
or period-level aggregation policy was invented.

## 7. Gold Metrics

### Source-provided metric

`ty_le_tien_do_giang_day`

```text
formula = direct carry-forward from canonical Silver
valid range = 0..100
unit = percent
status = DEMO / source-provided / not production verified
```

Gold does not claim that this value was calculated from planned sessions,
completed sessions, attendance, workload, or another unverified raw fact.

### Gold-derived metric

`so_ban_ghi_theo_doi`

```text
formula = literal 1 per valid Gold-grain row
type = long
unit = record
meaning = count of monitored Teaching Gold-grain records
```

It must not be described as number of students, sessions, completed classes, or
workload.

## 8. Gold Transformation

Added module:

```text
lakehouse/spark/spark_teaching_progress_to_gold.py
```

Transformation behavior:

```text
canonical Teaching Silver
    -> row-preserving select
    -> carry verified dimensions
    -> carry source progress percentage
    -> derive so_ban_ghi_theo_doi = 1
    -> carry lineage
    -> write Teaching Gold
```

Target:

```text
lakehouse.gold.teaching_progress_metrics
```

Write strategy:

```text
FULL_RECOMPUTE
```

No Generic Gold framework was introduced.

## 9. Gold DQ

Teaching-specific Gold gates verify:

```text
business key not null
business key unique
Gold rows reconcile with Teaching Silver
ty_le_tien_do_giang_day in 0..100
so_ban_ghi_theo_doi = 1
no NaN in percentage
lineage fields non-null
Gold-to-Silver lineage mismatch = 0
```

Final result:

```text
duplicate Gold keys      = 0
invalid progress         = 0
invalid monitor count    = 0
null lineage             = 0
lineage mismatch         = 0
```

## 10. Iceberg / Nessie

The existing safe publish pattern was reused:

```text
main
 -> temporary Nessie branch
 -> write Teaching Gold
 -> Gold quality gate
 -> branch read-back
 -> merge validated branch to main
 -> main read-back
```

Protected Nessie helpers were not modified.

The first isolated branch validation passed before any Teaching Gold was merged
to main.

## 11. Gold Runtime Reconciliation

Validated relationship:

```text
ACTIVE_SILVER_ROWS = 30
TRANSFORMED_ROWS   = 30
GOLD_ROWS          = 30
DISTINCT_GOLD_KEYS = 30
```

Final main state:

```text
Gold rows                  = 30
Gold distinct keys         = 30
Gold duplicate keys        = 0
SUM(so_ban_ghi_theo_doi)   = 30
invalid progress            = 0
semantic mismatch           = 0
lineage mismatch            = 0
```

Two consecutive full-recompute reruns also preserved the same stable business
content, lineage content, row count, and key count.

## 12. Business Value Reconciliation

Teaching Gold values were reconciled directly against canonical Teaching Silver.

Verified:

```text
dimension mismatch = 0
progress-value mismatch = 0
lineage mismatch = 0
unexplained Gold business value = 0
```

The Gold percentage is a direct source/Silver carry-forward. The only derived
metric is the literal monitored-record count.

## 13. Trend Decision

```text
TEACHING_TREND = NOT IMPLEMENTED
```

Multiple periods exist in the demo data, but the current contract does not prove
that the percentages represent business-comparable points in a Teaching
progress timeline. Learning's LAG/trend logic was therefore not copied.

## 14. Trino

Teaching Gold is readable through the existing Trino catalog.

Final verified serving counts:

```text
Learning Silver       = 30
Learning Gold         = 30
Teaching Silver       = 30
Teaching Gold         = 30
Teaching quarantine   = 0
Learning quarantine   = 0
```

Teaching Gold schema exposed through Trino matches the Gold table.

## 15. Superset Dataset

A separate Superset physical dataset was created/reused for:

```text
schema = gold
table  = teaching_progress_metrics
```

Final runtime dataset IDs:

```text
Learning dataset = 1
Teaching dataset = 2
```

No dataset collision was observed.

## 16. Dashboard

Separate dashboard:

```text
title = CTU IOC - Tiến độ giảng dạy
slug  = ctu-ioc-teaching-progress
dashboard id = 2
```

Two Teaching charts:

```text
chart 3 = Tổng quan tiến độ giảng dạy
chart 4 = Tiến độ theo lớp học phần
```

The existing Learning dashboard remained separate and unchanged.

A first Superset harness attempt failed while inserting a new `Slice` because the
test harness flushed the object before setting `datasource_type`. The recovery
was state-aware and non-destructive: the existing Teaching dataset was reused,
chart datasource fields were assigned before flush, and the full serving
validation then passed.

## 17. Dashboard Filters

Teaching dashboard contains four independent native filters:

```text
nam_hoc
hoc_ky
ten_don_vi
ma_lop_hoc_phan
```

Verified behavior:

```text
clear filters       -> 30 rows
single year filter  -> 15 rows
combined filters    -> 1 Gold-grain row
no-result filter    -> 0 rows
```

No Learning filter cascade policy was copied.

## 18. Dashboard Reconciliation

Superset-to-Trino reconciliation:

```text
Superset total rows       = 30
Trino total rows          = 30
Superset monitor sum      = 30
Trino monitor sum         = 30
representative mismatches = 0
business mismatch         = 0
```

Superset presentation contains no unsupported Teaching business formula.

```text
SUPERSET_BUSINESS_CALCULATIONS = GOLD_ONLY
```

## 19. Learning Regression

Final Learning state remained:

```text
Silver rows       = 30
active rows       = 30
deleted rows      = 0
quarantine rows   = 0
Gold rows         = 30
```

Frozen implementation guards:

```text
Learning Gold diff = 0
Teaching Silver diff = 0
Generic Silver diff = 0
Protected diff = 0
```

Learning Superset metadata snapshot hash was identical before and after Teaching
dashboard creation.

Learning chart IDs remained:

```text
1, 2
```

## 20. Multi-Dataset Serving

Final serving model:

```text
Learning Outcomes
Silver -> Learning Gold -> Trino -> Learning Superset dataset/dashboard

Teaching Progress
Silver -> Teaching Gold -> Trino -> Teaching Superset dataset/dashboard
```

Shared platform:

```text
Spark
Iceberg
Nessie
Trino
Superset
```

Separate business semantics:

```text
Learning KPI model != Teaching business model
```

Runtime collision checks:

```text
target serving datasets   = 2
target serving dashboards = 2
chart ID collisions       = 0
```

## 21. Platform vs Business Boundary

Reused platform mechanisms:

```text
Spark session
Iceberg
Nessie branch publishing
quality-gate pattern
table writer pattern
Trino serving
Superset database connection
```

Dataset-specific Teaching business logic remains local to Teaching Gold.

Not introduced:

```text
Generic Gold framework
Learning KPI reuse
Teaching trend formula
schedule-status formula
weighted-progress formula
new database technology
new query engine
```

## 22. Automated Tests

Day 7:

```text
Teaching Gold contract tests = 11
exit = 0
```

Frozen regression expectations also passed:

```text
Generic Silver tests          = 42
Learning behavior tests       = 18
Teaching Silver tests         = 13
```

Final compile, Spark E2E, Trino and Superset verification all passed.

Note: `LINEAGE_MISMATCH=1` printed during the unit-test suite is produced by the
intentional negative test that injects a bad checksum and verifies that the Gold
quality gate rejects it. The complete test suite exits `0`. Final runtime
lineage mismatch is `0`.

## 23. Files Added

Day 7 code commit added:

```text
lakehouse/spark/spark_teaching_progress_to_gold.py
lakehouse/spark/tests/test_teaching_progress_gold.py
```

## 24. Files Modified

Application files modified from the Day 6 checkpoint:

```text
NONE
```

Superset runtime metadata was extended with a separate Teaching dataset,
dashboard, charts, and filters. Existing Learning metadata was verified
unchanged.

## 25. Day 7 Acceptance Checklist

- [x] Starting checkpoint verified.
- [x] Day 6 Teaching Silver baseline reproduced.
- [x] Gold business questions explicitly defined.
- [x] Gold grain explicitly defined.
- [x] Every Gold metric has source/formula evidence.
- [x] No fabricated raw fact introduced.
- [x] No Learning KPI copied into Teaching Gold.
- [x] Teaching Gold table created.
- [x] Teaching-specific Gold DQ passed.
- [x] Iceberg/Nessie branch publishing passed.
- [x] Main Gold read-back passed.
- [x] Business reconciliation mismatch = 0.
- [x] Trino Teaching Gold query passed.
- [x] Superset Teaching dataset created.
- [x] Separate Teaching dashboard created.
- [x] Superset does not recalculate unsupported Gold business metrics.
- [x] Dashboard reconciliation mismatch = 0.
- [x] Learning pipeline regression passed.
- [x] Learning dashboard metadata unchanged.
- [x] Both datasets coexist through serving.
- [x] No target table/dataset/dashboard/chart collision.
- [x] Protected files changed = 0.
- [x] No Generic Gold framework introduced.
- [x] Final staged diff reviewed and code commit completed.

## 26. Risks / Remaining Unknowns

1. `education.teaching_progress` is still a mock/demo contract and has not been
   verified against CTU IOC production semantics.
2. `ty_le_tien_do_giang_day` is source-provided. Its upstream calculation is not
   represented by verified raw facts in this project.
3. No verified policy exists for on-schedule/behind-schedule, expected progress,
   weighted progress, completion counts, or Teaching trend; these remain
   intentionally unimplemented.
4. Superset dashboard objects are runtime metadata rather than a new
   version-controlled dashboard-as-code framework. Day 7 deliberately avoided
   adding such a framework.
5. The Superset environment reports existing CSP and in-memory rate-limit
   warnings. They are environment-hardening concerns, not failures of the Day 7
   business path.
6. Nessie audit branches were intentionally left available by runtime checks
   where the existing helper pattern does so; no destructive cleanup was added.

Final recommendation:

```text
DAY 7 = PASS
READY FOR NEXT DAY
```
