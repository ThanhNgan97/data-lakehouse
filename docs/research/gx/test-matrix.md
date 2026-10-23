# Great Expectations Research Test Matrix

## Purpose

This matrix defines the expected behavior before GX implementation. The same cases must be executed against the current checks and GX so their results can be compared directly.

## Result fields

Every executed case must record:

- Dataset ID
- Test case ID
- Input row count
- Expected current-check result
- Actual current-check result
- Expected GX result
- Actual GX result
- Expected Airflow task state
- Actual Airflow task state
- Nessie `main` hash before and after
- Candidate branch name
- Silver row count
- Quarantine row count
- Validation duration
- Failure rule or expectation

## Functional matrix

| ID | Input condition | Expected current behavior | Expected future GX behavior | Expected publication behavior |
|---|---|---|---|---|
| TP-S-001 | Valid Teaching Progress rows | Pass | Pass | Merge candidate branch |
| TP-S-002 | Empty dataset | Fail validation | Critical failure | Do not merge |
| TP-S-003 | Missing required source column | Fail mapping or processing | Critical schema failure | Do not merge |
| TP-S-004 | Null or blank `ma_ban_ghi` | Quarantine invalid row | Critical completeness failure or quarantine policy | Publish only if policy threshold allows |
| TP-S-005 | Null or blank `ma_don_vi` | Quarantine invalid row | Critical completeness failure or quarantine policy | Publish only if policy threshold allows |
| TP-S-006 | Null or blank `ma_lop_hoc_phan` | Quarantine invalid row | Critical completeness failure or quarantine policy | Publish only if policy threshold allows |
| TP-S-007 | Missing academic year | Quarantine invalid row | Critical completeness failure or quarantine policy | Publish only if policy threshold allows |
| TP-S-008 | Semester is null, zero, or negative | Quarantine invalid row | Critical validity failure or quarantine policy | Publish only if policy threshold allows |
| TP-S-009 | Teaching progress is null | Quarantine invalid row | Critical completeness failure or quarantine policy | Publish only if policy threshold allows |
| TP-S-010 | Teaching progress below zero | Quarantine invalid row | Critical range failure or quarantine policy | Publish only if policy threshold allows |
| TP-S-011 | Teaching progress above 100 | Quarantine invalid row | Critical range failure or quarantine policy | Publish only if policy threshold allows |
| TP-S-012 | Missing source update timestamp | Quarantine invalid row | Critical completeness failure or quarantine policy | Publish only if policy threshold allows |
| TP-S-013 | Duplicate business key in Silver | Fail quality gate | Critical uniqueness failure | Do not merge |
| TP-S-014 | Equal key and timestamp with different content | Quarantine conflict | Critical conflict metric or quarantine policy | Publish only if threshold allows |
| TP-S-015 | Exact repeated source row | Deduplicate | Pass with deduplication metric | Merge once |
| TP-S-016 | Optional new column | Ignore/drop according to mapping contract | Pass if additive policy allows | Merge |
| TP-S-017 | Wrong dataset identity metadata | Fail Bronze validation | Critical identity failure | Do not process Silver |
| TP-S-018 | Null record checksum | Fail Bronze validation | Critical technical failure | Do not process Silver |
| TP-S-019 | GX runtime/store unavailable | Not applicable | Infrastructure failure | Fail closed; do not merge |
| TP-S-020 | Same valid batch retried | Idempotent result | Same validation outcome | No duplicate Silver rows |

## Gold baseline matrix

Gold is not part of the first GX PoC, but its current quality contract must remain green during regression testing.

| ID | Input condition | Expected current behavior |
|---|---|---|
| TP-G-001 | Valid transformed Gold rows | Pass |
| TP-G-002 | Gold row count differs from Silver | Fail |
| TP-G-003 | Duplicate Gold grain | Fail |
| TP-G-004 | Null Gold business key | Fail |
| TP-G-005 | Progress outside `[0,100]` | Fail |
| TP-G-006 | Monitor count differs from one | Fail |
| TP-G-007 | Null lineage field | Fail |
| TP-G-008 | Gold lineage differs from Silver | Fail |

## Quarantine threshold experiments

The current implementation quarantines invalid rows but does not define a universal batch-level rejection threshold. The research phase must test and choose a policy rather than assume one.

| Input size | Invalid rows | Ratio | Candidate policy result |
|---:|---:|---:|---|
| 1,000 | 0 | 0% | Pass |
| 1,000 | 1 | 0.1% | Pass with quarantine |
| 1,000 | 10 | 1.0% | Boundary case to decide |
| 1,000 | 11 | 1.1% | Compare against selected threshold |
| 1,000 | 50 | 5.0% | Expected critical failure in initial research policy |

The final threshold must be dataset policy, not a hard-coded constant in the GX runner.

## Performance matrix

Use one warm-up followed by five measured runs per size. Record the median.

| Rows | Current checks median | Shadow GX median | Enforced GX median | GX overhead | Peak memory |
|---:|---:|---:|---:|---:|---:|
| 1,000 | Pending | Pending | Pending | Pending | Pending |
| 10,000 | Pending | Pending | Pending | Pending | Pending |
| 100,000 | Pending | Pending | Pending | Pending | Pending |
| 1,000,000 | Pending if hardware permits | Pending | Pending | Pending | Pending |

## Research acceptance criteria

| Metric | Required result |
|---|---:|
| Critical defect detection | 100% |
| False negatives | 0 |
| False positives on the agreed valid fixtures | 0 |
| Invalid datasets merged to Nessie `main` | 0 |
| Valid pipeline success | 100% |
| Median overhead | At most 20%, unless an explicit conditional decision is documented |
| Validation traceability | Dataset ID, run ID, suite, failed rule, observed value, timestamp |
| Second dataset integration | No change to the core GX runner |

## Current execution status

The current automated baseline is green in the authoritative Airflow container:

- Core quality and orchestration: `38/38` tests passed.
- Airflow DAG structure and imports: `14/14` tests passed.
- Silver-focused repeatability measurement: `5/5` runs passed.
- Silver-focused median: `17.389` seconds.

The functional cases in this matrix that do not already have explicit automated coverage remain specifications for the GX research fixtures. Row-volume performance cells also remain pending until GX exists, because overhead requires equivalent before-and-after workloads rather than unrelated standalone measurements.

