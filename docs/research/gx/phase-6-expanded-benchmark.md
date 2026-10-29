# Phase 6: expanded benchmark

Date: 2026-10-27  
Dataset: `education.teaching_progress`  
Runtime image: `lakehouse-gx-validator:1.24.0`

## Benchmark contract

The benchmark harness records the same schema for `off`, `report`, and
`enforce`:

- input rows and Iceberg data-file bytes;
- injected and detected violations, detection rate, and false negatives;
- critical rows published and whether a candidate branch was merged;
- GX Silver and GX Gold durations;
- candidate-branch query duration;
- Trino scan duration and bytes scanned from driver query statistics;
- process CPU seconds and peak RSS;
- Spark executor seconds;
- concurrent validation count;
- timeout and retry counts;
- total duration.

`off` is normalized by the harness: detection rate and false negatives are
`N/A`, GX durations are `0`, and Trino validation scan/bytes are `0`. This
prevents a skipped validator from being reported as a zero-percent detector.

The repeatable runner performs one warm-up followed by five measured runs and
reports medians. Raw per-run samples remain available unless `--summary-only`
is selected.

## Local pilot result

This bounded run was read-only: it queried Nessie `main`, did not create or
merge a branch, and did not inject violations into persistent tables. Silver
and Gold each contained 30 rows. Their Iceberg data files occupied 8,398 and
6,171 bytes respectively (14,569 bytes for the measured workload).

| Metric (median of 5) | Off | Report | Enforce |
|---|---:|---:|---:|
| Input rows (Silver / Gold) | 30 / 30 | 30 / 30 | 30 / 30 |
| Injected violations | 0 | 0 | 0 |
| GX detection rate | N/A | N/A | N/A |
| False negatives | N/A | 0 | 0 |
| Critical rows published | 0 | 0 | 0 |
| GX Silver duration (s) | 0 | 1.108 | 1.036 |
| GX Gold + reconciliation duration (s) | 0 | 1.944 | 1.942 |
| Candidate identity query (s) | 0.025 | 0.029 | 0.027 |
| Trino scan duration (s) | 0 | 0.330 | 0.315 |
| Trino bytes scanned | 0 | 68,302 | 68,302 |
| Process CPU (s) | 0.006 | 0.348 | 0.381 |
| Peak RSS | 49,020,928 B | 49,020,928 B | 49,020,928 B |
| Spark executor seconds | 0 | 0 | 0 |
| Concurrent validations | 1 | 1 | 1 |
| Timeout / retry | 0 / 0 | 0 / 0 | 0 / 0 |
| Total duration (s) | 0.025 | 2.980 | 3.015 |
| Branch merged | No | No | No |

All three executed validation summaries in both `report` and `enforce` were
`PASSED`: Silver `6/6`, Gold `5/5`, and reconciliation `3/3`.

## Interpretation and limits

- `report` and `enforce` execute the same technical expectations, so their
  close medians are expected; their difference is the policy decision after
  execution.
- The reported Trino bytes come from the Trino Python driver's final query
  statistics. They are not inferred from table size.
- Peak RSS is the process high-water mark, not an allocation delta for one
  case.
- Spark executor usage is zero because this pilot benchmark uses the SQL/Trino
  suites. Spark volume benchmarks require a representative generated or copied
  dataset and an isolated executor measurement.
- No violations were injected into the persistent demo tables, so detection
  rate is correctly `N/A`. Independent expectation tests from phase 4 continue
  to prove pass/fail behavior; they are not substituted for a performance run.
- A 30-row demo snapshot cannot support a production overhead claim. The
  1K/10K/100K/1M rows and concurrent-validation matrix remains pending until
  representative, disposable benchmark datasets and hardware limits are
  agreed. Results must not be extrapolated from this pilot.

## Reproduction

From the repository root, after building the GX image and starting Trino and
Nessie on `lakehouse-network`:

```powershell
docker run --rm --network lakehouse-network --entrypoint python `
  -v "${PWD}\scripts\run_gx_phase6_benchmark.py:/tmp/run_gx_phase6_benchmark.py:ro" `
  lakehouse-gx-validator:1.24.0 `
  /tmp/run_gx_phase6_benchmark.py --warmups 1 --runs 5
```

Use `--output <path>` for a durable JSON artifact and `--summary-only` when raw
samples are not required.
