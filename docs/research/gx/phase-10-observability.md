# Phase 10: GX observability

Date: 2026-10-27

## Outcome

The standalone GX HTTP service exposes Prometheus text format at `GET /metrics`.
Prometheus/Grafana infrastructure is deliberately outside the GX scope.

The endpoint publishes:

- `gx_validation_duration_seconds` as a histogram;
- `gx_validation_rows` and `gx_validation_bytes_scanned` as last-observed gauges
  when the validation engine supplies those measurements;
- `gx_validation_timeout_total`, `gx_engine_error_total`, `gx_retry_total`, and
  `gx_quality_failure_total` as counters.

Retries are counted when a request has `retry_attempt > 0`. Engine adapters may
publish row and scan measurements through summary metric keys
`validation_rows`/`row_count`/`dataset_rows` and
`validation_bytes_scanned`/`bytes_scanned`/`trino_bytes_scanned`.

## Cardinality policy

Only bounded operational dimensions are labels: `layer`, `engine`, and
`status`. `run_id`, `snapshot_id`, `commit_hash`, validation IDs, asset names,
and exception messages are never Prometheus labels. They remain in durable
validation results or structured application logs.

## Scrape contract

```text
GET http://gx-validator:8090/metrics
Content-Type: text/plain; version=0.0.4; charset=utf-8
```

The Compose environment sets `GX_METRICS_PATH=/metrics`. Deployments may
override it when their scrape convention requires another path; `/metrics`
remains the stable default contract.
