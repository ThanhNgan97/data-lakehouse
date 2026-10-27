# Phase 11: Kubernetes readiness

Date: 2026-10-27

## GX-owned runtime contract

The GX image remains standalone, non-root, stateless, and configured through
environment variables. Validation summaries and Data Docs use external object
storage; suites and contracts are immutable image content. Secrets are injected
at runtime and are not copied into the image.

The service exposes:

- `GET /health/live`: process HTTP loop is alive;
- `GET /health/ready`: the service is accepting new validation requests;
- `python -m gx_runtime.healthcheck live|ready`: container or Kubernetes probe;
- `GET /metrics`: Prometheus scrape contract from phase 10.

On `SIGTERM` or `SIGINT`, readiness becomes false before HTTP shutdown begins.
New validation requests receive retryable `GX_UNAVAILABLE`; the server then
waits for active request threads, disposes the SQLAlchemy connection pool, and
closes its listener. Compose allows 30 seconds before forced termination.

## Resource starting point

The Compose service records the initial sizing proposal:

| Resource | Request | Limit |
|---|---:|---:|
| CPU | 1 core | 2 cores |
| Memory | 2 GiB | 4 GiB |

These values are a deployment starting point, not a universal production SLO.
They must be recalibrated using phase-6 dataset-size, concurrency, Spark, and
peak-memory measurements. Spark DataFrame validation may need a separate worker
profile from the lighter SQL service.

## Kubernetes hand-off

The infrastructure team owns Deployment/Job manifests, scheduling, storage
classes, ingress/service discovery, network policies, autoscaling, Pod
Disruption Budgets, and secret objects. Recommended probe mapping:

```yaml
livenessProbe:  /health/live
readinessProbe: /health/ready
terminationGracePeriodSeconds: 30
```

## Restart and retry behavior

- Important state is written outside the process before a successful response.
- A restarted instance reconstructs registries and clients from immutable files
  and environment configuration.
- Airflow retries infrastructure failures using the same validation identity;
  deterministic validation IDs and object keys make that retry traceable.
- Probe failures return non-zero without mutating validation state.
- Quality failures remain non-retryable; snapshot mismatches require refreshed
  identity and a new validation run.

Unit coverage verifies probe success/failure, repeated failed probes, and the
shutdown transition that marks the service unready and disposes pooled clients.
The full Kubernetes cluster and production manifests remain outside GX scope.
