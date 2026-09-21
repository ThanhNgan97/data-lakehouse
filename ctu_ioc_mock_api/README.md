# CTU IOC Mock API — Learning Outcomes

Mock REST API used to replace the static JSON-file input temporarily and verify
that the existing Lakehouse can ingest `education.learning_outcomes` through HTTP.

The service uses the same 30 mock business records already used for the verified
Bronze → Silver → Gold → Superset flow.

## 1. Start with Docker

```powershell
cd ctu_ioc_mock_api
docker compose up -d --build
```

Check:

```powershell
Invoke-RestMethod http://localhost:8090/health
```

Full request:

```powershell
Invoke-RestMethod "http://localhost:8090/api/v1/education/learning-outcomes?limit=500"
```

Interactive OpenAPI documentation:

```text
http://localhost:8090/docs
```

## 2. URLs to use from the Lakehouse

From the Windows host:

```text
http://localhost:8090/api/v1/education/learning-outcomes
```

From another container in the SAME Docker Compose network:

```text
http://ctu-ioc-mock-api:8000/api/v1/education/learning-outcomes
```

Do not use `localhost:8090` from another container. Inside a container,
`localhost` refers to that container itself.

## 3. Pagination

Example:

```text
GET /api/v1/education/learning-outcomes?limit=10
```

Use the returned opaque `pagination.next_cursor`:

```text
GET /api/v1/education/learning-outcomes?limit=10&cursor=<next_cursor>
```

## 4. Incremental query

Example:

```text
GET /api/v1/education/learning-outcomes?updated_after=2026-09-21T00:00:00%2B07:00&limit=500
```

The filter is strict:

```text
record.thoi_gian_cap_nhat_nguon > updated_after
```

## 5. Optional mock authentication

Set in `docker-compose.yml`:

```yaml
MOCK_API_KEY: "demo-secret"
```

Then call:

```http
Authorization: Bearer demo-secret
```

Leaving `MOCK_API_KEY` empty keeps the local mock endpoint open.

## 6. Mock-only scenarios for incremental tests

These endpoints are NOT part of the proposed production CTU IOC contract.

Reset:

```powershell
Invoke-RestMethod -Method Post http://localhost:8090/mock/scenario/baseline
```

Simulate a newer update:

```powershell
Invoke-RestMethod -Method Post http://localhost:8090/mock/scenario/newer_update
```

Simulate a soft delete:

```powershell
Invoke-RestMethod -Method Post http://localhost:8090/mock/scenario/soft_delete
```

Simulate a new record:

```powershell
Invoke-RestMethod -Method Post http://localhost:8090/mock/scenario/new_record
```

After activating a scenario, query the normal production-style endpoint again.
This lets the existing Bronze/Silver pipeline prove UPDATE, DELETE and INSERT
behavior using real HTTP instead of a local JSON file.

## 7. Smoke test

After the container is running:

```powershell
python smoke_test.py
```

Expected output starts with:

```text
MOCK_API_CONNECTION_PASS
```

## 8. Integrating into the existing Lakehouse

The current pipeline should NOT be rewritten.

Only replace the mock-file read step:

```text
read local JSON file
```

with:

```text
HTTP GET
→ parse JSON response
→ existing contract validation
→ existing explicit Spark schema
→ existing Bronze metadata/checksum
→ existing MinIO Bronze writer
```

A minimal fetch example is in:

```text
integration_fetch_example.py
```

The important integration test is:

```text
Mock API HTTP
    ↓
existing Bronze ingestion
    ↓
Bronze Parquet
    ↓
existing Silver
    ↓
existing Gold
```

For the first HTTP test, use the `baseline` scenario and expect 30 records.

## 9. Production-contract boundary

The production-like endpoint is:

```text
GET /api/v1/education/learning-outcomes
```

The following are test-only:

```text
GET  /mock/scenarios
POST /mock/scenario/{scenario}
```

Do not include `/mock/*` in the final specification sent to CTU IOC.


## Naming contract v1.0

The Vietnamese snake_case business schema is the final demo
contract version 1.0.

The earlier English business-field schema was an internal
pre-release representation and was not treated as a published
external contract.

Technical identifiers such as the dataset name, API route,
pagination parameters, and Lakehouse metadata remain unchanged.
