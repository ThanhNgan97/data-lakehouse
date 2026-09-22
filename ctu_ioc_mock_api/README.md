# CTU IOC Mock API — Learning Outcomes

Mock REST API used to replace the static JSON-file input temporarily and verify
that the existing Lakehouse can ingest `education.learning_outcomes` through HTTP.

The service uses the same 30 mock business records already used for the verified
Bronze → Silver → Gold → Superset flow.

The active source contract is `schema_version = 2.0`: the API and Bronze use
source-native English field names, while Silver keeps the established canonical
Vietnamese business schema through an explicit mapping boundary after Bronze.

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
record.updated_at > updated_after
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
→ active v2 contract validation
→ configured source-native Spark schema
→ 9 Bronze metadata fields + deterministic checksum
→ source-native MinIO Bronze Parquet
→ explicit source-to-canonical mapping
→ canonical Silver processing
```

A minimal fetch example is in:

```text
integration_fetch_example.py
```

The important integration test is:

```text
Mock API HTTP (source-native v2)
    ↓
generic HTTP/Bronze ingestion
    ↓
Bronze Parquet (source-native)
    ↓
explicit source-to-canonical mapping
    ↓
Silver (canonical Vietnamese fields)
    ↓
Gold
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


## 10. Source contract v2.0 and Silver compatibility boundary

The active Mock API contract is version `2.0`.

The API now emits the 16 source-native business fields:

```text
record_id
program_code
program_name
academic_year
semester
student_count
passed_course_count
attempted_course_count
gpa_point_sum
gpa_student_count
warning_student_count
dropout_risk_student_count
on_track_student_count
progress_evaluated_student_count
updated_at
is_deleted
```

Bronze preserves these source field names exactly and adds the same 9
Lakehouse-managed metadata fields:

```text
_source_system
_source_type
_dataset
_schema_version
_ingestion_mode
_batch_id
_source_updated_at
_ingested_at
_record_checksum
```

The active JSON Schema is:

```text
lakehouse/spark/contracts/learning_outcomes.schema.json
```

The historical v1 schema is retained separately as:

```text
lakehouse/spark/contracts/learning_outcomes_v0_1.schema.json
```

The rename from the earlier v1 business field contract to v2 is a breaking
source-contract change, so the external `schema_version` is `2.0`.

The compatibility boundary is intentionally after Bronze:

```text
source-native API
    ↓
source-native Bronze
    ↓
source_to_canonical mapping
    ↓
canonical Silver
```

Silver therefore continues to use the existing Vietnamese canonical business
fields such as `ma_ban_ghi`, `ma_chuong_trinh`, `nam_hoc`, and `hoc_ky`.
Gold and Superset continue to consume the established canonical downstream
schema.

The v2 record schema allows harmless additional source fields so source
evolution can be preserved in Bronze. Unknown source fields are not promoted
automatically into Silver; only configured fields cross the mapping boundary.

Technical identifiers such as the dataset name, API route, pagination
parameters, and Lakehouse metadata names remain unchanged.
