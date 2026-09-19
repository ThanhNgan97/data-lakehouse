# CTU IOC Mock API -> Bronze implementation

Target: baseline `feature/new-direction`.

## Safety / architecture
1. **Step 1 is additive only**: add the API adapter, dataset config/schema, testdata, and shared Bronze writer. Do not touch the existing PDF/DOCX DAG.
2. API demo writes only to Bronze under:
   `bronze/api/ctu_ioc/education/learning_outcomes/batch_id=<batch_id>/data.parquet`
3. Current KPI Silver only consumes `bronze/data_extracted_*.parquet`, so the API object is intentionally isolated from KPI Silver.
4. After API A-L passes, apply `spark_ingest_bronze_shared_writer.patch` and rerun one PDF/DOCX ingestion for test M. That tiny patch makes the existing document path reuse the same physical Bronze writer without changing Gemini/KpiRecord parsing.

## Files to add
- `lakehouse/spark/bronze_writer.py`
- `lakehouse/spark/api_dataset_registry.py`
- `lakehouse/spark/api_ingestion.py`
- `lakehouse/spark/spark_ingest_api.py`
- `lakehouse/spark/contracts/learning_outcomes_v0_1.schema.json`
- `lakehouse/spark/testdata/mock_learning_outcomes_api_v0_1.json`
- `lakehouse/spark/testdata/bronze_learning_outcomes_demo_preview.csv`

## Runtime command for A-K
Because `./lakehouse/spark` is already mounted at `/opt/airflow/spark`, no image rebuild is needed for these Python files.

```bash
cd /opt/airflow/spark
python spark_ingest_api.py \
  --input-json /opt/airflow/spark/testdata/mock_learning_outcomes_api_v0_1.json \
  --dataset education.learning_outcomes \
  --ingestion-mode FULL_DEMO \
  --expected-count 30
```

Run it a second time for L. It creates a new batch ID; Bronze remains append/raw. Compare the printed checksum of the sample record across both runs: it must be identical.

## HTTP later
The core engine exposes `ingest_payload_to_bronze(...)`. A future HTTP adapter only needs:

```python
payload = response.json()
ingest_payload_to_bronze(spark, payload, dataset="education.learning_outcomes", ...)
```

No business-schema logic needs to move into the HTTP layer.
