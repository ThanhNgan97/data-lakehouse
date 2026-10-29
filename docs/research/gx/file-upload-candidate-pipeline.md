# Governed file-upload candidate pipeline

`file_upload_candidate_pipeline` is an isolated migration target; the existing
universal DAG remains unchanged. Backend traffic continues to use the configured
`AIRFLOW_FILE_DAG_ID`, whose default is the legacy DAG.

Registered files follow this path:

```text
profile + contract resolution
-> prepare Silver candidate
-> GX Silver
-> CAS publish Silver
-> build sealed dbt Gold candidate
-> GX Gold
-> GX reconciliation
-> guarded Gold publish
-> Trino
-> Superset
-> archive source
```

An upload that cannot be mapped to an active file route follows only
`onboarding_required`. It never reaches a writer or publisher and reports
`NOT_CONFIGURED` with `merge_allowed=null`.

The Airflow GX tasks are checkpoints calling the same `gx-validator` service;
they are not separate GX deployments. Candidate state, validation request and
CAS publisher implementations remain shared with the API pipeline.

Cut over after acceptance testing with:

```text
AIRFLOW_FILE_DAG_ID=file_upload_candidate_pipeline
```
