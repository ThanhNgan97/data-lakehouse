# Apache Ozone migration baseline

Recorded for the removal of MinIO from the active lakehouse runtime.

## Runtime baseline

- Apache Ozone SCM, OM, three DataNodes, S3 Gateway and Recon were healthy before implementation.
- Backend, Airflow and Trino used the Ozone S3 Gateway endpoint on port 9878.
- The active Compose configuration no longer declared a MinIO service.
- Stopped legacy containers `demo-minio` and `demo-minio-init` were still present.
- Legacy Docker volumes with `minio_data` names were still present and were not removed before validation.

## Compatibility baseline

- Bucket: `university-lakehouse`
- Staging prefix: `staging/`
- Processed source archive: previously mixed below `archive/`
- Iceberg warehouse: `s3a://university-lakehouse/iceberg-warehouse`
- Nessie reference: `main`
- Trino catalog: `lakehouse`

## Migration gates

1. Source code and runtime dependencies no longer use MinIO.
2. Upload writes an exact object to Ozone and records SHA-256 metadata.
3. Airflow validates the exact uploaded object without scanning or demo fallback.
4. Archive copy is verified before the staging source is deleted.
5. Retention remains disabled and dry-run until manually enabled.
6. File and API pipelines pass through Iceberg, Nessie and Trino on Ozone.
7. Only after those gates pass may stopped MinIO containers and project-scoped volumes be removed.
