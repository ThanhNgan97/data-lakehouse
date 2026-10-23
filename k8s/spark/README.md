# Spark runtime deployment

## Images

The initial integration smoke image is published to:

```text
docker.io/tdang2202/cusc-datalakehouse:spark-3.5.0-v1
```

The current application runtime image is:

```text
docker.io/tdang2202/cusc-datalakehouse:spark-3.5.0-v2
```

Build from the repository root. The build context is intentionally limited to
`lakehouse/spark` so `.env`, Git metadata, and unrelated project files cannot be
copied into the image. The build reuses the five resolved Iceberg/Nessie/S3A
JARs from the existing `custom-airflow:2.9.2` image. Build that image first if it
is not present locally:

```powershell
docker compose build airflow-webserver
```

```powershell
docker build `
  -f spark.Dockerfile `
  -t tdang2202/cusc-datalakehouse:spark-3.5.0-v2 `
  lakehouse/spark

docker push tdang2202/cusc-datalakehouse:spark-3.5.0-v2
```

The `v2` image contains Apache Spark, the Iceberg/Nessie/S3A JARs, and the
pinned Python dependencies in `lakehouse/spark/spark-runtime-requirements.txt`.
Do not reuse the full Airflow/application lockfile because those packages are
not part of Spark's runtime contract.

## MinIO credentials

Create the Secret from the local `.env` without writing its values to a YAML
file. Run these commands from the repository root:

```powershell
$minioAccessKey = (Get-Content .env | Where-Object { $_ -match '^MINIO_ACCESS_KEY=' } | Select-Object -First 1) -replace '^MINIO_ACCESS_KEY=', ''
$minioSecretKey = (Get-Content .env | Where-Object { $_ -match '^MINIO_SECRET_KEY=' } | Select-Object -First 1) -replace '^MINIO_SECRET_KEY=', ''

if (-not $minioAccessKey -or -not $minioSecretKey) {
  throw 'MINIO_ACCESS_KEY or MINIO_SECRET_KEY is missing from .env'
}

kubectl create secret generic minio-credentials `
  --namespace lakehouse-spark `
  --from-literal=access-key=$minioAccessKey `
  --from-literal=secret-key=$minioSecretKey `
  --dry-run=client -o yaml | kubectl apply -f -

Remove-Variable minioAccessKey, minioSecretKey
```

Do not commit an exported Secret manifest or the `.env` file.

The Airflow-triggered Bronze-to-Silver job also records pipeline failures in
PostgreSQL. Create its password Secret from the same local `.env` file:

```powershell
$postgresPassword = (Get-Content .env | Where-Object { $_ -match '^PG_PASSWORD=' } | Select-Object -First 1) -replace '^PG_PASSWORD=', ''

if (-not $postgresPassword) {
  throw 'PG_PASSWORD is missing from .env'
}

kubectl create secret generic postgres-credentials `
  --namespace lakehouse-spark `
  --from-literal=password=$postgresPassword `
  --dry-run=client -o yaml | kubectl apply -f -

Remove-Variable postgresPassword
```

## Private Docker Hub repository

If the Docker Hub repository is private, create a pull secret with a Docker Hub
access token. Do not use the account password:

```powershell
kubectl create secret docker-registry dockerhub-credentials `
  --namespace lakehouse-spark `
  --docker-server=https://index.docker.io/v1/ `
  --docker-username=tdang2202 `
  --docker-password='<DOCKER_HUB_ACCESS_TOKEN>'

kubectl patch serviceaccount spark-operator-spark `
  --namespace lakehouse-spark `
  --type merge `
  --patch '{"imagePullSecrets":[{"name":"dockerhub-credentials"}]}'
```

Skip this section when the repository is public.

## Run the integration smoke test

```powershell
kubectl delete sparkapplication spark-iceberg-smoke-test `
  --namespace lakehouse-spark `
  --ignore-not-found

kubectl apply -f k8s/spark/spark-iceberg-smoke-test.yaml
kubectl get sparkapplication spark-iceberg-smoke-test `
  --namespace lakehouse-spark `
  --watch
```

After the application reaches `COMPLETED`:

```powershell
kubectl logs spark-iceberg-smoke-test-driver `
  --namespace lakehouse-spark | Select-String 'LAKEHOUSE_SMOKE_TEST_OK|TABLE=|ROW_COUNT='
```

Expected markers:

```text
LAKEHOUSE_SMOKE_TEST_OK
TABLE=lakehouse.spark_smoke.spark_k8s_smoke
ROW_COUNT=3
```

Verify the same table through Trino:

```powershell
docker compose exec trino trino `
  --execute 'SELECT * FROM lakehouse.spark_smoke.spark_k8s_smoke ORDER BY id'
```

## Run the first migrated application

The Bronze-to-Silver application uses the `v2` image, which adds only the
Python packages required by that job:

```powershell
docker build `
  -f spark.Dockerfile `
  -t tdang2202/cusc-datalakehouse:spark-3.5.0-v2 `
  lakehouse/spark

docker push tdang2202/cusc-datalakehouse:spark-3.5.0-v2
```

Submit the application:

```powershell
kubectl delete sparkapplication spark-bronze-to-silver `
  --namespace lakehouse-spark `
  --ignore-not-found

kubectl apply -f k8s/spark/spark-bronze-to-silver.yaml
kubectl get sparkapplication spark-bronze-to-silver `
  --namespace lakehouse-spark `
  --watch
```

If no `bronze/data_extracted_*.parquet` object is waiting in MinIO, a successful
run logs that there is no new Bronze file and exits without changing Silver.

## Let Airflow submit SparkApplication resources

Apply the namespace-scoped Airflow permissions:

```powershell
kubectl apply -f k8s/spark/airflow-rbac.yaml
```

The Compose configuration mounts `./k8s` read-only into the Airflow webserver
and scheduler. Recreate only those services after adding or changing the mount:

```powershell
docker compose up -d --force-recreate airflow-webserver airflow-scheduler
```

Create a 30-day ServiceAccount token and store the generated Kubernetes config
in Airflow's metadata database. The token is passed over stdin and is not
written to the repository:

```powershell
$apiServer = (kubectl config view --minify -o jsonpath='{.clusters[0].cluster.server}').Replace('127.0.0.1', 'host.docker.internal')
$airflowToken = kubectl create token airflow-spark-submitter `
  --namespace lakehouse-spark `
  --duration=720h

$airflowToken | docker compose exec -T `
  -e KUBERNETES_API_SERVER=$apiServer `
  airflow-webserver `
  python /opt/airflow/k8s/spark/configure_airflow_connection.py

Remove-Variable airflowToken, apiServer
```

Refresh this connection before the displayed token expiry. The local Docker
Desktop connection disables TLS verification because the API certificate does
not match the `host.docker.internal` alias. Production clusters must use a
trusted API hostname and certificate verification.

Validate the migrated task without running the rest of the DAG:

```powershell
docker compose exec airflow-scheduler airflow tasks test `
  lakehouse_pipeline bronze_to_silver 2026-10-22T00:00:00+00:00
```

The task succeeds when Airflow creates the SparkApplication, streams the driver
log, observes exit code `0`, and deletes the completed test resource.
