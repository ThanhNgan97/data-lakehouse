# Spark runtime on Docker Desktop Kubernetes

Airflow runs in Docker Compose and submits every PySpark workload to the
Spark Operator in the `lakehouse-spark` namespace. Apache Ozone is the only
object store; Spark pods reach its S3 Gateway through
`http://host.docker.internal:9878`.

## Build and publish the runtime image

Build the Airflow image first because it supplies the pinned Iceberg, Nessie,
and S3A JARs used by `spark.Dockerfile`:

```powershell
docker compose build airflow-webserver
docker build -f spark.Dockerfile `
  -t tdang2202/cusc-datalakehouse:spark-3.5.0-v3 `
  lakehouse/spark
docker push tdang2202/cusc-datalakehouse:spark-3.5.0-v3
```

## Create runtime secrets

Create secrets directly from `.env`; never export or commit their manifests:

```powershell
$s3AccessKey = (Get-Content .env | Where-Object { $_ -match '^S3_ACCESS_KEY=' } | Select-Object -First 1) -replace '^S3_ACCESS_KEY=', ''
$s3SecretKey = (Get-Content .env | Where-Object { $_ -match '^S3_SECRET_KEY=' } | Select-Object -First 1) -replace '^S3_SECRET_KEY=', ''
$postgresPassword = (Get-Content .env | Where-Object { $_ -match '^PG_PASSWORD=' } | Select-Object -First 1) -replace '^PG_PASSWORD=', ''

kubectl create namespace lakehouse-spark --dry-run=client -o yaml | kubectl apply -f -
kubectl create secret generic ozone-s3-credentials `
  --namespace lakehouse-spark `
  --from-literal=access-key=$s3AccessKey `
  --from-literal=secret-key=$s3SecretKey `
  --dry-run=client -o yaml | kubectl apply -f -
kubectl create secret generic postgres-credentials `
  --namespace lakehouse-spark `
  --from-literal=password=$postgresPassword `
  --dry-run=client -o yaml | kubectl apply -f -

Remove-Variable s3AccessKey, s3SecretKey, postgresPassword
```

For a private Docker Hub repository, attach a registry secret to the
`spark-operator-spark` service account.

## Configure Airflow access

```powershell
kubectl apply -f k8s/spark/airflow-rbac.yaml

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

`airflow-init` creates the one-slot `spark_k8s` pool. Recreate Airflow after
changing DAGs or mounts:

```powershell
docker compose up -d --force-recreate airflow-init airflow-webserver airflow-scheduler
```

## Validate the integration

```powershell
kubectl delete sparkapplication spark-iceberg-smoke-test `
  --namespace lakehouse-spark --ignore-not-found
kubectl apply -f k8s/spark/spark-iceberg-smoke-test.yaml
kubectl get sparkapplication spark-iceberg-smoke-test `
  --namespace lakehouse-spark --watch
```

The driver log must contain:

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

All templates under `k8s/spark/templates` use the v3 image, Ozone S3 Gateway,
Nessie, namespace-scoped RBAC, and the `ozone-s3-credentials` secret.
