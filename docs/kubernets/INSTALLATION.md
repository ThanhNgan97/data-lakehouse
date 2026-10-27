# Spark Operator on Docker Desktop Kubernetes

## Architecture

```text
Airflow (Docker Compose)
        |
        | SparkKubernetesOperator / Kubernetes API
        v
Spark driver + executor pods (Docker Desktop Kubernetes)
        |-- Apache Ozone S3 Gateway: host.docker.internal:9878
        |-- Project Nessie: host.docker.internal:19120
        `-- PostgreSQL when required: host.docker.internal:5433
```

Airflow performs orchestration and non-Spark validation only. Every task that
creates a SparkSession runs as a SparkApplication.

## Prerequisites

1. Enable Docker Desktop Kubernetes.
2. Install the Spark Operator and confirm the `SparkApplication` CRD exists.
3. Ensure the `lakehouse-spark` namespace and service accounts exist.
4. Start the Compose services and verify Ozone, Nessie, PostgreSQL, Trino, and
   Airflow are reachable on their published ports.

```powershell
kubectl get nodes
kubectl get crd sparkapplications.sparkoperator.k8s.io
kubectl get pods -n spark-operator
docker compose ps
```

## Runtime setup

Follow `k8s/spark/README.md` to:

1. build and publish `spark-3.5.0-v3`;
2. create `ozone-s3-credentials` and `postgres-credentials`;
3. apply namespace-scoped Airflow RBAC;
4. configure the Airflow `kubernetes_default` connection;
5. run the Iceberg/Nessie/Ozone smoke test.

Do not put credentials in YAML. Spark driver and executor pods receive them
from Kubernetes Secrets. Do not replace `host.docker.internal` with a Docker
Compose service name: pods and Compose containers use different networks.

## Operational checks

```powershell
kubectl auth can-i create sparkapplications.sparkoperator.k8s.io `
  --as=system:serviceaccount:lakehouse-spark:airflow-spark-submitter `
  --namespace lakehouse-spark

kubectl get sparkapplications -n lakehouse-spark
kubectl get pods -n lakehouse-spark
docker compose exec airflow-webserver airflow pools get spark_k8s
```

Only one Spark task is allowed through the `spark_k8s` Airflow pool. This keeps
the driver plus two one-core executors within the current Docker Desktop
resource budget.
