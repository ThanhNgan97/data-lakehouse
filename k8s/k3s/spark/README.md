# K3s Spark smoke test

This directory is a K3s-only smoke-test path. It deliberately leaves the
Docker Desktop manifests under `k8s/spark/` unchanged.

The test image uses the existing Spark 3.5.0 runtime. A one-replica TCP proxy
on the K3s node forwards Ozone S3 Gateway and Nessie traffic to the Windows
Docker host through its stable MagicDNS name `dylan.tail2f6d62.ts.net`.
The proxy queries Tailscale MagicDNS directly (`100.100.100.100`) because
cluster DNS and the VM's gateway resolver do not resolve MagicDNS names for
containers.

## Preconditions

1. Start the Windows Compose stack, including `postgres`, `s3g`, `nessie`, and
   the mock API if later application templates need it.
2. Allow inbound Tailscale traffic from the K3s VM to Windows ports `9878`,
   `19120`, `5433`, and `8090`. Do not expose these ports to the public
   internet.
3. From the K3s VM, verify the two smoke-test dependencies:

   ```bash
   curl -fsS http://dylan.tail2f6d62.ts.net:19120/api/v1/config
   curl -sS -o /dev/null -w '%{http_code}\n' http://dylan.tail2f6d62.ts.net:9878/
   ```

4. Install a Spark Operator release that serves
   `sparkapplications.sparkoperator.k8s.io/v1beta2` and configures
   `lakehouse-spark` as its job namespace, then verify its CRD:

   ```bash
   sudo k3s kubectl get crd sparkapplications.sparkoperator.k8s.io
   ```

## Deploy

Create the `lakehouse-spark` namespace and `ozone-s3-credentials` from the
existing S3 access and secret keys. For a private Docker Hub image, create a
`dockerhub-pull` Secret with a Docker Hub PAT granted **Repo Read-only** (not
Repo Public Read-only). Do not save or commit a Secret manifest or token.

```bash
sudo k3s kubectl create namespace lakehouse-spark --dry-run=client -o yaml | sudo k3s kubectl apply -f -
sudo k3s kubectl -n lakehouse-spark patch serviceaccount spark-operator-spark \
  --type=merge -p '{"imagePullSecrets":[{"name":"dockerhub-pull"}]}'
sudo k3s kubectl apply -f k8s/k3s/spark/windows-dependencies-proxy.yaml
sudo k3s kubectl -n lakehouse-spark apply -f k8s/k3s/spark/spark-iceberg-smoke-test.yaml
sudo k3s kubectl -n lakehouse-spark get sparkapplication,pods --watch
```

The driver log must contain `LAKEHOUSE_SMOKE_TEST_OK` and `ROW_COUNT=3`.

## Rollback

```bash
sudo k3s kubectl -n lakehouse-spark delete sparkapplication spark-iceberg-smoke-test --ignore-not-found
```
