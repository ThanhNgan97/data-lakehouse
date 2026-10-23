"""Configure Airflow's Kubernetes connection from a token read on stdin."""

from __future__ import annotations

import base64
import json
import os
import sys
from datetime import datetime, timezone

from airflow.models import Connection
from airflow.utils.session import create_session


CONNECTION_ID = "kubernetes_default"
NAMESPACE = "lakehouse-spark"


def token_expiry(token: str) -> str:
    try:
        payload = token.split(".")[1]
        payload += "=" * (-len(payload) % 4)
        claims = json.loads(base64.urlsafe_b64decode(payload))
        return datetime.fromtimestamp(claims["exp"], timezone.utc).isoformat()
    except (IndexError, KeyError, TypeError, ValueError, json.JSONDecodeError):
        return "unknown"


def main() -> None:
    api_server = os.environ["KUBERNETES_API_SERVER"]
    token = sys.stdin.read().strip()
    if not token:
        raise RuntimeError("Kubernetes token was not provided on stdin")

    kube_config = {
        "apiVersion": "v1",
        "kind": "Config",
        "clusters": [
            {
                "name": "docker-desktop",
                "cluster": {
                    "server": api_server,
                    "insecure-skip-tls-verify": True,
                },
            }
        ],
        "contexts": [
            {
                "name": "airflow",
                "context": {
                    "cluster": "docker-desktop",
                    "namespace": NAMESPACE,
                    "user": "airflow-spark-submitter",
                },
            }
        ],
        "current-context": "airflow",
        "users": [
            {
                "name": "airflow-spark-submitter",
                "user": {"token": token},
            }
        ],
    }
    extra = {
        "in_cluster": False,
        "kube_config": json.dumps(kube_config),
        "namespace": NAMESPACE,
        "disable_verify_ssl": True,
    }

    with create_session() as session:
        existing = (
            session.query(Connection)
            .filter(Connection.conn_id == CONNECTION_ID)
            .one_or_none()
        )
        if existing is not None:
            session.delete(existing)
            session.flush()
        session.add(
            Connection(
                conn_id=CONNECTION_ID,
                conn_type="kubernetes",
                extra=json.dumps(extra),
            )
        )

    print(f"Configured Airflow connection: {CONNECTION_ID}")
    print(f"Token expiry (UTC): {token_expiry(token)}")


if __name__ == "__main__":
    main()
