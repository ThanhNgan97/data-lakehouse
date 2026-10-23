"""Fail fast when the services required by a Spark/Iceberg run are unreachable.

This does not replace runtime monitoring: Docker DNS can still fail later in a
job. It avoids starting an expensive Spark session during an existing outage.
"""

from __future__ import annotations

import os
import socket
import time
import urllib.request
from urllib.parse import urlparse


def _tcp_ready(host: str, port: int, timeout: float = 3.0) -> None:
    with socket.create_connection((host, port), timeout=timeout):
        pass


def _nessie_ready(url: str, timeout: float = 3.0) -> None:
    with urllib.request.urlopen(url.rstrip("/") + "/config", timeout=timeout) as response:
        if response.status != 200:
            raise RuntimeError(f"Nessie returned HTTP {response.status}")


def check_services(attempts: int = 5, delay: float = 3.0) -> None:
    nessie_url = os.environ.get("NESSIE_API_URL", "http://nessie:19120/api/v1")
    s3_url = os.environ.get("S3_ENDPOINT", "http://s3g:9878")
    parsed_s3 = urlparse(s3_url)
    if not parsed_s3.hostname:
        raise ValueError("S3_ENDPOINT must contain a hostname")
    s3_port = parsed_s3.port or (443 if parsed_s3.scheme == "https" else 80)

    for attempt in range(1, attempts + 1):
        try:
            _tcp_ready("postgres", 5432)
            _tcp_ready(parsed_s3.hostname, s3_port)
            _nessie_ready(nessie_url)
            print("PREFLIGHT_OK=postgres,ozone_s3g,nessie", flush=True)
            return
        except (OSError, RuntimeError) as exc:
            print(f"PREFLIGHT_RETRY={attempt}/{attempts}: {exc}", flush=True)
            if attempt == attempts:
                raise RuntimeError("Required lakehouse services are unavailable") from exc
            time.sleep(delay)


if __name__ == "__main__":
    check_services()
