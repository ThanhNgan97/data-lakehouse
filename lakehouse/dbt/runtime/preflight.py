"""Step 1 readiness: SELECT 1, HTTP GET and S3 HEAD only; no business data writes."""
import argparse
from importlib.metadata import version
import json
import os
import platform
import sys

import boto3
from botocore.config import Config
import requests
import trino


EXPECTED = {"dbt-core": "1.9.8", "dbt-trino": "1.9.3"}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--local", action="store_true", help="Check installed packages only")
    args = parser.parse_args()
    report = {"status": "running", "checks": {}, "read_only": True}

    def check(name, action):
        try:
            report["checks"][name] = {"status": "passed", "detail": action()}
        except Exception as exc:
            # Do not echo exception text: SDK/HTTP exceptions may contain credentials.
            report["checks"][name] = {"status": "failed", "error_type": type(exc).__name__}

    def packages():
        actual = {name: version(name) for name in EXPECTED}
        if actual != EXPECTED or sys.version_info[:2] != (3, 11):
            raise RuntimeError("Runtime version mismatch")
        return {"python": platform.python_version(), **actual,
                "trino-client": version("trino"), "boto3": version("boto3")}

    def trino_info():
        host = os.environ["DBT_TRINO_HOST"]
        port = int(os.environ.get("DBT_TRINO_PORT", "8080"))
        response = requests.get(f"http://{host}:{port}/v1/info", timeout=10)
        response.raise_for_status()
        info = response.json()
        if info.get("starting") or info.get("nodeVersion", {}).get("version") != "435":
            raise RuntimeError("Expected ready Trino 435")
        return {"version": "435", "starting": False}

    def select_one():
        connection = trino.dbapi.connect(
            host=os.environ["DBT_TRINO_HOST"],
            port=int(os.environ.get("DBT_TRINO_PORT", "8080")),
            user="dbt_readiness", http_scheme="http", request_timeout=10,
            max_attempts=1,
        )
        cursor = connection.cursor()
        try:
            cursor.execute("SELECT 1")
            if cursor.fetchall() != [[1]]:
                raise RuntimeError("Unexpected SELECT 1 result")
            return {"query_id": cursor.query_id, "result": 1}
        finally:
            cursor.close()
            connection.close()

    def nessie():
        url = os.environ["NESSIE_API_URL"].rstrip("/")
        response = requests.get(url + "/config", timeout=10)
        response.raise_for_status()
        response.json()
        return {"config_reachable": True}

    def ozone():
        client = boto3.client(
            "s3", endpoint_url=os.environ["S3_ENDPOINT"],
            aws_access_key_id=os.environ["S3_ACCESS_KEY"],
            aws_secret_access_key=os.environ["S3_SECRET_KEY"],
            region_name=os.environ.get("S3_REGION", "us-east-1"),
            config=Config(s3={"addressing_style": "path"}, connect_timeout=5,
                          read_timeout=10, retries={"max_attempts": 0}),
        )
        try:
            response = client.head_bucket(Bucket=os.environ["S3_BUCKET_NAME"])
            status = response["ResponseMetadata"]["HTTPStatusCode"]
            if status != 200:
                raise RuntimeError("Bucket HEAD failed")
            return {"head_bucket_http": status}
        finally:
            client.close()

    check("packages", packages)
    if not args.local:
        check("trino_info", trino_info)
        check("trino_select_one", select_one)
        check("nessie", nessie)
        check("ozone", ozone)
    passed = all(item["status"] == "passed" for item in report["checks"].values())
    report["status"] = "passed" if passed else "failed"
    print(json.dumps(report, indent=2), flush=True)
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
