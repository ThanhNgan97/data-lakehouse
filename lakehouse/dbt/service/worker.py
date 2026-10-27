"""Private-network dbt worker. Allowlisted datasets/models; no shell or arbitrary SQL API."""
from concurrent.futures import ThreadPoolExecutor
import copy
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import subprocess
import sys
from threading import RLock
import time
import re

import trino
import requests

sys.path[:0] = ["/contract"]
from dbt_gold_contract import identity, DATASETS, outputs, SQL_VALIDATION_CONTRACT
from result_gate import verify_results, verify_sealed_results

TRINO_HOST = os.environ.get("DBT_TRINO_HOST", "trino")
ROOT = Path("/artifacts")
ROOT.mkdir(exist_ok=True)
POOL = ThreadPoolExecutor(max_workers=1)
LOCK = RLock()
STATES = {}


def read(job):
    with LOCK:
        if job not in STATES:
            STATES[job] = json.loads((ROOT / job / "status.json").read_text())
        return copy.deepcopy(STATES[job])


def save(job, value):
    with LOCK:
        directory = ROOT / job
        directory.mkdir(exist_ok=True)
        temporary = directory / "status.tmp"
        for attempt in range(10):
            try:
                temporary.write_text(json.dumps(value), encoding="utf-8")
                temporary.replace(directory / "status.json")
                break
            except OSError:
                if attempt == 9:
                    raise
                time.sleep(0.1)
        STATES[job] = copy.deepcopy(value)



def project_fingerprint():
    digest = hashlib.sha256()
    for path in sorted(Path("/project").rglob("*")):
        if path.is_file() and path.suffix in (".sql", ".yml", ".yaml"):
            digest.update(path.relative_to("/project").as_posix().encode())
            digest.update(b"\0")
            digest.update(path.read_bytes())
    return digest.hexdigest()


def execute(payload):
    job, ref, alias = identity(payload["dataset"], payload["run_id"])
    directory = ROOT / job
    state = {"job_id": job, "ref": ref, "status": "running", **payload}
    save(job, state)
    cursor = None
    catalog_created = False
    try:
        cursor = trino.dbapi.connect(host=TRINO_HOST,
                                    port=int(os.environ.get("DBT_TRINO_PORT", "8080")),
                                    user="dbt_worker").cursor()
        deadline = time.monotonic() + 120
        while True:
            try:
                cursor.execute("SELECT 1").fetchall()
                break
            except Exception:
                if time.monotonic() >= deadline:
                    raise
                time.sleep(2)
        start = requests.get(os.environ["NESSIE_API_URL"].rstrip("/") + "/trees/tree/" + ref, timeout=30)
        start.raise_for_status()
        if start.json()["hash"] != payload["expected_head"]:
            raise RuntimeError("Prepared candidate changed before build")
        project_hash = project_fingerprint()
        # Secrets stay in Trino's environment, never in SQL or HTTP payloads.
        properties = {
            "iceberg.catalog.type": "nessie",
            "iceberg.nessie-catalog.uri": os.environ["NESSIE_API_URL"],
            "iceberg.nessie-catalog.ref": ref,
            "iceberg.nessie-catalog.default-warehouse-dir": "s3a://" + os.environ["S3_BUCKET_NAME"] + "/iceberg-warehouse",
            "fs.native-s3.enabled": "true", "fs.hadoop.enabled": "false",
            "s3.endpoint": os.environ["S3_ENDPOINT"],
            "s3.path-style-access": "true", "s3.region": os.environ.get("S3_REGION", "us-east-1"),
            "s3.aws-access-key": "${ENV:S3_ACCESS_KEY}",
            "s3.aws-secret-key": "${ENV:S3_SECRET_KEY}",
        }
        sql = ", ".join('"' + key + '" = \'' + value.replace("'", "''") + "'" for key, value in properties.items())
        cursor.execute(f"CREATE CATALOG {ref} USING iceberg WITH ({sql})").fetchall()
        catalog_created = True
        env = dict(os.environ, DBT_TRINO_HOST=TRINO_HOST, DBT_TARGET_CATALOG=ref,
                   DBT_TARGET_SCHEMA="gold", DBT_TARGET_PATH=str(directory / "target"),
                   DBT_LOG_PATH=str(directory / "logs"))
        selector = DATASETS[payload["dataset"]][0]
        learning = payload["dataset"] == "education.learning_outcomes"
        alias_var = "learning_gold_alias" if learning else "gold_alias"
        variables = {alias_var: alias}
        if learning:
            variables["learning_period_alias"] = outputs(payload["dataset"], payload["run_id"])["learning_outcomes_by_period"]
        source_key = "silver." + payload["dataset"].split(".")[-1]
        source_response = requests.get(os.environ["NESSIE_API_URL"].rstrip("/") + "/contents/" + source_key,
                                       params={"ref": ref}, timeout=30)
        source_response.raise_for_status()
        source_before = source_response.json()
        with (directory / "build.log").open("w") as log:
            result = subprocess.run([
                "dbt", "build", "--project-dir", "/project", "--profiles-dir", "/project",
                "--select", "+" + selector, "--vars", json.dumps(variables),
            ], env=env, stdout=log, stderr=subprocess.STDOUT, timeout=900)
        if result.returncode:
            raise RuntimeError("dbt build failed; inspect private worker build.log")
        if project_fingerprint() != project_hash:
            raise RuntimeError("Project changed during build; start a new run")
        artifact = directory / "target/run_results.json"
        summary = verify_results(artifact, payload["dataset"])
        response = requests.get(os.environ["NESSIE_API_URL"].rstrip("/") + "/trees/tree/" + ref, timeout=30)
        response.raise_for_status()
        build_hash = response.json()["hash"]
        source_response = requests.get(os.environ["NESSIE_API_URL"].rstrip("/") + "/contents/" + source_key,
                                       params={"ref": ref, "hashOnRef": build_hash}, timeout=30)
        source_response.raise_for_status()
        if source_response.json() != source_before:
            raise RuntimeError("Silver source changed during dbt build/tests")
        candidates = {}
        for name, candidate_alias in outputs(payload["dataset"], payload["run_id"]).items():
            response = requests.get(os.environ["NESSIE_API_URL"].rstrip("/") + "/contents/gold." + candidate_alias,
                                    params={"ref": ref, "hashOnRef": build_hash}, timeout=30)
            response.raise_for_status()
            candidates[name] = response.json()
        state.update(status="passed", dataset=payload["dataset"], validation_contract=SQL_VALIDATION_CONTRACT,
                     source_content=source_before, artifact_sha256=hashlib.sha256(artifact.read_bytes()).hexdigest(),
                     result_summary=summary, project_sha256=project_hash, build_ref_hash=build_hash,
                     candidate_contents=candidates)
    except Exception as exc:
        state.update(status="failed", error=str(exc))
    finally:
        if catalog_created:
            try:
                cursor.execute(f"DROP CATALOG {ref}").fetchall()
            except Exception:
                state.update(catalog_cleanup_failed=True, status="failed", error="Candidate catalog cleanup failed")
        if cursor is not None:
            try:
                cursor.close()
            except Exception:
                state.update(status="failed", error="Worker connection cleanup failed")
        save(job, state)


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def respond(self, code, value):
        body = json.dumps(value).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path == "/health":
            return self.respond(200, {"status": "ok"})
        job = self.path.removeprefix("/jobs/")
        if len(job) != 24 or any(c not in "0123456789abcdef" for c in job):
            return self.respond(400, {"error": "Invalid job ID"})
        try:
            state = read(job)
            if state.get("status") == "passed":
                # Re-read the sealed artifact on every successful receipt, including publish retries.
                artifact = ROOT / job / "target/run_results.json"
                if state.get("validation_contract") != SQL_VALIDATION_CONTRACT or state.get("catalog_cleanup_failed"):
                    raise RuntimeError("Unknown or incomplete build contract")
                verify_sealed_results(artifact, state["dataset"], state["artifact_sha256"])
            self.respond(200, state)
        except RuntimeError as exc:
            self.respond(409, {"error": str(exc)})
        except FileNotFoundError:
            self.respond(404, {"error": "Unknown job"})
        except (OSError, json.JSONDecodeError):
            self.respond(503, {"error": "Job state temporarily unavailable"})

    def do_POST(self):
        try:
            length = int(self.headers.get("Content-Length", "0"))
            if self.path != "/jobs" or not 0 < length <= 4096:
                raise ValueError("Invalid request")
            payload = json.loads(self.rfile.read(length))
            if set(payload) != {"dataset", "run_id", "expected_head"} or not isinstance(payload["expected_head"], str) or not re.fullmatch(r"[0-9a-f]{8,128}", payload["expected_head"]):
                raise ValueError("Invalid job payload")
            job, ref, _ = identity(payload["dataset"], payload["run_id"])
            with LOCK:
                if not (ROOT / job / "status.json").exists():
                    save(job, {"job_id": job, "ref": ref, "status": "queued", **payload})
                    POOL.submit(execute, payload)
                else:
                    previous = read(job)
                    if any(previous.get(key) != value for key, value in payload.items()):
                        raise ValueError("Job identity/expected head changed")
            self.respond(202, {"job_id": job, "ref": ref, "status": "accepted"})
        except Exception as exc:
            self.respond(400, {"error": str(exc)})


if __name__ == "__main__":
    # A worker restart cannot prove an interrupted build completed. Fail closed.
    for path in ROOT.glob("*/status.json"):
        state = json.loads(path.read_text())
        if state.get("status") in ("queued", "running"):
            state.update(status="failed", error="Worker restarted; use a new DAG run")
            save(path.parent.name, state)
    ThreadingHTTPServer(("0.0.0.0", 8080), Handler).serve_forever()
