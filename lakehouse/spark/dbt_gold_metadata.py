"""Metadata-only Iceberg Java helper; no SparkSession and no manual JSON mutation."""
import hashlib
import importlib.util
import os
from pathlib import Path
import subprocess
import tempfile

from dbt_gold_contract import identity, outputs
from env_config import S3_ENDPOINT, S3_ACCESS_KEY, S3_SECRET_KEY, S3_BUCKET_NAME, NESSIE_API_URL


def guard(config, run_id, state):
    _, ref, _ = identity(config.dataset, run_id)
    expected = outputs(config.dataset, run_id)
    if state["ref"] != ref or {name: entry["alias"] for name, entry in state["outputs"].items()} != expected:
        raise RuntimeError("Metadata guard output identity mismatch")
    source = Path(__file__).parent / "java/GoldMetadataGuard.java"
    classpath = os.environ.get("DBT_ICEBERG_CLASSPATH")
    if not classpath:
        spec = importlib.util.find_spec("pyspark")
        if spec is None:
            raise RuntimeError("Set DBT_ICEBERG_CLASSPATH to the tested Iceberg/Hadoop library set")
        classpath = str(Path(spec.origin).parent / "jars/*") + os.pathsep + "/opt/spark/extra-jars/*"
    digest = hashlib.sha256(source.read_bytes() + classpath.encode()).hexdigest()[:16]
    classes = Path(tempfile.gettempdir()) / ("dbt-gold-metadata-"+digest)
    classes.mkdir(exist_ok=True)
    env = dict(os.environ, S3_ENDPOINT=S3_ENDPOINT, S3_ACCESS_KEY=S3_ACCESS_KEY,
               S3_SECRET_KEY=S3_SECRET_KEY, S3_BUCKET_NAME=S3_BUCKET_NAME, NESSIE_API_URL=NESSIE_API_URL)
    if not (classes / "GoldMetadataGuard.class").exists():
        result = subprocess.run(["javac", "-cp", classpath, "-d", str(classes), str(source)],
                                env=env, capture_output=True, text=True, timeout=60)
        if result.returncode:
            raise RuntimeError("Iceberg metadata helper compilation failed: " + result.stderr[-2000:])
    result = subprocess.run(["java", "-Xmx256m", "-cp", str(classes)+os.pathsep+classpath,
                             "GoldMetadataGuard", ref, *expected.values()],
                            env=env, capture_output=True, text=True, timeout=120)
    if result.returncode or any("GUARD_OK="+alias not in result.stdout for alias in expected.values()):
        raise RuntimeError("Iceberg metadata helper failed: " + result.stderr[-2000:])
    return {"engine":"iceberg_java", "outputs":list(expected), "spark_session":False}
