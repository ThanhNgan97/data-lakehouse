"""Configure Spark's JVM dependencies for container and host execution."""

from __future__ import annotations

import os
from pathlib import Path


SPARK_PACKAGES = (
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3,"
    "org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.77.1,"
    "org.apache.hadoop:hadoop-aws:3.3.4"
)


def configure_pyspark_dependencies() -> None:
    """Set submit arguments before importing PySpark.

    The Airflow image contains pre-resolved JARs in ``SPARK_EXTRA_JARS_DIR``.
    The package fallback preserves direct host execution for developers, while
    containerized Airflow never depends on Maven during a DAG run.
    """
    jar_dir_value = os.environ.get("SPARK_EXTRA_JARS_DIR", "").strip()
    if jar_dir_value:
        jar_dir = Path(jar_dir_value)
        if not jar_dir.is_dir():
            raise RuntimeError(
                f"SPARK_EXTRA_JARS_DIR does not exist: {jar_dir}"
            )
        dependency_args = (
            f"--conf spark.driver.extraClassPath={jar_dir}/* "
            f"--conf spark.executor.extraClassPath={jar_dir}/* "
        )
    else:
        dependency_args = f"--packages {SPARK_PACKAGES} "

    os.environ["PYSPARK_SUBMIT_ARGS"] = (
        '--driver-java-options "-Djava.net.preferIPv4Stack=true" '
        f"{dependency_args}"
        "pyspark-shell"
    )
