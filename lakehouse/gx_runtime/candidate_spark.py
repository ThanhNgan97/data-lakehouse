"""Lazy Spark runtime that exposes one isolated catalog per Nessie branch."""

from __future__ import annotations

import glob
import hashlib
import os
from threading import Lock
from typing import Any


class CandidateSparkSessions:
    def __init__(self) -> None:
        self._base: Any | None = None
        self._lock = Lock()

    def close(self) -> None:
        with self._lock:
            if self._base is not None:
                self._base.stop()
                self._base = None

    def for_branch(self, branch: str) -> tuple[Any, str]:
        safe_branch = str(branch or "").strip()
        if not safe_branch or safe_branch == "main":
            raise ValueError("Candidate Spark catalog requires a non-main branch")
        alias = "gx_" + hashlib.sha256(safe_branch.encode("utf-8")).hexdigest()[:16]
        with self._lock:
            base = self._base_session()
            session = base.newSession()
            prefix = f"spark.sql.catalog.{alias}"
            settings = {
                prefix: "org.apache.iceberg.spark.SparkCatalog",
                f"{prefix}.catalog-impl": "org.apache.iceberg.nessie.NessieCatalog",
                f"{prefix}.uri": os.getenv("NESSIE_API_URL", "http://nessie:19120/api/v1"),
                f"{prefix}.ref": safe_branch,
                f"{prefix}.warehouse": os.getenv(
                    "ICEBERG_WAREHOUSE", "s3a://university-lakehouse/iceberg-warehouse"
                ),
                f"{prefix}.cache-enabled": "false",
            }
            for key, value in settings.items():
                session.conf.set(key, value)
            return session, alias

    def _base_session(self) -> Any:
        if self._base is not None:
            return self._base
        from pyspark.sql import SparkSession

        jars = ",".join(sorted(glob.glob("/opt/spark/extra-jars/*.jar")))
        builder = (
            SparkSession.builder
            .master(os.getenv("GX_SPARK_MASTER", "local[2]"))
            .appName("gx-candidate-validator")
            .config("spark.sql.extensions", (
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,"
                "org.projectnessie.spark.extensions.NessieSparkSessionExtensions"
            ))
            .config("spark.sql.catalogImplementation", "in-memory")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.endpoint", os.getenv("S3_ENDPOINT", "http://s3g:9878"))
            .config("spark.hadoop.fs.s3a.access.key", os.getenv("S3_ACCESS_KEY", "lakehouse"))
            .config("spark.hadoop.fs.s3a.secret.key", os.getenv("S3_SECRET_KEY", "lakehouse-secret"))
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.ui.enabled", "false")
        )
        if jars:
            builder = builder.config("spark.jars", jars)
        self._base = builder.getOrCreate()
        self._base.sparkContext.setLogLevel("WARN")
        return self._base
