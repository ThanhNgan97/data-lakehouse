"""Runtime probes for the standalone GX container.

The probes deliberately keep infrastructure checks separate from the local
Spark/GX check. A missing Trino service, Nessie branch, or Iceberg table is an
error rather than a silently skipped test.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any
from urllib.parse import quote
from urllib.request import urlopen


@dataclass(frozen=True)
class ProbeResult:
    probe: str
    status: str
    details: dict[str, Any]


def _required_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise ValueError(f"Required environment variable {name} is not set")
    return value


def probe_imports() -> ProbeResult:
    import great_expectations as gx
    import pyspark
    import sqlalchemy
    import trino
    from sqlalchemy.dialects import registry

    dialect = registry.load("trino")
    return ProbeResult(
        probe="imports",
        status="PASS",
        details={
            "great_expectations": gx.__version__,
            "pyspark": pyspark.__version__,
            "sqlalchemy": sqlalchemy.__version__,
            "trino": trino.__version__,
            "sqlalchemy_trino_dialect": f"{dialect.__module__}.{dialect.__name__}",
        },
    )


def _run_gx_dataframe(rows: list[dict[str, Any]]) -> bool:
    import great_expectations as gx

    context = gx.get_context(mode="ephemeral")
    source = context.data_sources.add_spark(name="phase_1_spark")
    asset = source.add_dataframe_asset(name="runtime_dataframe")
    batch = asset.add_batch_definition_whole_dataframe(name="whole_dataframe")
    suite = gx.ExpectationSuite(name="phase_1_runtime")
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(column="id")
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="progress", min_value=0, max_value=100
        )
    )
    context.suites.add(suite)
    definition = context.validation_definitions.add(
        gx.ValidationDefinition(name="phase_1_runtime", data=batch, suite=suite)
    )
    checkpoint = context.checkpoints.add(
        gx.Checkpoint(name="phase_1_runtime", validation_definitions=[definition])
    )

    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.master("local[1]")
        .appName("gx-phase-1-runtime")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    try:
        dataframe = spark.createDataFrame(rows)
        return bool(checkpoint.run(batch_parameters={"dataframe": dataframe}).success)
    finally:
        spark.stop()


def probe_spark() -> ProbeResult:
    valid_passed = _run_gx_dataframe(
        [{"id": 1, "progress": 80.0}, {"id": 2, "progress": 100.0}]
    )
    invalid_passed = _run_gx_dataframe(
        [{"id": 1, "progress": 101.0}, {"id": 2, "progress": 80.0}]
    )
    if not valid_passed or invalid_passed:
        raise RuntimeError(
            "GX Spark behavior mismatch: valid data must pass and invalid data must fail"
        )
    return ProbeResult(
        probe="spark",
        status="PASS",
        details={"valid_data": "PASS", "invalid_data": "FAIL_AS_EXPECTED"},
    )


def _trino_url() -> str:
    host = os.getenv("TRINO_HOST", "trino")
    port = os.getenv("TRINO_PORT", "8080")
    user = quote(os.getenv("TRINO_USER", "gx-validator"), safe="")
    catalog = os.getenv("TRINO_CATALOG", "lakehouse").strip("/")
    schema = os.getenv("TRINO_SCHEMA", "default").strip("/")
    return f"trino://{user}@{host}:{port}/{catalog}/{schema}"


def probe_trino() -> ProbeResult:
    from sqlalchemy import create_engine, text

    engine = create_engine(_trino_url(), pool_pre_ping=True)
    try:
        with engine.connect() as connection:
            value = connection.execute(text("SELECT 1")).scalar_one()
    finally:
        engine.dispose()
    if value != 1:
        raise RuntimeError(f"Unexpected Trino SELECT 1 result: {value!r}")
    return ProbeResult(
        probe="trino",
        status="PASS",
        details={"driver": engine.dialect.driver, "select_1": value},
    )


def _nessie_reference(nessie_uri: str, branch: str) -> dict[str, Any]:
    url = f"{nessie_uri.rstrip('/')}/trees/tree/{quote(branch, safe='')}"
    with urlopen(url, timeout=15) as response:
        payload = json.load(response)
    if payload.get("type") != "BRANCH" or payload.get("name") != branch:
        raise RuntimeError(f"Nessie did not resolve requested branch {branch!r}")
    return payload


def _iceberg_spark(branch: str):
    from pyspark.sql import SparkSession

    nessie_uri = os.getenv("NESSIE_API_URL", "http://nessie:19120/api/v1")
    warehouse = os.getenv(
        "ICEBERG_WAREHOUSE", "s3a://university-lakehouse/iceberg-warehouse"
    )
    s3_endpoint = os.getenv("S3_ENDPOINT", "http://s3g:9878")
    access_key = _required_env("S3_ACCESS_KEY")
    secret_key = _required_env("S3_SECRET_KEY")
    jars_directory = Path(
        os.getenv("SPARK_EXTRA_JARS_DIR", "/opt/spark/extra-jars")
    )
    jars = ",".join(str(path) for path in sorted(jars_directory.glob("*.jar")))
    if not jars:
        raise RuntimeError(f"No Spark runtime JARs found in {jars_directory}")
    return (
        SparkSession.builder.master(os.getenv("SPARK_MASTER", "local[1]"))
        .appName("gx-phase-1-nessie-branch")
        .config("spark.ui.enabled", "false")
        .config("spark.jars", jars)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,"
            "org.projectnessie.spark.extensions.NessieSparkSessionExtensions",
        )
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
        .config(
            "spark.sql.catalog.lakehouse.catalog-impl",
            "org.apache.iceberg.nessie.NessieCatalog",
        )
        .config("spark.sql.catalog.lakehouse.uri", nessie_uri)
        .config("spark.sql.catalog.lakehouse.ref", branch)
        .config("spark.sql.catalog.lakehouse.warehouse", warehouse)
        .config("spark.sql.catalog.lakehouse.cache-enabled", "false")
        .config("spark.hadoop.fs.s3a.endpoint", s3_endpoint)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.access.key", access_key)
        .config("spark.hadoop.fs.s3a.secret.key", secret_key)
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )


def probe_nessie_table() -> ProbeResult:
    nessie_uri = os.getenv("NESSIE_API_URL", "http://nessie:19120/api/v1")
    branch = _required_env("NESSIE_BRANCH")
    table = _required_env("ICEBERG_TABLE")
    reference = _nessie_reference(nessie_uri, branch)
    spark = _iceberg_spark(branch)
    try:
        row_count = spark.table(table).count()
        snapshots = spark.sql(
            f"SELECT snapshot_id FROM {table}.snapshots "
            "ORDER BY committed_at DESC LIMIT 1"
        ).collect()
    finally:
        spark.stop()
    snapshot_id = str(snapshots[0]["snapshot_id"]) if snapshots else None
    return ProbeResult(
        probe="nessie-table",
        status="PASS",
        details={
            "table": table,
            "nessie_branch": branch,
            "nessie_commit_hash": reference.get("hash"),
            "snapshot_id": snapshot_id,
            "row_count": row_count,
        },
    )


PROBES = {
    "imports": probe_imports,
    "spark": probe_spark,
    "trino": probe_trino,
    "nessie-table": probe_nessie_table,
}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("probe", choices=[*PROBES, "all"])
    args = parser.parse_args(argv)
    names = list(PROBES) if args.probe == "all" else [args.probe]
    results = []
    try:
        for name in names:
            results.append(asdict(PROBES[name]()))
    except Exception as exc:
        print(
            json.dumps(
                {"probe": name, "status": "FAIL", "error": str(exc)},
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 1
    print(json.dumps(results, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
