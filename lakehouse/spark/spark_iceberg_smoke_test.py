"""Verify Spark, Iceberg, Nessie, and MinIO integration on Kubernetes."""

from __future__ import annotations

from pyspark.sql import SparkSession


CATALOG = "lakehouse"
NAMESPACE = "spark_smoke"
TABLE = f"{CATALOG}.{NAMESPACE}.spark_k8s_smoke"


def main() -> None:
    spark = SparkSession.builder.appName("Spark_Iceberg_Nessie_Smoke_Test").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    try:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{NAMESPACE}")
        spark.sql(f"DROP TABLE IF EXISTS {TABLE}")
        spark.sql(
            f"""
            CREATE TABLE {TABLE} (
                id BIGINT,
                message STRING
            ) USING iceberg
            """
        )
        spark.sql(
            f"""
            INSERT INTO {TABLE} VALUES
                (1, 'spark-driver'),
                (2, 'spark-executor'),
                (3, 'iceberg-nessie-minio')
            """
        )

        rows = spark.sql(f"SELECT id, message FROM {TABLE} ORDER BY id").collect()
        actual = [(row.id, row.message) for row in rows]
        expected = [
            (1, "spark-driver"),
            (2, "spark-executor"),
            (3, "iceberg-nessie-minio"),
        ]
        if actual != expected:
            raise RuntimeError(f"Unexpected smoke-test rows: {actual!r}")

        print("LAKEHOUSE_SMOKE_TEST_OK")
        print(f"TABLE={TABLE}")
        print(f"ROW_COUNT={len(actual)}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
