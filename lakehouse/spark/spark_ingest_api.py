# -*- coding: utf-8 -*-
"""CLI entry point for generic local/mock JSON API ingestion into Bronze only."""

from __future__ import annotations

import argparse
import sys

from pyspark.sql import SparkSession

from api_ingestion import ingest_json_file_to_bronze


def main():
    parser = argparse.ArgumentParser(description="Generic API JSON -> MinIO Bronze Parquet")
    parser.add_argument("--input-json", required=True, help="Local JSON file representing response.json()")
    parser.add_argument("--dataset", required=True, help="Dataset key registered in api_dataset_registry.py")
    parser.add_argument("--ingestion-mode", default="FULL_DEMO", choices=["FULL_DEMO", "FULL", "INCREMENTAL"])
    parser.add_argument("--batch-id", default=None)
    parser.add_argument("--expected-count", type=int, default=None)
    # Reserved now so HTTP incremental can pass the same arguments later without redesigning the engine.
    parser.add_argument("--updated-from", default=None)
    parser.add_argument("--updated-to", default=None)
    args = parser.parse_args()

    sys.stdout.reconfigure(encoding="utf-8")

    spark = (
        SparkSession.builder
        .appName("generic-api-bronze-ingestion")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        result = ingest_json_file_to_bronze(
            spark,
            args.input_json,
            dataset=args.dataset,
            ingestion_mode=args.ingestion_mode,
            batch_id=args.batch_id,
            expected_count=args.expected_count,
        )
        print("\n=== API BRONZE INGEST COMPLETED ===")
        print(f"batch_id={result.batch_id}")
        print(f"object_key={result.object_key}")
        print(f"records={result.readback_count}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
