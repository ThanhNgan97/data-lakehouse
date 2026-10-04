# -*- coding: utf-8 -*-
"""CLI entry point for generic JSON-file or REST API ingestion into Bronze."""

from __future__ import annotations

import argparse
import os
import sys

from pyspark.sql import SparkSession

from api_ingestion import (
    ingest_http_api_to_bronze,
    ingest_json_file_to_bronze,
)


def main():
    parser = argparse.ArgumentParser(
        description="Generic API/JSON -> MinIO Bronze Parquet"
    )

    source = parser.add_mutually_exclusive_group(
        required=True
    )

    source.add_argument(
        "--input-json",
        help="Local JSON file representing response.json()",
    )

    source.add_argument(
        "--api-url",
        help="REST endpoint URL",
    )

    parser.add_argument(
        "--dataset",
        required=True,
        help="Dataset key registered in api_dataset_registry.py",
    )

    parser.add_argument(
        "--ingestion-mode",
        default="FULL_DEMO",
        choices=[
            "FULL_DEMO",
            "FULL",
            "INCREMENTAL",
        ],
    )

    parser.add_argument(
        "--batch-id",
        default=None,
    )

    parser.add_argument(
        "--expected-count",
        type=int,
        default=None,
    )

    parser.add_argument(
        "--updated-from",
        default=None,
        help=(
            "ISO-8601 lower watermark. "
            "For REST sources it is sent as updated_after."
        ),
    )

    # Kept for CLI compatibility; current CTU IOC mock contract
    # supports updated_after but not an upper watermark.
    parser.add_argument(
        "--updated-to",
        default=None,
    )

    parser.add_argument(
        "--api-key",
        default=os.getenv("CTU_IOC_API_KEY"),
        help=(
            "Optional bearer token. "
            "Defaults to CTU_IOC_API_KEY environment variable."
        ),
    )

    parser.add_argument(
        "--page-limit",
        type=int,
        default=500,
    )

    parser.add_argument(
        "--http-timeout",
        type=int,
        default=30,
    )

    args = parser.parse_args()

    sys.stdout.reconfigure(
        encoding="utf-8"
    )

    if (
        args.api_url
        and args.ingestion_mode == "INCREMENTAL"
        and not args.updated_from
    ):
        parser.error(
            "--updated-from is required for "
            "INCREMENTAL REST ingestion"
        )

    if args.api_url and args.updated_to:
        parser.error(
            "--updated-to is reserved; "
            "the current REST contract supports "
            "updated_after only"
        )

    spark = (
        SparkSession.builder
        .appName("generic-api-bronze-ingestion")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    try:
        if args.input_json:
            result = ingest_json_file_to_bronze(
                spark,
                args.input_json,
                dataset=args.dataset,
                ingestion_mode=args.ingestion_mode,
                batch_id=args.batch_id,
                expected_count=args.expected_count,
            )

        else:
            result = ingest_http_api_to_bronze(
                spark,
                args.api_url,
                dataset=args.dataset,
                ingestion_mode=args.ingestion_mode,
                batch_id=args.batch_id,
                expected_count=args.expected_count,
                updated_after=args.updated_from,
                api_key=args.api_key,
                page_limit=args.page_limit,
                timeout_seconds=args.http_timeout,
            )

        print(
            "\n=== API BRONZE INGEST COMPLETED ==="
        )
        print(
            f"batch_id={result.batch_id}"
        )
        print(
            f"object_key={result.object_key}"
        )
        print(
            f"records={result.readback_count}"
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()