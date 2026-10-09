"""Persistent checkpoint state for incremental API ingestion.

The store owns checkpoint persistence and compare-and-set semantics.
It deliberately does not interpret watermark payloads and has no knowledge of
HTTP, Spark, Bronze, Airflow, or dataset business semantics.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from urllib.parse import quote

from botocore.exceptions import ClientError

from bronze_writer import get_s3_client
from db_utils import get_db_connection
from env_config import MINIO_BUCKET_NAME


ConnectionFactory = Callable[[], Any]


class CheckpointStoreError(RuntimeError):
    """Base error for checkpoint persistence failures."""


class CheckpointConflictError(CheckpointStoreError):
    """The expected checkpoint version no longer matches persistent state."""


@dataclass(frozen=True)
class CheckpointState:
    dataset_id: str
    strategy_type: str
    checkpoint_payload: dict[str, Any]
    version: int
    last_batch_id: str | None


class PostgresCheckpointStore:
    """PostgreSQL checkpoint store using optimistic version-based CAS."""

    _STATE_COLUMNS = """
        dataset_id,
        strategy_type,
        checkpoint_payload,
        version,
        last_batch_id
    """

    def __init__(
        self,
        connection_factory: ConnectionFactory = get_db_connection,
    ) -> None:
        self._connection_factory = connection_factory

    @staticmethod
    def _require_nonempty(
        value: str,
        *,
        field_name: str,
    ) -> str:
        if not isinstance(value, str) or not value.strip():
            raise ValueError(
                f"{field_name} must be a non-empty string"
            )

        return value

    @staticmethod
    def _serialize_payload(
        payload: Mapping[str, Any],
    ) -> str:
        if not isinstance(payload, Mapping):
            raise ValueError(
                "checkpoint_payload must be an object"
            )

        materialized = dict(payload)

        try:
            return json.dumps(
                materialized,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            )
        except (TypeError, ValueError) as exc:
            raise ValueError(
                "checkpoint_payload must be JSON serializable"
            ) from exc

    @staticmethod
    def _row_to_state(
        row: Any,
    ) -> CheckpointState:
        if row is None or len(row) != 5:
            raise CheckpointStoreError(
                "checkpoint row has an unexpected shape"
            )

        (
            dataset_id,
            strategy_type,
            checkpoint_payload,
            version,
            last_batch_id,
        ) = row

        if (
            not isinstance(dataset_id, str)
            or not dataset_id.strip()
        ):
            raise CheckpointStoreError(
                "stored dataset_id is invalid"
            )

        if (
            not isinstance(strategy_type, str)
            or not strategy_type.strip()
        ):
            raise CheckpointStoreError(
                "stored strategy_type is invalid"
            )

        if not isinstance(checkpoint_payload, Mapping):
            raise CheckpointStoreError(
                "stored checkpoint_payload is not an object"
            )

        if (
            not isinstance(version, int)
            or isinstance(version, bool)
            or version < 1
        ):
            raise CheckpointStoreError(
                "stored checkpoint version is invalid"
            )

        if (
            last_batch_id is not None
            and not isinstance(last_batch_id, str)
        ):
            raise CheckpointStoreError(
                "stored last_batch_id is invalid"
            )

        return CheckpointState(
            dataset_id=dataset_id,
            strategy_type=strategy_type,
            checkpoint_payload=dict(checkpoint_payload),
            version=version,
            last_batch_id=last_batch_id,
        )

    def load(
        self,
        dataset_id: str,
    ) -> CheckpointState | None:
        dataset_id = self._require_nonempty(
            dataset_id,
            field_name="dataset_id",
        )

        sql = f"""
            SELECT {self._STATE_COLUMNS}
            FROM public.api_ingestion_checkpoints
            WHERE dataset_id = %s
        """

        connection = self._connection_factory()
        cursor = connection.cursor()

        try:
            cursor.execute(
                sql,
                (dataset_id,),
            )
            row = cursor.fetchone()

            if row is None:
                return None

            return self._row_to_state(row)
        finally:
            cursor.close()
            connection.close()

    def compare_and_set(
        self,
        *,
        dataset_id: str,
        strategy_type: str,
        expected_version: int | None,
        checkpoint_payload: Mapping[str, Any],
        batch_id: str,
    ) -> CheckpointState:
        dataset_id = self._require_nonempty(
            dataset_id,
            field_name="dataset_id",
        )
        strategy_type = self._require_nonempty(
            strategy_type,
            field_name="strategy_type",
        )
        batch_id = self._require_nonempty(
            batch_id,
            field_name="batch_id",
        )

        if (
            expected_version is not None
            and (
                not isinstance(expected_version, int)
                or isinstance(expected_version, bool)
                or expected_version < 1
            )
        ):
            raise ValueError(
                "expected_version must be None or an integer >= 1"
            )

        payload_json = self._serialize_payload(
            checkpoint_payload
        )

        if expected_version is None:
            sql = f"""
                INSERT INTO public.api_ingestion_checkpoints (
                    dataset_id,
                    strategy_type,
                    checkpoint_payload,
                    version,
                    last_batch_id
                )
                VALUES (%s, %s, %s::jsonb, 1, %s)
                ON CONFLICT (dataset_id) DO NOTHING
                RETURNING {self._STATE_COLUMNS}
            """

            params = (
                dataset_id,
                strategy_type,
                payload_json,
                batch_id,
            )
        else:
            sql = f"""
                UPDATE public.api_ingestion_checkpoints
                SET
                    checkpoint_payload = %s::jsonb,
                    version = version + 1,
                    last_batch_id = %s,
                    updated_at = NOW()
                WHERE dataset_id = %s
                  AND version = %s
                  AND strategy_type = %s
                RETURNING {self._STATE_COLUMNS}
            """

            params = (
                payload_json,
                batch_id,
                dataset_id,
                expected_version,
                strategy_type,
            )

        connection = self._connection_factory()
        cursor = connection.cursor()

        try:
            try:
                cursor.execute(
                    sql,
                    params,
                )
                row = cursor.fetchone()

                if row is None:
                    raise CheckpointConflictError(
                        "checkpoint compare-and-set conflict "
                        f"for dataset {dataset_id!r}"
                    )

                state = self._row_to_state(row)

                connection.commit()

                return state
            except Exception:
                connection.rollback()
                raise
        finally:
            cursor.close()
            connection.close()


class MinioCheckpointStore:
    """MinIO-backed checkpoint store for the serialized API Airflow DAG.

    Each registered dataset owns one small JSON object. The DAG is configured
    with ``max_active_runs=1``, so the read/version-check/write sequence is not
    executed concurrently by separate API pipeline runs.
    """

    def __init__(
        self,
        *,
        s3_client_factory: Callable[[], Any] = get_s3_client,
        bucket: str = MINIO_BUCKET_NAME,
        prefix: str = "metadata/api_ingestion_checkpoints",
    ) -> None:
        self._s3_client_factory = s3_client_factory
        self._bucket = bucket
        self._prefix = prefix.strip("/")

    @staticmethod
    def _require_nonempty(
        value: str,
        *,
        field_name: str,
    ) -> str:
        return PostgresCheckpointStore._require_nonempty(
            value,
            field_name=field_name,
        )

    @staticmethod
    def _serialize_payload(
        payload: Mapping[str, Any],
    ) -> str:
        return PostgresCheckpointStore._serialize_payload(payload)

    def _object_key(self, dataset_id: str) -> str:
        encoded_dataset = quote(dataset_id, safe="._-")
        return f"{self._prefix}/{encoded_dataset}.json"

    @staticmethod
    def _document_to_state(
        document: Any,
        *,
        expected_dataset_id: str,
    ) -> CheckpointState:
        if not isinstance(document, Mapping):
            raise CheckpointStoreError(
                "stored checkpoint document is not an object"
            )

        state = PostgresCheckpointStore._row_to_state((
            document.get("dataset_id"),
            document.get("strategy_type"),
            document.get("checkpoint_payload"),
            document.get("version"),
            document.get("last_batch_id"),
        ))

        if state.dataset_id != expected_dataset_id:
            raise CheckpointStoreError(
                "stored checkpoint dataset does not match object key"
            )

        return state

    def load(
        self,
        dataset_id: str,
    ) -> CheckpointState | None:
        dataset_id = self._require_nonempty(
            dataset_id,
            field_name="dataset_id",
        )
        client = self._s3_client_factory()

        try:
            response = client.get_object(
                Bucket=self._bucket,
                Key=self._object_key(dataset_id),
            )
        except ClientError as exc:
            error_code = str(
                exc.response.get("Error", {}).get("Code", "")
            )
            if error_code in {"NoSuchKey", "404", "NotFound"}:
                return None
            raise CheckpointStoreError(
                f"unable to read MinIO checkpoint for {dataset_id!r}"
            ) from exc

        try:
            raw_document = response["Body"].read()
            document = json.loads(raw_document.decode("utf-8"))
        except (KeyError, AttributeError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise CheckpointStoreError(
                f"invalid MinIO checkpoint document for {dataset_id!r}"
            ) from exc

        return self._document_to_state(
            document,
            expected_dataset_id=dataset_id,
        )

    def compare_and_set(
        self,
        *,
        dataset_id: str,
        strategy_type: str,
        expected_version: int | None,
        checkpoint_payload: Mapping[str, Any],
        batch_id: str,
    ) -> CheckpointState:
        dataset_id = self._require_nonempty(
            dataset_id,
            field_name="dataset_id",
        )
        strategy_type = self._require_nonempty(
            strategy_type,
            field_name="strategy_type",
        )
        batch_id = self._require_nonempty(
            batch_id,
            field_name="batch_id",
        )

        if expected_version is not None and (
            not isinstance(expected_version, int)
            or isinstance(expected_version, bool)
            or expected_version < 1
        ):
            raise ValueError(
                "expected_version must be None or an integer >= 1"
            )

        payload_json = self._serialize_payload(checkpoint_payload)
        current = self.load(dataset_id)
        current_version = current.version if current is not None else None

        if current_version != expected_version:
            raise CheckpointConflictError(
                "checkpoint compare-and-set conflict "
                f"for dataset {dataset_id!r}"
            )
        if current is not None and current.strategy_type != strategy_type:
            raise CheckpointConflictError(
                "checkpoint strategy conflict "
                f"for dataset {dataset_id!r}"
            )

        state = CheckpointState(
            dataset_id=dataset_id,
            strategy_type=strategy_type,
            checkpoint_payload=json.loads(payload_json),
            version=1 if current is None else current.version + 1,
            last_batch_id=batch_id,
        )
        document = {
            "dataset_id": state.dataset_id,
            "strategy_type": state.strategy_type,
            "checkpoint_payload": state.checkpoint_payload,
            "version": state.version,
            "last_batch_id": state.last_batch_id,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }

        client = self._s3_client_factory()
        client.put_object(
            Bucket=self._bucket,
            Key=self._object_key(dataset_id),
            Body=json.dumps(
                document,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8"),
            ContentType="application/json",
        )

        stored = self.load(dataset_id)
        if stored != state:
            raise CheckpointConflictError(
                "checkpoint verification conflict "
                f"for dataset {dataset_id!r}"
            )

        return stored
