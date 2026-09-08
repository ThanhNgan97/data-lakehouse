# -*- coding: utf-8 -*-
"""
connector_runtime.py

Đọc cấu hình Data Connector từ PostgreSQL tại runtime.

Nguyên tắc bảo mật:
- Airflow/Spark chỉ nhận connector_id.
- Password được đọc dưới dạng ciphertext từ PostgreSQL.
- Fernet decrypt chỉ diễn ra trong memory.
- Không in password hoặc ciphertext ra log.
"""

import os
from dataclasses import dataclass
from typing import Any, Dict, Optional

import psycopg2
from cryptography.fernet import Fernet


@dataclass
class ConnectorRuntimeConfig:
    id: int
    name: str
    connector_type: str

    host: str
    port: int
    database_name: str
    username: str
    password: str

    source_config: Optional[Dict[str, Any]]
    schema_mapping: Optional[Dict[str, Any]]

    is_default: bool
    is_active: bool


def _get_fernet() -> Fernet:
    key = os.getenv("FERNET_KEY")

    if not key:
        raise RuntimeError(
            "FERNET_KEY không tồn tại trong Airflow runtime."
        )

    try:
        return Fernet(key.encode("utf-8"))
    except Exception as exc:
        raise RuntimeError(
            "FERNET_KEY không hợp lệ."
        ) from exc


def _resolve_postgres_runtime():
    """
    Backend chạy trên Windows có thể dùng:
        127.0.0.1:5433

    Nhưng Airflow chạy trong Docker network phải dùng:
        postgres:5432
    """
    host = os.getenv("PG_HOST", "postgres")
    port = int(os.getenv("PG_PORT", "5432"))

    if host in {"127.0.0.1", "localhost"}:
        host = "postgres"

    if host == "postgres" and port == 5433:
        port = 5432

    return host, port


def _decrypt_password(ciphertext: str) -> str:
    if not ciphertext:
        raise RuntimeError(
            "Connector không có credential đã mã hóa."
        )

    try:
        return (
            _get_fernet()
            .decrypt(ciphertext.encode("utf-8"))
            .decode("utf-8")
        )
    except Exception as exc:
        raise RuntimeError(
            "Không thể giải mã credential của connector."
        ) from exc


def load_connector(connector_id: int) -> ConnectorRuntimeConfig:
    """
    Đọc connector theo ID từ university_db.

    Không trả ciphertext ra ngoài module.
    """
    if connector_id <= 0:
        raise ValueError("connector_id phải lớn hơn 0.")

    pg_host, pg_port = _resolve_postgres_runtime()

    connection = None
    cursor = None

    try:
        connection = psycopg2.connect(
            host=pg_host,
            port=pg_port,
            user=os.getenv("PG_USER", "postgres"),
            password=os.getenv("PG_PASSWORD", "240203"),
            dbname=os.getenv("PG_DATABASE", "university_db"),
            connect_timeout=5,
        )

        cursor = connection.cursor()

        cursor.execute(
            """
            SELECT
                id,
                name,
                connector_type,
                host,
                port,
                database_name,
                username,
                password_encrypted,
                source_config,
                schema_mapping,
                is_default,
                is_active
            FROM data_connectors
            WHERE id = %s
            """,
            (connector_id,),
        )

        row = cursor.fetchone()

        if not row:
            raise RuntimeError(
                f"Không tìm thấy Data Connector ID {connector_id}."
            )

        (
            connector_db_id,
            name,
            connector_type,
            host,
            port,
            database_name,
            username,
            password_encrypted,
            source_config,
            schema_mapping,
            is_default,
            is_active,
        ) = row

        if not is_active:
            raise RuntimeError(
                f"Data Connector ID {connector_id} đang bị vô hiệu hóa."
            )

        password = _decrypt_password(password_encrypted)

        return ConnectorRuntimeConfig(
            id=connector_db_id,
            name=name,
            connector_type=connector_type,
            host=host,
            port=port,
            database_name=database_name,
            username=username,
            password=password,
            source_config=source_config,
            schema_mapping=schema_mapping,
            is_default=is_default,
            is_active=is_active,
        )

    finally:
        if cursor is not None:
            cursor.close()

        if connection is not None:
            connection.close()


def resolve_mysql_runtime_host(
    connector: ConnectorRuntimeConfig,
) -> str:
    """
    Chuyển hostname từ góc nhìn Backend sang góc nhìn Docker.

    Default connector:
        127.0.0.1 -> mysql

    Custom connector chạy trên máy Windows:
        127.0.0.1 -> host.docker.internal

    Remote MySQL:
        giữ nguyên hostname/IP.
    """
    host = connector.host.strip()

    if host not in {"127.0.0.1", "localhost"}:
        return host

    if connector.is_default:
        return "mysql"

    return "host.docker.internal"