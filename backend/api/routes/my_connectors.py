from datetime import datetime, timezone
import logging

import mysql.connector
import requests
from fastapi import APIRouter, Depends, HTTPException, Response, status
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from api.dependencies import get_current_user
from api.schemas.connectors import (
    ConnectorCreate,
    ConnectorResponse,
    ConnectorTestResponse,
    ConnectorUpdate,
)
from core.config import AIRFLOW_WEBSERVER_URL
from core.connector_crypto import decrypt_secret, encrypt_secret
from db.database import get_db
from db.models import DataConnector

router = APIRouter(prefix="/my-connectors", tags=["My Data Connectors"])

DEFAULT_SOURCE_CONFIG = {
    "mode": "cusc_kpi_operational",
    "tables": ["don_vi", "muc_tieu_kpi", "ket_qua_danh_gia"],
    "primary_table": "ket_qua_danh_gia",
}


def _require_user_id(current_user) -> int:
    if hasattr(current_user, "id") and current_user.id is not None:
        return int(current_user.id)
    if isinstance(current_user, dict) and current_user.get("id") is not None:
        return int(current_user["id"])
    raise HTTPException(
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        detail="Không thể xác minh quyền sở hữu Data Connector.",
    )


def _owned(db: Session, user_id: int, connector_id: int) -> DataConnector:
    connector = (
        db.query(DataConnector)
        .filter(
            DataConnector.id == connector_id,
            DataConnector.created_by == user_id,
            DataConnector.is_default.is_(False),
        )
        .first()
    )
    if not connector:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Data Connector không tồn tại hoặc bạn không có quyền truy cập.",
        )
    return connector


def _validate_mysql_fields(payload):
    connector_type = payload.connector_type.strip().upper()
    if connector_type != "MYSQL":
        raise HTTPException(400, "Hiện tại hệ thống chỉ hỗ trợ MYSQL.")
    name = payload.name.strip()
    host = payload.host.strip()
    database_name = payload.database_name.strip()
    username = payload.username.strip()
    if not name:
        raise HTTPException(400, "Tên connector không được để trống.")
    if not host:
        raise HTTPException(400, "MySQL host không được để trống.")
    if not database_name:
        raise HTTPException(400, "Tên database không được để trống.")
    if not username:
        raise HTTPException(400, "MySQL username không được để trống.")
    return connector_type, name, host, database_name, username


@router.get("", response_model=list[ConnectorResponse])
def list_my_connectors(
    db: Session = Depends(get_db),
    current_user=Depends(get_current_user),
):
    user_id = _require_user_id(current_user)
    rows = (
        db.query(DataConnector)
        .filter(
            DataConnector.created_by == user_id,
            DataConnector.is_default.is_(False),
        )
        .order_by(DataConnector.id.asc())
        .all()
    )
    return [row.to_dict() for row in rows]


@router.post("", response_model=ConnectorResponse, status_code=201)
def create_my_connector(
    payload: ConnectorCreate,
    db: Session = Depends(get_db),
    current_user=Depends(get_current_user),
):
    user_id = _require_user_id(current_user)
    connector_type, name, host, database_name, username = _validate_mysql_fields(payload)

    # Model hiện tại vẫn unique toàn cục theo name; Day 5B có thể đổi sang unique theo owner.
    if db.query(DataConnector).filter(DataConnector.name == name).first():
        raise HTTPException(409, "Tên Data Connector này đã được sử dụng.")

    try:
        connector = DataConnector(
            name=name,
            connector_type=connector_type,
            host=host,
            port=payload.port,
            database_name=database_name,
            username=username,
            password_encrypted=encrypt_secret(payload.password),
            source_config=payload.source_config or dict(DEFAULT_SOURCE_CONFIG),
            schema_mapping=payload.schema_mapping,
            is_default=False,
            is_active=payload.is_active,
            last_test_status="not_tested",
            created_by=user_id,
        )
        db.add(connector)
        db.commit()
        db.refresh(connector)
        logging.info("User connector created: id=%s owner=%s", connector.id, user_id)
        return connector.to_dict()
    except IntegrityError:
        db.rollback()
        raise HTTPException(409, "Tên Data Connector này chưa khả dụng.")
    except HTTPException:
        db.rollback()
        raise
    except Exception as exc:
        db.rollback()
        logging.exception("Create user connector failed for owner=%s", user_id)
        raise HTTPException(500, "Không thể tạo Data Connector.") from exc


@router.put("/{connector_id}", response_model=ConnectorResponse)
def update_my_connector(
    connector_id: int,
    payload: ConnectorUpdate,
    db: Session = Depends(get_db),
    current_user=Depends(get_current_user),
):
    user_id = _require_user_id(current_user)
    connector = _owned(db, user_id, connector_id)
    data = payload.model_dump(exclude_unset=True)

    if "name" in data:
        name = data["name"].strip()
        if not name:
            raise HTTPException(400, "Tên connector không được để trống.")
        duplicate = (
            db.query(DataConnector)
            .filter(DataConnector.name == name, DataConnector.id != connector_id)
            .first()
        )
        if duplicate:
            raise HTTPException(409, "Tên Data Connector này đã được sử dụng.")
        connector.name = name

    for field, label in (
        ("host", "MySQL host"),
        ("database_name", "Tên database"),
        ("username", "MySQL username"),
    ):
        if field in data:
            value = data[field].strip()
            if not value:
                raise HTTPException(400, f"{label} không được để trống.")
            setattr(connector, field, value)

    if "port" in data:
        connector.port = data["port"]
    if "password" in data:
        connector.password_encrypted = encrypt_secret(data["password"])
    if "source_config" in data:
        connector.source_config = data["source_config"]
    if "schema_mapping" in data:
        connector.schema_mapping = data["schema_mapping"]
    if "is_active" in data:
        connector.is_active = data["is_active"]

    try:
        db.commit()
        db.refresh(connector)
        return connector.to_dict()
    except IntegrityError:
        db.rollback()
        raise HTTPException(409, "Tên Data Connector này chưa khả dụng.")
    except Exception as exc:
        db.rollback()
        raise HTTPException(500, "Không thể cập nhật Data Connector.") from exc


@router.post("/{connector_id}/test", response_model=ConnectorTestResponse)
def test_my_connector(
    connector_id: int,
    db: Session = Depends(get_db),
    current_user=Depends(get_current_user),
):
    user_id = _require_user_id(current_user)
    connector = _owned(db, user_id, connector_id)
    mysql_connection = None
    cursor = None

    try:
        mysql_connection = mysql.connector.connect(
            host=connector.host,
            port=connector.port,
            user=connector.username,
            password=decrypt_secret(connector.password_encrypted),
            database=connector.database_name,
            connection_timeout=5,
        )
        cursor = mysql_connection.cursor()
        cursor.execute("SELECT 1")
        result = cursor.fetchone()
        if not result or result[0] != 1:
            raise RuntimeError("MySQL health check không hợp lệ.")

        connector.last_test_status = "success"
        connector.last_test_message = "Kết nối MySQL thành công."
        connector.last_tested_at = datetime.now(timezone.utc)
        db.commit()
        return {"success": True, "message": "Kết nối MySQL thành công."}
    except Exception as exc:
        db.rollback()
        safe_message = str(exc)[:400]
        connector.last_test_status = "failed"
        connector.last_test_message = safe_message
        connector.last_tested_at = datetime.now(timezone.utc)
        try:
            db.commit()
        except Exception:
            db.rollback()
        return {"success": False, "message": safe_message}
    finally:
        if cursor is not None:
            try:
                cursor.close()
            except Exception:
                pass
        if mysql_connection is not None:
            try:
                if mysql_connection.is_connected():
                    mysql_connection.close()
            except Exception:
                pass


@router.post("/{connector_id}/sync")
def sync_my_connector(
    connector_id: int,
    db: Session = Depends(get_db),
    current_user=Depends(get_current_user),
):
    user_id = _require_user_id(current_user)
    connector = _owned(db, user_id, connector_id)
    if not connector.is_active:
        raise HTTPException(400, "Data Connector đang bị vô hiệu hóa.")

    airflow_url = f"{AIRFLOW_WEBSERVER_URL}/api/v1/dags/mysql_connector_sync/dagRuns"
    try:
        response = requests.post(
            airflow_url,
            json={"conf": {"connector_id": connector.id}},
            auth=("airflow", "airflow"),
            timeout=10,
        )
        if response.status_code not in (200, 201):
            connector.last_sync_status = "trigger_failed"
            connector.last_sync_at = datetime.now(timezone.utc)
            db.commit()
            raise HTTPException(502, "Airflow không chấp nhận yêu cầu đồng bộ.")

        try:
            data = response.json()
        except ValueError:
            connector.last_sync_status = "trigger_failed"
            connector.last_sync_at = datetime.now(timezone.utc)
            db.commit()
            raise HTTPException(502, "Airflow trả về phản hồi không hợp lệ.")

        connector.last_sync_status = "triggered"
        connector.last_sync_at = datetime.now(timezone.utc)
        db.commit()
        return {
            "success": True,
            "message": "Đã kích hoạt đồng bộ MySQL.",
            "connector_id": connector.id,
            "dag_id": "mysql_connector_sync",
            "dag_run_id": data.get("dag_run_id"),
            "state": data.get("state", "queued"),
        }
    except HTTPException:
        raise
    except requests.RequestException as exc:
        db.rollback()
        connector.last_sync_status = "airflow_unreachable"
        connector.last_sync_at = datetime.now(timezone.utc)
        try:
            db.commit()
        except Exception:
            db.rollback()
        raise HTTPException(503, "Không thể kết nối tới Airflow.") from exc


@router.delete("/{connector_id}", status_code=204)
def delete_my_connector(
    connector_id: int,
    db: Session = Depends(get_db),
    current_user=Depends(get_current_user),
):
    user_id = _require_user_id(current_user)
    connector = _owned(db, user_id, connector_id)
    try:
        db.delete(connector)
        db.commit()
        return Response(status_code=status.HTTP_204_NO_CONTENT)
    except Exception as exc:
        db.rollback()
        raise HTTPException(500, "Không thể xóa Data Connector.") from exc
