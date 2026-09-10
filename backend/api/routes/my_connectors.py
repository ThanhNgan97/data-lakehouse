from datetime import datetime, timezone
from urllib.parse import quote
import json
import logging
import os

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


def _required_env(name: str) -> str:
    """
    Lấy cấu hình runtime bắt buộc.
    Không hardcode dashboard ID hay credential Superset vào source code.
    """
    value = os.getenv(name, "").strip()
    if not value:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Thiếu cấu hình runtime {name}.",
        )
    return value


def _superset_runtime_config():
    return {
        "api_url": _required_env("SUPERSET_API_URL").rstrip("/"),
        "dashboard_id": _required_env("SUPERSET_DASHBOARD_ID"),
        "username": _required_env("SUPERSET_API_USERNAME"),
        "password": _required_env("SUPERSET_API_PASSWORD"),
        "filter_name": os.getenv(
            "SUPERSET_SOURCE_FILTER_NAME",
            "Nguồn MySQL",
        ).strip() or "Nguồn MySQL",
        "filter_column": os.getenv(
            "SUPERSET_SOURCE_FILTER_COLUMN",
            "source_connector_name",
        ).strip() or "source_connector_name",
    }


def _superset_access_headers(config: dict) -> dict:
    """
    Đăng nhập Superset server-to-server.
    Access token và password chỉ tồn tại trong backend memory, không trả về frontend.
    """
    login_url = f'{config["api_url"]}/api/v1/security/login'
    try:
        response = requests.post(
            login_url,
            json={
                "username": config["username"],
                "password": config["password"],
                "provider": "db",
                "refresh": True,
            },
            timeout=10,
        )
    except requests.RequestException as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Không thể kết nối tới Superset API.",
        ) from exc

    if response.status_code != 200:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Superset API không chấp nhận tài khoản dịch vụ.",
        )

    try:
        token = response.json().get("access_token")
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Superset trả về phản hồi đăng nhập không hợp lệ.",
        ) from exc

    if not token:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Superset không trả về access token.",
        )

    headers = {"Authorization": f"Bearer {token}"}

    # Các POST API của Superset có thể yêu cầu CSRF token.
    csrf_url = f'{config["api_url"]}/api/v1/security/csrf_token/'
    try:
        csrf_response = requests.get(
            csrf_url,
            headers=headers,
            timeout=10,
        )
        if csrf_response.status_code == 200:
            csrf_token = (csrf_response.json() or {}).get("result")
            if csrf_token:
                headers["X-CSRFToken"] = csrf_token
    except (requests.RequestException, ValueError):
        # Không fail ở đây: một số cấu hình Superset/JWT không yêu cầu CSRF cho API này.
        pass

    return headers


def _find_superset_source_filter(config: dict, headers: dict) -> str:
    """
    Tìm ID native filter từ metadata dashboard theo tên/cột.
    Không hardcode NATIVE_FILTER-... vì ID này thay đổi khi dashboard được tạo lại.
    """
    dashboard_url = (
        f'{config["api_url"]}/api/v1/dashboard/{config["dashboard_id"]}'
    )

    try:
        response = requests.get(
            dashboard_url,
            headers=headers,
            timeout=10,
        )
    except requests.RequestException as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Không thể đọc metadata KPI Dashboard từ Superset.",
        ) from exc

    if response.status_code != 200:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Không thể đọc cấu hình Native Filter của KPI Dashboard.",
        )

    try:
        payload = response.json()
        result = payload.get("result") or payload
        metadata = result.get("json_metadata") or {}
        if isinstance(metadata, str):
            metadata = json.loads(metadata)
    except (ValueError, TypeError, json.JSONDecodeError) as exc:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Metadata KPI Dashboard không hợp lệ.",
        ) from exc

    native_filters = metadata.get("native_filter_configuration") or []
    by_name = None

    for item in native_filters:
        filter_id = item.get("id")
        if not filter_id:
            continue

        if item.get("name") == config["filter_name"]:
            by_name = filter_id

        for target in item.get("targets") or []:
            column = target.get("column") or {}
            if column.get("name") == config["filter_column"]:
                return filter_id

    if by_name:
        return by_name

    raise HTTPException(
        status_code=status.HTTP_502_BAD_GATEWAY,
        detail=(
            f'Không tìm thấy Native Filter "{config["filter_name"]}" '
            f'cho cột "{config["filter_column"]}".'
        ),
    )


def _create_superset_filter_state(connector: DataConnector) -> dict:
    """
    Tạo temporary native filter state để iframe mở ngay dữ liệu của connector
    vừa đồng bộ thành công.
    """
    config = _superset_runtime_config()
    headers = _superset_access_headers(config)
    filter_id = _find_superset_source_filter(config, headers)

    # Cấu trúc DataMask mà Superset Native Filter dùng cho filter_select.
    data_mask = {
        filter_id: {
            "id": filter_id,
            "ownState": {},
            "extraFormData": {
                "filters": [
                    {
                        "col": config["filter_column"],
                        "op": "IN",
                        "val": [connector.name],
                    }
                ]
            },
            "filterState": {
                "label": connector.name,
                "value": [connector.name],
            },
        }
    }

    state_url = (
        f'{config["api_url"]}/api/v1/dashboard/'
        f'{config["dashboard_id"]}/filter_state'
    )

    try:
        response = requests.post(
            state_url,
            headers=headers,
            json={
                "value": json.dumps(
                    data_mask,
                    ensure_ascii=False,
                    separators=(",", ":"),
                )
            },
            timeout=10,
        )
    except requests.RequestException as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Không thể tạo trạng thái filter trên Superset.",
        ) from exc

    if response.status_code not in (200, 201):
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Superset không thể tạo trạng thái Native Filter.",
        )

    try:
        key = response.json().get("key")
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Superset trả về filter state không hợp lệ.",
        ) from exc

    if not key:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Superset không trả về native_filters_key.",
        )

    return {
        "dashboard_id": config["dashboard_id"],
        "native_filters_key": key,
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


@router.get("/{connector_id}/runs/{dag_run_id}")
def get_my_connector_sync_status(
    connector_id: int,
    dag_run_id: str,
    db: Session = Depends(get_db),
    current_user=Depends(get_current_user),
):
    """
    Trả trạng thái DAG run MySQL của connector thuộc user hiện tại.

    Backend xác minh ownership của connector và kiểm tra DAG run thực sự
    được trigger với đúng connector_id trước khi trả task states.
    """
    user_id = _require_user_id(current_user)
    connector = _owned(db, user_id, connector_id)

    encoded_run_id = quote(dag_run_id, safe="")
    run_url = (
        f"{AIRFLOW_WEBSERVER_URL}"
        f"/api/v1/dags/mysql_connector_sync/dagRuns/{encoded_run_id}"
    )
    tasks_url = f"{run_url}/taskInstances"

    try:
        run_response = requests.get(
            run_url,
            auth=("airflow", "airflow"),
            timeout=10,
        )

        if run_response.status_code == 404:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Không tìm thấy tiến trình đồng bộ này.",
            )

        if run_response.status_code != 200:
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail="Không thể lấy trạng thái DAG run từ Airflow.",
            )

        try:
            run_data = run_response.json()
        except ValueError as exc:
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail="Airflow trả về trạng thái DAG không hợp lệ.",
            ) from exc

        run_conf = run_data.get("conf") or {}
        run_connector_id = run_conf.get("connector_id")

        # Không cho user dùng connector của mình để đọc trạng thái DAG
        # thuộc connector khác.
        if str(run_connector_id) != str(connector.id):
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Không tìm thấy tiến trình đồng bộ này.",
            )

        tasks_response = requests.get(
            tasks_url,
            auth=("airflow", "airflow"),
            timeout=10,
        )

        if tasks_response.status_code != 200:
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail="Không thể lấy trạng thái task từ Airflow.",
            )

        try:
            tasks_data = tasks_response.json()
        except ValueError as exc:
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail="Airflow trả về trạng thái task không hợp lệ.",
            ) from exc

        tasks = []
        for item in tasks_data.get("task_instances", []):
            tasks.append(
                {
                    "task_id": item.get("task_id"),
                    "state": item.get("state"),
                    "start_date": item.get("start_date"),
                    "end_date": item.get("end_date"),
                    "duration": item.get("duration"),
                    "try_number": item.get("try_number"),
                }
            )

        return {
            "connector_id": connector.id,
            "connector_name": connector.name,
            "dag_id": "mysql_connector_sync",
            "dag_run_id": run_data.get("dag_run_id", dag_run_id),
            "state": run_data.get("state", "queued"),
            "execution_date": run_data.get("execution_date"),
            "start_date": run_data.get("start_date"),
            "end_date": run_data.get("end_date"),
            "tasks": tasks,
        }

    except HTTPException:
        raise
    except requests.RequestException as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Không thể kết nối tới Airflow.",
        ) from exc


@router.post("/{connector_id}/dashboard-filter")
def create_my_connector_dashboard_filter(
    connector_id: int,
    db: Session = Depends(get_db),
    current_user=Depends(get_current_user),
):
    """
    Sinh native_filters_key cho đúng connector thuộc user hiện tại.

    Endpoint này chỉ tạo trạng thái hiển thị Superset; pipeline dữ liệu đã hoàn thành
    độc lập trước đó. Credential/token Superset không được trả về frontend.
    """
    user_id = _require_user_id(current_user)
    connector = _owned(db, user_id, connector_id)

    if connector.connector_type.strip().upper() != "MYSQL":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Auto-filter hiện chỉ hỗ trợ MySQL Connector.",
        )

    filter_state = _create_superset_filter_state(connector)

    return {
        "success": True,
        "connector_id": connector.id,
        "connector_name": connector.name,
        **filter_state,
    }


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
