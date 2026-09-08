from datetime import datetime, timezone

import logging

import mysql.connector

from fastapi import APIRouter, Depends, HTTPException, Response, status
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from api.dependencies import get_current_active_admin
from api.schemas.connectors import (
    ConnectorCreate,
    ConnectorResponse,
    ConnectorTestResponse,
    ConnectorUpdate,
)
from core.connector_crypto import decrypt_secret, encrypt_secret
from db.database import get_db
from db.models import DataConnector


router = APIRouter(
    prefix="/connectors",
    tags=["Data Connectors"],
)


def _get_user_id(current_user):
    """
    get_current_user() hiện có thể trả về:
    - User ORM object
    - dict fallback từ JWT

    Chỉ lấy ID khi thực sự có.
    """
    if hasattr(current_user, "id"):
        return current_user.id

    if isinstance(current_user, dict):
        return current_user.get("id")

    return None


@router.get(
    "",
    response_model=list[ConnectorResponse],
)
def list_connectors(
    db: Session = Depends(get_db),
    current_user=Depends(get_current_active_admin),
):
    """
    Admin xem danh sách Data Connectors.

    Credential mã hóa không được trả về frontend.
    """
    connectors = (
        db.query(DataConnector)
        .order_by(
            DataConnector.is_default.desc(),
            DataConnector.id.asc(),
        )
        .all()
    )

    return [connector.to_dict() for connector in connectors]


@router.post(
    "",
    response_model=ConnectorResponse,
    status_code=status.HTTP_201_CREATED,
)
def create_connector(
    payload: ConnectorCreate,
    db: Session = Depends(get_db),
    current_user=Depends(get_current_active_admin),
):
    """
    Admin tạo một MySQL Data Connector mới.

    Password plaintext chỉ tồn tại trong request,
    sau đó được mã hóa bằng Fernet trước khi lưu PostgreSQL.
    """

    connector_type = payload.connector_type.strip().upper()

    if connector_type != "MYSQL":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Hiện tại hệ thống chỉ hỗ trợ connector_type MYSQL.",
        )

    name = payload.name.strip()
    host = payload.host.strip()
    database_name = payload.database_name.strip()
    username = payload.username.strip()

    if not name:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Tên connector không được để trống.",
        )

    if not host:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="MySQL host không được để trống.",
        )

    if not database_name:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Tên database không được để trống.",
        )

    if not username:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="MySQL username không được để trống.",
        )

    existing = (
        db.query(DataConnector)
        .filter(DataConnector.name == name)
        .first()
    )

    if existing:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Data Connector '{name}' đã tồn tại.",
        )

    try:
        encrypted_password = encrypt_secret(payload.password)

        connector = DataConnector(
            name=name,
            connector_type=connector_type,
            host=host,
            port=payload.port,
            database_name=database_name,
            username=username,
            password_encrypted=encrypted_password,
            source_config=payload.source_config,
            schema_mapping=payload.schema_mapping,
            is_default=False,
            is_active=payload.is_active,
            last_test_status="not_tested",
            created_by=_get_user_id(current_user),
        )

        db.add(connector)
        db.commit()
        db.refresh(connector)

        logging.info(
            "Data Connector '%s' created successfully.",
            connector.name,
        )

        return connector.to_dict()

    except IntegrityError:
        db.rollback()

        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Tên Data Connector đã tồn tại.",
        )

    except HTTPException:
        db.rollback()
        raise

    except Exception as exc:
        db.rollback()

        logging.exception(
            "Failed to create Data Connector '%s'.",
            name,
        )

        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Không thể tạo Data Connector.",
        ) from exc

@router.put(
    "/{connector_id}",
    response_model=ConnectorResponse,
)
def update_connector(
    connector_id: int,
    payload: ConnectorUpdate,
    db: Session = Depends(get_db),
    current_user=Depends(get_current_active_admin),
):
    """
    Admin cập nhật Data Connector.

    Nếu request không gửi password mới,
    credential đã mã hóa hiện tại sẽ được giữ nguyên.
    """
    connector = (
        db.query(DataConnector)
        .filter(DataConnector.id == connector_id)
        .first()
    )

    if not connector:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Data Connector không tồn tại.",
        )

    update_data = payload.model_dump(exclude_unset=True)

    if "name" in update_data:
        name = update_data["name"].strip()

        if not name:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Tên connector không được để trống.",
            )

        duplicate = (
            db.query(DataConnector)
            .filter(
                DataConnector.name == name,
                DataConnector.id != connector_id,
            )
            .first()
        )

        if duplicate:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"Data Connector '{name}' đã tồn tại.",
            )

        connector.name = name

    if "host" in update_data:
        host = update_data["host"].strip()

        if not host:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="MySQL host không được để trống.",
            )

        connector.host = host

    if "port" in update_data:
        connector.port = update_data["port"]

    if "database_name" in update_data:
        database_name = update_data["database_name"].strip()

        if not database_name:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Tên database không được để trống.",
            )

        connector.database_name = database_name

    if "username" in update_data:
        username = update_data["username"].strip()

        if not username:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="MySQL username không được để trống.",
            )

        connector.username = username

    # Chỉ thay credential khi Admin thực sự gửi password mới.
    if "password" in update_data:
        connector.password_encrypted = encrypt_secret(
            update_data["password"]
        )

    if "source_config" in update_data:
        connector.source_config = update_data["source_config"]

    if "schema_mapping" in update_data:
        connector.schema_mapping = update_data["schema_mapping"]

    if "is_active" in update_data:
        connector.is_active = update_data["is_active"]

    try:
        db.commit()
        db.refresh(connector)

        logging.info(
            "Data Connector '%s' updated successfully.",
            connector.name,
        )

        return connector.to_dict()

    except IntegrityError:
        db.rollback()

        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Tên Data Connector đã tồn tại.",
        )

    except HTTPException:
        db.rollback()
        raise

    except Exception as exc:
        db.rollback()

        logging.exception(
            "Failed to update Data Connector ID %s.",
            connector_id,
        )

        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Không thể cập nhật Data Connector.",
        ) from exc

@router.post(
    "/{connector_id}/test",
    response_model=ConnectorTestResponse,
)
def test_connector(
    connector_id: int,
    db: Session = Depends(get_db),
    current_user=Depends(get_current_active_admin),
):
    """
    Admin kiểm tra kết nối tới MySQL.

    Password chỉ được giải mã trong runtime và không bao giờ
    được trả về API response.
    """
    connector = (
        db.query(DataConnector)
        .filter(DataConnector.id == connector_id)
        .first()
    )

    if not connector:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Data Connector không tồn tại.",
        )

    if connector.connector_type.upper() != "MYSQL":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Test Connection hiện chỉ hỗ trợ MySQL.",
        )

    mysql_connection = None
    cursor = None

    try:
        password = decrypt_secret(connector.password_encrypted)

        mysql_connection = mysql.connector.connect(
            host=connector.host,
            port=connector.port,
            user=connector.username,
            password=password,
            database=connector.database_name,
            connection_timeout=5,
        )

        cursor = mysql_connection.cursor()
        cursor.execute("SELECT 1")
        result = cursor.fetchone()

        if not result or result[0] != 1:
            raise RuntimeError(
                "MySQL health check không trả về kết quả hợp lệ."
            )

        connector.last_test_status = "success"
        connector.last_test_message = "Kết nối MySQL thành công."
        connector.last_tested_at = datetime.now(timezone.utc)

        db.commit()

        return {
            "success": True,
            "message": "Kết nối MySQL thành công.",
        }

    except Exception as exc:
        db.rollback()

        # Không đưa password/ciphertext vào message.
        safe_message = str(exc)
        if len(safe_message) > 400:
            safe_message = safe_message[:400]

        connector.last_test_status = "failed"
        connector.last_test_message = safe_message
        connector.last_tested_at = datetime.now(timezone.utc)

        try:
            db.commit()
        except Exception:
            db.rollback()

        logging.warning(
            "MySQL connection test failed for connector ID %s: %s",
            connector_id,
            safe_message,
        )

        return {
            "success": False,
            "message": safe_message,
        }

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


@router.delete(
    "/{connector_id}",
    status_code=status.HTTP_204_NO_CONTENT,
)
def delete_connector(
    connector_id: int,
    db: Session = Depends(get_db),
    current_user=Depends(get_current_active_admin),
):
    """
    Admin xóa custom connector.

    Default connector được bảo vệ và không thể bị xóa.
    """
    connector = (
        db.query(DataConnector)
        .filter(DataConnector.id == connector_id)
        .first()
    )

    if not connector:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Data Connector không tồn tại.",
        )

    if connector.is_default:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Không thể xóa Default Data Connector.",
        )

    try:
        connector_name = connector.name

        db.delete(connector)
        db.commit()

        logging.info(
            "Data Connector '%s' deleted successfully.",
            connector_name,
        )

        return Response(
            status_code=status.HTTP_204_NO_CONTENT,
        )

    except HTTPException:
        db.rollback()
        raise

    except Exception as exc:
        db.rollback()

        logging.exception(
            "Failed to delete Data Connector ID %s.",
            connector_id,
        )

        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Không thể xóa Data Connector.",
        ) from exc
