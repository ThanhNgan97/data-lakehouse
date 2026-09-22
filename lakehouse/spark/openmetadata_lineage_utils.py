# -*- coding: utf-8 -*-
"""
openmetadata_lineage_utils.py
------------------------------------------------------------
Đẩy Data Lineage THỦ CÔNG lên OpenMetadata sau khi pipeline Spark
merge dữ liệu thành công vào Nessie 'main'.

Lý do phải làm thủ công (không dùng auto-lineage của OpenMetadata):
  - Auto-lineage của OpenMetadata phân tích query log của Trino, nhưng
    pipeline của đồ án này ghi dữ liệu qua Spark + Nessie Extensions
    (để có branch/merge/quality-gate — xem nessie_catalog_utils.py),
    KHÔNG đi qua Trino, nên Trino không có query log để auto-lineage.
  - Bronze là file Parquet thô (không phải bảng Iceberg thật), Trino
    không nhìn thấy nó -> phải đăng ký thủ công 1 bảng "ảo" đại diện
    cho Bronze trong OpenMetadata thì mới vẽ được lineage tới nó.

Cài đặt:
    pip install openmetadata-ingestion

Biến môi trường cần thiết (đặt trước khi chạy pipeline):
    setx OPENMETADATA_JWT_TOKEN "<token lấy từ ingestion-bot>"
    (đóng mở lại PowerShell sau khi setx, hoặc dùng $env:OPENMETADATA_JWT_TOKEN="...")
------------------------------------------------------------
"""

import os
import logging

OPENMETADATA_AVAILABLE = False
_OPENMETADATA_IMPORT_ERROR = None

try:
    # pyrefly: ignore [missing-import]
    from metadata.generated.schema.entity.services.connections.metadata.openMetadataConnection import (
        OpenMetadataConnection,
    )
    from metadata.generated.schema.security.client.openMetadataJWTClientConfig import (
        OpenMetadataJWTClientConfig,
    )
    from metadata.ingestion.ometa.ometa_api import OpenMetadata

    from metadata.generated.schema.entity.data.database import Database
    from metadata.generated.schema.entity.data.databaseSchema import DatabaseSchema
    from metadata.generated.schema.entity.data.table import Table, Column, DataType
    from metadata.generated.schema.api.data.createDatabaseSchema import CreateDatabaseSchemaRequest
    from metadata.generated.schema.api.data.createTable import CreateTableRequest
    from metadata.generated.schema.api.lineage.addLineage import AddLineageRequest
    from metadata.generated.schema.type.entityLineage import EntitiesEdge, LineageDetails
    from metadata.generated.schema.type.entityReference import EntityReference
    OPENMETADATA_AVAILABLE = True
except ModuleNotFoundError as exc:
    OpenMetadataConnection = None
    OpenMetadataJWTClientConfig = None
    OpenMetadata = None
    Database = None
    DatabaseSchema = None
    Table = None
    Column = None
    DataType = None
    CreateDatabaseSchemaRequest = None
    CreateTableRequest = None
    AddLineageRequest = None
    EntitiesEdge = None
    LineageDetails = None
    EntityReference = None
    _OPENMETADATA_IMPORT_ERROR = exc

from env_config import OPENMETADATA_HOST_PORT, OPENMETADATA_JWT_TOKEN

# Phải khớp với tên bạn đã đặt khi tạo Database Service trên UI OpenMetadata
SERVICE_NAME = "lakehouse-trino"
DATABASE_NAME = "lakehouse"       # = tên catalog Trino
BRONZE_SCHEMA_NAME = "bronze"     # schema "ảo", chỉ để mô tả Bronze cho lineage
BRONZE_TABLE_NAME = "data_extracted_parquet"


def get_client():
    """Tạo client kết nối tới OpenMetadata. Raise lỗi rõ ràng nếu thiếu token hoặc package."""
    if not OPENMETADATA_AVAILABLE:
        raise RuntimeError(
            "OpenMetadata client chưa sẵn sàng: "
            f"{_OPENMETADATA_IMPORT_ERROR}. "
            "Cài đặt package openmetadata-ingestion để bật lineage, hoặc bỏ qua bước này."
        )
    if not OPENMETADATA_JWT_TOKEN:
        raise RuntimeError(
            "Thiếu biến môi trường OPENMETADATA_JWT_TOKEN. "
            "Lấy token tại OpenMetadata UI -> Settings -> Bots -> ingestion-bot -> Token."
        )
    server_config = OpenMetadataConnection(
        hostPort=OPENMETADATA_HOST_PORT,
        authProvider="openmetadata",
        securityConfig=OpenMetadataJWTClientConfig(jwtToken=OPENMETADATA_JWT_TOKEN),
    )
    client = OpenMetadata(server_config)
    if not client.health_check():
        raise RuntimeError(f"Không kết nối được OpenMetadata tại {OPENMETADATA_HOST_PORT}")
    return client


def ensure_bronze_table(client) -> str:
    """
    Đảm bảo tồn tại 1 bảng "ảo" đại diện cho Bronze (file Parquet thô) trong
    OpenMetadata, dưới CÙNG service 'lakehouse-trino' đã tạo qua UI (chỉ để
    có nơi neo lineage, bảng này KHÔNG thật sự query được qua Trino).
    Trả về fully qualified name (FQN) của bảng đó.
    """
    database_fqn = f"{SERVICE_NAME}.{DATABASE_NAME}"
    database_entity = client.get_by_name(entity=Database, fqn=database_fqn)
    if database_entity is None:
        raise RuntimeError(
            f"Không tìm thấy database '{database_fqn}' trong OpenMetadata. "
            "Hãy chạy Metadata Ingestion Agent cho service 'lakehouse-trino' trước "
            "(để OpenMetadata biết catalog 'lakehouse' tồn tại)."
        )

    schema_fqn = f"{database_fqn}.{BRONZE_SCHEMA_NAME}"
    schema_entity = client.get_by_name(entity=DatabaseSchema, fqn=schema_fqn)
    if schema_entity is None:
        print(f"🆕 Đang tạo schema ảo '{schema_fqn}' để neo lineage cho Bronze...")
        schema_entity = client.create_or_update(
            data=CreateDatabaseSchemaRequest(
                name=BRONZE_SCHEMA_NAME,
                database=database_entity.fullyQualifiedName,
                description="Schema ảo đại diện cho tầng Bronze (file Parquet thô trên MinIO, "
                             "không phải bảng Iceberg thật, chỉ dùng để neo lineage).",
            )
        )

    table_fqn = f"{schema_fqn}.{BRONZE_TABLE_NAME}"
    table_entity = client.get_by_name(entity=Table, fqn=table_fqn)
    if table_entity is None:
        print(f"🆕 Đang đăng ký bảng ảo '{table_fqn}' đại diện cho Bronze...")
        client.create_or_update(
            data=CreateTableRequest(
                name=BRONZE_TABLE_NAME,
                databaseSchema=schema_entity.fullyQualifiedName,
                description="File Parquet thô sinh ra từ spark_ingest_bronze.py "
                             "(trích xuất PDF/DOCX) tại "
                             "s3a://university-lakehouse/bronze/structured_data/data_extracted.parquet",
                columns=[
                    Column(name="ma_chi_tieu", dataType=DataType.STRING),
                    Column(name="nhom_don_vi", dataType=DataType.STRING),
                    Column(name="quy_danh_gia", dataType=DataType.STRING),
                    Column(name="ket_qua_he_thong", dataType=DataType.STRING),
                    Column(name="checksum_sha256", dataType=DataType.STRING),
                ],
            )
        )

    return table_fqn


def push_lineage(client, from_table_fqn: str, to_table_fqn: str, sql_query: str, description: str = ""):
    """
    Đẩy 1 cạnh lineage from_table -> to_table lên OpenMetadata.
    from_table_fqn / to_table_fqn dạng: "<service>.<database>.<schema>.<table>"
    """
    from_entity = client.get_by_name(entity=Table, fqn=from_table_fqn)
    to_entity = client.get_by_name(entity=Table, fqn=to_table_fqn)

    if from_entity is None:
        print(f"⚠️  Bỏ qua lineage: không tìm thấy bảng nguồn '{from_table_fqn}' trong OpenMetadata.")
        return
    if to_entity is None:
        print(f"⚠️  Bỏ qua lineage: không tìm thấy bảng đích '{to_table_fqn}' trong OpenMetadata "
              f"(hãy chạy lại Metadata Ingestion Agent để quét bảng mới nhất).")
        return

    request = AddLineageRequest(
        edge=EntitiesEdge(
            fromEntity=EntityReference(id=from_entity.id, type="table"),
            toEntity=EntityReference(id=to_entity.id, type="table"),
            lineageDetails=LineageDetails(sqlQuery=sql_query, description=description),
        )
    )
    client.add_lineage(data=request)
    print(f"🔗 Đã đẩy lineage: {from_table_fqn}  ->  {to_table_fqn}")


def push_lineage_safe(*args, **kwargs):
    """Bản bọc an toàn: lỗi đẩy lineage KHÔNG được làm sập pipeline chính."""
    try:
        push_lineage(*args, **kwargs)
    except Exception as e:
        logging.warning(f"⚠️  Đẩy lineage thất bại (không ảnh hưởng dữ liệu đã ghi): {e}")