from minio import Minio
from core.config import MINIO_URL, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_BUCKET_NAME

# Khởi tạo MinIO client
minio_client = Minio(
    MINIO_URL,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False # Đang dùng localhost HTTP
)

def init_minio():
    # Đảm bảo Bucket luôn sẵn sàng
    if not minio_client.bucket_exists(MINIO_BUCKET_NAME):
        minio_client.make_bucket(MINIO_BUCKET_NAME)

# Khởi tạo ngay khi module được import
init_minio()
