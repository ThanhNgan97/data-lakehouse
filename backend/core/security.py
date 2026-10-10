import base64
import hashlib
from datetime import datetime, timedelta, timezone
from typing import Optional
from cryptography.fernet import Fernet, InvalidToken
from jose import jwt, JWTError
from passlib.context import CryptContext
from core.config import (
    SECRET_KEY,
    API_SOURCE_CREDENTIAL_KEY,
    ALGORITHM,
    ACCESS_TOKEN_EXPIRE_HOURS,
)

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


def _credential_cipher() -> Fernet:
    """Build the cipher used for persisted API-source credentials.

    Deployments should provide a stable API_SOURCE_CREDENTIAL_KEY. Falling
    back to SECRET_KEY keeps local development usable without storing bearer
    tokens as plaintext.
    """
    key_material = API_SOURCE_CREDENTIAL_KEY or SECRET_KEY
    key = base64.urlsafe_b64encode(hashlib.sha256(key_material.encode("utf-8")).digest())
    return Fernet(key)


def encrypt_secret(value: str) -> str:
    return _credential_cipher().encrypt(value.encode("utf-8")).decode("ascii")


def decrypt_secret(value: str) -> str:
    try:
        return _credential_cipher().decrypt(value.encode("ascii")).decode("utf-8")
    except (InvalidToken, ValueError, UnicodeError) as exc:
        raise ValueError("Không thể giải mã credential của nguồn API.") from exc

def verify_password(plain_password: str, hashed_password: str) -> bool:
    """Kiểm tra mật khẩu nhập vào có khớp với hash không."""
    try:
        return pwd_context.verify(plain_password, hashed_password)
    except Exception:
        return False

def get_password_hash(password: str) -> str:
    """Mã hóa mật khẩu bằng bcrypt."""
    return pwd_context.hash(password)

def create_access_token(data: dict, expires_delta: Optional[timedelta] = None) -> str:
    """Tạo JWT Access Token."""
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.now(timezone.utc) + expires_delta
    else:
        expire = datetime.now(timezone.utc) + timedelta(hours=ACCESS_TOKEN_EXPIRE_HOURS)
    
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt

def decode_access_token(token: str) -> Optional[dict]:
    """Giải mã và xác thực JWT token."""
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        return payload
    except JWTError:
        return None
