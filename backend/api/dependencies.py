# -*- coding: utf-8 -*-
from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from jose import JWTError, jwt
from sqlalchemy.orm import Session
from core.config import SECRET_KEY, ALGORITHM
from db.database import get_db

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="login")


async def get_current_user(
    token: str = Depends(oauth2_scheme),
    db: Session = Depends(get_db),
):
    """Giải mã JWT, trả về User ORM object hoặc dict nếu DB không khả dụng."""
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Token không hợp lệ hoặc đã hết hạn",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        role: str = payload.get("role", "user")
        if username is None:
            raise credentials_exception
    except JWTError:
        raise credentials_exception

    # Thử lấy từ DB trước
    try:
        from db.models import User
        user = db.query(User).filter(User.username == username).first()
        if user:
            return user
    except Exception:
        pass

    # Fallback: trả về dict với thông tin từ token (khi DB offline)
    return {"username": username, "role": role}


async def get_current_active_admin(
    current_user=Depends(get_current_user),
):
    """Chỉ cho phép admin. Raise 403 nếu không phải admin."""
    role = (
        current_user.role
        if hasattr(current_user, "role")
        else current_user.get("role", "user")
    )
    if role != "admin":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Chỉ admin mới có quyền thực hiện thao tác này.",
        )
    return current_user
