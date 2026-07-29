# -*- coding: utf-8 -*-
"""
api/routes/users.py
------------------------------------------------------------
CRUD User Management — chỉ dành cho admin.
Dùng SQLAlchemy ORM + PostgreSQL (nhất quán với auth.py mới).
------------------------------------------------------------
"""

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from api.dependencies import get_current_user, get_current_active_admin
from api.schemas.auth import UserCreate, UserResponse, PasswordChange, RoleUpdate
from core.security import get_password_hash, verify_password
from db.database import get_db
from db.models import User

router = APIRouter()


@router.get("/users", response_model=list[UserResponse])
async def list_users(
    db: Session = Depends(get_db),
    current_user=Depends(get_current_active_admin),
):
    """Danh sách tất cả user — admin only."""
    return db.query(User).all()


@router.post("/users", response_model=UserResponse, status_code=201)
async def create_user(
    body: UserCreate,
    db: Session = Depends(get_db),
    current_user=Depends(get_current_active_admin),
):
    """Tạo user mới — admin only."""
    if db.query(User).filter(User.username == body.username).first():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Tài khoản '{body.username}' đã tồn tại.",
        )
    if body.email and db.query(User).filter(User.email == body.email).first():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Email này đã được sử dụng.",
        )
    new_user = User(
        username=body.username,
        email=body.email,
        full_name=body.full_name,
        hashed_password=get_password_hash(body.password),
        role=body.role or "user",
        is_active=True,
    )
    db.add(new_user)
    db.commit()
    db.refresh(new_user)
    return new_user


@router.delete("/users/{user_id}", status_code=200)
async def delete_user(
    user_id: int,
    db: Session = Depends(get_db),
    current_user=Depends(get_current_active_admin),
):
    """Xóa user — admin only. Không thể tự xóa chính mình."""
    target = db.query(User).filter(User.id == user_id).first()
    if not target:
        raise HTTPException(status_code=404, detail="Không tìm thấy tài khoản.")
    # Lấy username của admin đang đăng nhập
    current_username = (
        current_user.username if hasattr(current_user, "username") else current_user.get("username")
    )
    if target.username == current_username:
        raise HTTPException(status_code=400, detail="Không thể tự xóa tài khoản đang đăng nhập.")
    db.delete(target)
    db.commit()
    return {"message": f"Đã xóa tài khoản '{target.username}'."}


@router.put("/users/{user_id}/role", status_code=200)
async def update_role(
    user_id: int,
    body: RoleUpdate,
    db: Session = Depends(get_db),
    current_user=Depends(get_current_active_admin),
):
    """Cập nhật role cho user — admin only."""
    if body.role not in ("admin", "user"):
        raise HTTPException(status_code=400, detail="Role không hợp lệ. Chỉ chấp nhận 'admin' hoặc 'user'.")
    target = db.query(User).filter(User.id == user_id).first()
    if not target:
        raise HTTPException(status_code=404, detail="Không tìm thấy tài khoản.")
    target.role = body.role
    db.commit()
    return {"message": f"Đã cập nhật quyền của '{target.username}' thành '{body.role}'."}


@router.put("/users/{user_id}/toggle-active", status_code=200)
async def toggle_active(
    user_id: int,
    db: Session = Depends(get_db),
    current_user=Depends(get_current_active_admin),
):
    """Khoá / mở khoá tài khoản — admin only."""
    target = db.query(User).filter(User.id == user_id).first()
    if not target:
        raise HTTPException(status_code=404, detail="Không tìm thấy tài khoản.")
    target.is_active = not target.is_active
    db.commit()
    action = "mở khoá" if target.is_active else "khoá"
    return {"message": f"Đã {action} tài khoản '{target.username}'.", "is_active": target.is_active}


@router.put("/users/{user_id}/password", status_code=200)
async def change_password(
    user_id: int,
    body: PasswordChange,
    db: Session = Depends(get_db),
    current_user=Depends(get_current_user),
):
    """Đổi mật khẩu — admin có thể đổi bất kỳ ai, user chỉ đổi của mình."""
    target = db.query(User).filter(User.id == user_id).first()
    if not target:
        raise HTTPException(status_code=404, detail="Không tìm thấy tài khoản.")
    current_role = (
        current_user.role if hasattr(current_user, "role") else current_user.get("role")
    )
    current_uname = (
        current_user.username if hasattr(current_user, "username") else current_user.get("username")
    )
    if current_role != "admin" and target.username != current_uname:
        raise HTTPException(status_code=403, detail="Không có quyền đổi mật khẩu tài khoản khác.")
    target.hashed_password = get_password_hash(body.new_password)
    db.commit()
    return {"message": f"Đã đổi mật khẩu cho tài khoản '{target.username}'."}
