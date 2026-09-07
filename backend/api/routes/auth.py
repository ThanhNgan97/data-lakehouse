from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordRequestForm
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError

from core.security import get_password_hash, verify_password, create_access_token
from db.database import get_db, get_user
from db.models import User
from api.dependencies import get_current_user, get_current_active_admin
from api.schemas.auth import UserLogin, UserCreate, UserResponse, Token, RoleUpdate

router = APIRouter()

@router.post("/login", response_model=Token, summary="Đăng nhập hệ thống (Form Data OAuth2)")
async def login(
    form_data: OAuth2PasswordRequestForm = Depends(),
    db: Session = Depends(get_db)
):
    """
    Xác thực tài khoản và mật khẩu, trả về JWT Access Token cùng vai trò (role).
    Đồng bộ hoàn toàn với OAuth2 Password Standard & Frontend React Form Data.
    """
    username = form_data.username
    password = form_data.password

    try:
        user = get_user(username=username, db=db)
    except SQLAlchemyError:
        # A database outage is not an authentication failure.  Returning a
        # response here also lets the client release its loading state.
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Không thể kết nối cơ sở dữ liệu. Vui lòng thử lại sau."
        )
    
    # Lấy thông tin hashed_password và role tùy thuộc vào loại đối tượng user (ORM hay Dict)
    if isinstance(user, User):
        user_hashed_pwd = user.hashed_password
        user_role = user.role
        is_active = user.is_active
    elif isinstance(user, dict):
        user_hashed_pwd = user.get("hashed_password")
        user_role = user.get("role", "user")
        is_active = user.get("is_active", True)
    else:
        user_hashed_pwd = None

    if not user or not user_hashed_pwd or not verify_password(password, user_hashed_pwd):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Sai tài khoản hoặc mật khẩu"
        )

    if not is_active:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Tài khoản hiện đang bị khóa"
        )

    access_token = create_access_token(data={"sub": username, "role": user_role})
    return {
        "access_token": access_token,
        "token_type": "bearer",
        "role": user_role,
        "username": username
    }


@router.post("/logout", summary="Đăng xuất khỏi hệ thống")
async def logout():
    """Đăng xuất tài khoản (Xóa phiên đăng nhập ở client)."""
    return {"message": "Đăng xuất thành công"}


@router.post("/create_user", response_model=UserResponse, status_code=status.HTTP_201_CREATED, summary="Tạo người dùng mới")
@router.post("/register", response_model=UserResponse, status_code=status.HTTP_201_CREATED, summary="Đăng ký tài khoản mới")
async def create_user(
    user_in: UserCreate,
    db: Session = Depends(get_db)
):
    """
    Tạo người dùng mới và lưu trữ vào cơ sở dữ liệu.
    """
    try:
        existing_user = db.query(User).filter(User.username == user_in.username).first()
        if existing_user:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Tài khoản đã tồn tại trên hệ thống"
            )
        
        if user_in.email:
            existing_email = db.query(User).filter(User.email == user_in.email).first()
            if existing_email:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Email đã được sử dụng"
                )

        new_user = User(
            username=user_in.username,
            email=user_in.email,
            full_name=user_in.full_name,
            hashed_password=get_password_hash(user_in.password),
            role=user_in.role or "user",
            is_active=True
        )
        db.add(new_user)
        db.commit()
        db.refresh(new_user)
        return new_user
    except HTTPException:
        raise
    except Exception as e:
        # If DB operation fails, return server error to surface the issue.
        raise HTTPException(status_code=500, detail=f"Lỗi tạo người dùng: {str(e)}")


@router.get("/me", response_model=UserResponse, summary="Lấy thông tin tài khoản đang đăng nhập")
async def get_me(current_user = Depends(get_current_user)):
    """Trả về chi tiết profile của người dùng đang đăng nhập."""
    if isinstance(current_user, User):
        return current_user
    return {
        "id": current_user.get("id"),
        "username": current_user.get("username"),
        "email": current_user.get("email"),
        "full_name": current_user.get("full_name"),
        "role": current_user.get("role", "user"),
        "is_active": current_user.get("is_active", True),
        "created_at": None
    }


