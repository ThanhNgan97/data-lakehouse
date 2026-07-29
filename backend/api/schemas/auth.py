from typing import Optional
from datetime import datetime
from pydantic import BaseModel, EmailStr, Field

# Schema Yêu cầu Đăng nhập dạng JSON
class UserLogin(BaseModel):
    username: str = Field(..., description="Tên đăng nhập")
    password: str = Field(..., description="Mật khẩu")

# Schema Tạo người dùng mới
class UserCreate(BaseModel):
    username: str = Field(..., min_length=3, max_length=50, description="Tên tài khoản")
    password: str = Field(..., min_length=6, description="Mật khẩu (tối thiểu 6 ký tự)")
    email: Optional[EmailStr] = Field(None, description="Email người dùng")
    full_name: Optional[str] = Field(None, description="Họ và tên")
    role: Optional[str] = Field("user", description="Vai trò: 'admin' hoặc 'user'")

# Schema Cập nhật Vai trò
class RoleUpdate(BaseModel):
    role: str = Field(..., description="Vai trò mới: 'admin' hoặc 'user'")

# Schema Đổi mật khẩu
class PasswordChange(BaseModel):
    old_password: str = Field(..., description="Mật khẩu cũ")
    new_password: str = Field(..., min_length=6, description="Mật khẩu mới")

# Schema Trả về Thông tin người dùng
class UserResponse(BaseModel):
    id: Optional[int] = None
    username: str
    email: Optional[str] = None
    full_name: Optional[str] = None
    role: str
    is_active: bool
    created_at: Optional[datetime] = None

    class Config:
        from_attributes = True

# Schema Trả về Token Đăng nhập
class Token(BaseModel):
    access_token: str
    token_type: str = "bearer"
    role: str
    username: str

# Payload của JWT Token
class TokenData(BaseModel):
    username: Optional[str] = None
    role: Optional[str] = None
