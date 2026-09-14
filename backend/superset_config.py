# Cho phép nhúng Dashboard
FEATURE_FLAGS = {
    "EMBEDDED_SUPERSET": True
}


SESSION_COOKIE_NAME = "superset_session"
PERMANENT_SESSION_LIFETIME = 31536000  # 1 year in seconds
PUBLIC_ROLE_LIKE = "Gamma"
AUTH_ROLE_PUBLIC = "Public"
SESSION_REFRESH_EACH_REQUEST = True

# Mở CORS để React ở cổng 3000 có thể gọi API vào Superset ở cổng 8088
ENABLE_CORS = True
CORS_OPTIONS = {
    'supports_credentials': True,
    'allowed_origins': ['http://localhost:3000'],
    'always_send': True
}

# Tắt tạm bảo mật CSRF cho môi trường Dev local
WTF_CSRF_ENABLED = False