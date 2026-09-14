ENABLE_CORS = True
CORS_OPTIONS = {
    'supports_credentials': True,
    'allow_headers': ['*'],
    'resources': ['*'],
    'origins': ['http://localhost:5173', 'http://localhost:3000', 'http://127.0.0.1:5173', 'http://127.0.0.1:3000']
}

TALISMAN_ENABLED = False
WTF_CSRF_ENABLED = False

HTTP_HEADERS = {'X-Frame-Options': 'ALLOWALL'}
SUPERSET_WEBSERVER_HTTP_HEADERS = {'X-Frame-Options': 'ALLOWALL'}

# Allow embedding Superset & Public Dashboard Access without Login
FEATURE_FLAGS = {
    "EMBEDDED_SUPERSET": True
}
PUBLIC_ROLE_LIKE = "Gamma"
AUTH_ROLE_PUBLIC = "Public"

# Đổi tên Cookie để không bị đè với Airflow (cả 2 đều dùng tên 'session' mặc định trên localhost)
SESSION_COOKIE_NAME = "superset_session"

# Kéo dài session (1 năm)
PERMANENT_SESSION_LIFETIME = 31536000  # 1 year in seconds
SESSION_REFRESH_EACH_REQUEST = True
