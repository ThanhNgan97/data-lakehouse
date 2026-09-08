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

# Allow embedding Superset
FEATURE_FLAGS = {
    "EMBEDDED_SUPERSET": True
}

# Kéo dài session để không bị logout sớm
PERMANENT_SESSION_LIFETIME = 604800* 2  # 7 days in seconds
SESSION_COOKIE_SAMESITE = 'None'
SESSION_COOKIE_SECURE = True
SESSION_REFRESH_EACH_REQUEST = True
