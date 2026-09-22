ENABLE_CORS = True
CORS_OPTIONS = {
    'supports_credentials': True,
    'allow_headers': ['*'],
    'resources': ['*'],
    'origins': ['*']
}

TALISMAN_ENABLED = False
WTF_CSRF_ENABLED = False

HTTP_HEADERS = {'X-Frame-Options': 'ALLOWALL'}
SUPERSET_WEBSERVER_HTTP_HEADERS = {'X-Frame-Options': 'ALLOWALL'}

# Allow embedding Superset
SUPERSET_FEATURE_EMBEDDED_SUPERSET = True

# Kéo dài session để không bị logout sớm
PERMANENT_SESSION_LIFETIME = 604800* 2  # 7 days in seconds
SESSION_COOKIE_SAMESITE = None
SESSION_COOKIE_SECURE = False
SESSION_REFRESH_EACH_REQUEST = True
