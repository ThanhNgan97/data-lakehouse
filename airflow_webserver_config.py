"""Flask-AppBuilder configuration for the Airflow webserver."""

from flask_appbuilder.security.manager import AUTH_DB


AUTH_TYPE = AUTH_DB

# Browser cookies are scoped by domain and path, not by port. Airflow and
# Superset both run on localhost during development, so their default
# ``session`` cookies would otherwise overwrite each other.
SESSION_COOKIE_NAME = "airflow_session"
