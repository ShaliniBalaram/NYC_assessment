import os

# Superset specific config
ROW_LIMIT = 5000
SUPERSET_WEBSERVER_PORT = 8088

# Flask App Builder configuration
APP_NAME = "NYC TLC Data Analytics"
APP_ICON = "/static/assets/images/superset-logo-horiz.png"
FAVICONS = [{"href": "/static/assets/images/favicon.png"}]

# The SQLAlchemy connection string to your BigQuery database
SQLALCHEMY_DATABASE_URI = f"bigquery://{os.environ.get('GOOGLE_CLOUD_PROJECT')}"

# Google OAuth2 configuration
AUTH_TYPE = 1  # Database Authentication
OAUTH_PROVIDERS = []

# Cache configuration
CACHE_CONFIG = {
    'CACHE_TYPE': 'redis',
    'CACHE_DEFAULT_TIMEOUT': 300,
    'CACHE_KEY_PREFIX': 'superset_',
    'CACHE_REDIS_HOST': 'redis',
    'CACHE_REDIS_PORT': 6379,
    'CACHE_REDIS_DB': 1,
    'CACHE_REDIS_URL': 'redis://redis:6379/1'
}

# Additional configurations
FEATURE_FLAGS = {
    "DASHBOARD_NATIVE_FILTERS": True,
    "DASHBOARD_CROSS_FILTERS": True,
    "DASHBOARD_NATIVE_FILTERS_SET": True,
    "ENABLE_TEMPLATE_PROCESSING": True,
}

# Dashboard refresh frequency
DASHBOARD_AUTO_REFRESH_INTERVALS = [
    [0, 'Never'],
    [300, '5 minutes'],
    [600, '10 minutes'],
    [1800, '30 minutes'],
    [3600, '1 hour'],
    [21600, '6 hours'],
    [43200, '12 hours'],
    [86400, '24 hours'],
]

# Default database configurations
DATABASES = {
    'BigQuery': {
        'allow_csv_upload': False,
        'allow_ctas': True,
        'allow_cvas': True,
        'allow_dml': True,
        'cancel_query_on_windows_unload': True,
        'expose_in_sqllab': True,
        'force_ctas_schema': False,
        'allow_multi_schema_metadata_fetch': True,
    }
}
