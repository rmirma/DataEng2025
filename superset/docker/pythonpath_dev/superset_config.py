import os

# Superset specific config
ROW_LIMIT = 5000

# Flask App Builder configuration
SECRET_KEY = os.getenv("SUPERSET_SECRET_KEY", "SUp3r$3cReT_kEy_f0R_dAtA_eNg1n33RiNg_Pr0j3cT_2025!")

# Database configuration
SQLALCHEMY_DATABASE_URI = (
    f"postgresql://"
    f"{os.getenv('DATABASE_USER', 'superset')}:"
    f"{os.getenv('DATABASE_PASSWORD', 'superset')}@"
    f"{os.getenv('DATABASE_HOST', 'superset-db')}:"
    f"{os.getenv('DATABASE_PORT', '5432')}/"
    f"{os.getenv('DATABASE_DB', 'superset')}"
)

# Redis configuration for caching and Celery
REDIS_HOST = os.getenv("REDIS_HOST", "superset-redis")
REDIS_PORT = os.getenv("REDIS_PORT", "6379")

# Celery configuration
class CeleryConfig:
    broker_url = f"redis://{REDIS_HOST}:{REDIS_PORT}/0"
    result_backend = f"redis://{REDIS_HOST}:{REDIS_PORT}/0"

CELERY_CONFIG = CeleryConfig

# Cache configuration
CACHE_CONFIG = {
    "CACHE_TYPE": "RedisCache",
    "CACHE_DEFAULT_TIMEOUT": 300,
    "CACHE_KEY_PREFIX": "superset_",
    "CACHE_REDIS_HOST": REDIS_HOST,
    "CACHE_REDIS_PORT": REDIS_PORT,
    "CACHE_REDIS_DB": 1,
}

DATA_CACHE_CONFIG = CACHE_CONFIG

# Session configuration - use Redis for persistent sessions
SESSION_TYPE = "redis"
SESSION_REDIS_HOST = REDIS_HOST
SESSION_REDIS_PORT = int(REDIS_PORT)
SESSION_PERMANENT = True
PERMANENT_SESSION_LIFETIME = 86400  # 24 hours in seconds

# Disable CSRF for API calls (fixes "CSRF session token is missing" error)
WTF_CSRF_ENABLED = False
WTF_CSRF_EXEMPT_LIST = []

# Feature flags
FEATURE_FLAGS = {
    "ENABLE_TEMPLATE_PROCESSING": True,
}

# Prevent timeout issues
SUPERSET_WEBSERVER_TIMEOUT = 300
