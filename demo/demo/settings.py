"""
Django settings for the Qraft demo project.

Supports both SQLite (quick testing) and PostgreSQL (recommended).
Set DEMO_USE_POSTGRES=1 to use PostgreSQL.
"""

import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent

SECRET_KEY = "demo-only-not-for-production"
DEBUG = True
ALLOWED_HOSTS = ["*"]

INSTALLED_APPS = [
    "django.contrib.contenttypes",
    "django.contrib.auth",
    "django_q",
    "qraft",
    "showcase",
]

ROOT_URLCONF = "demo.urls"
WSGI_APPLICATION = "demo.wsgi.application"

# Database configuration
# PostgreSQL recommended for concurrent task processing
# SQLite works for basic testing but may have locking issues under load
if os.environ.get("DEMO_USE_POSTGRES") or os.environ.get("POSTGRES_DB"):
    DATABASES = {
        "default": {
            "ENGINE": "django.db.backends.postgresql",
            "NAME": os.environ.get("POSTGRES_DB", "qraft_demo"),
            "USER": os.environ.get("POSTGRES_USER", "postgres"),
            "PASSWORD": os.environ.get("POSTGRES_PASSWORD", ""),
            "HOST": os.environ.get("POSTGRES_HOST", "localhost"),
            "PORT": os.environ.get("POSTGRES_PORT", "5432"),
            "CONN_MAX_AGE": 0,  # Must be 0 with native pooling
            "OPTIONS": {
                "pool": {
                    "min_size": 2,  # Minimum connections to keep alive
                    "max_size": 20,  # Conservative pool size to prevent accumulation
                    "timeout": 10,  # Wait time for available connection (seconds)
                    "max_lifetime": 300,  # Close connections after 5 minutes (prevents accumulation)
                    "max_idle": 60,  # Close idle connections after 1 minute (aggressive cleanup)
                }
            },
        }
    }
else:
    DATABASES = {
        "default": {
            "ENGINE": "django.db.backends.sqlite3",
            "NAME": BASE_DIR / "db.sqlite3",
        }
    }

DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"
USE_TZ = True
TIME_ZONE = "UTC"

# Logging
LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "simple": {"format": "[%(levelname)s] %(message)s"},
    },
    "handlers": {
        "console": {"class": "logging.StreamHandler", "formatter": "simple"},
    },
    "loggers": {
        "qraft": {"handlers": ["console"], "level": "DEBUG"},
        "showcase": {"handlers": ["console"], "level": "INFO"},
    },
}

# Django-Q2 configuration (ORM broker)
# Baseline cluster: standard Django-Q2 without threading
Q_CLUSTER = {
    "name": "baseline",
    "workers": 2,
    "timeout": 300,
    "retry": 600,
    "orm": "default",
    # Alternative clusters for performance comparison
    "ALT_CLUSTERS": {
        "qraft": {
            "name": "qraft",
            "workers": 2,
            "timeout": 300,
            "retry": 600,
            "orm": "default",
        }
    },
}

# Qraft configuration
# Default configuration for baseline cluster (no threading)
# Use Q_CLUSTER_NAME environment variable to select alternative configurations
QRAFT_CLUSTER = {
    "threads": 1,  # Baseline: standard Django-Q2 workers (no threading)
    "max_inflight": 2,
    "retry_defaults": {
        "max_attempts": 3,
        "delay": 2.0,
        "backoff": "exponential",
        "jitter": True,
    },
    # Alternative cluster configurations (selected via Q_CLUSTER_NAME env var)
    "ALT_CLUSTERS": {
        "qraft": {
            "threads": 4,  # Qraft: multithreaded workers
            "max_inflight": 8,
            "retry_defaults": {
                "max_attempts": 3,
                "delay": 2.0,
                "backoff": "exponential",
                "jitter": True,
            },
        }
    },
}
