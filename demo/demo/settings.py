"""
Settings for the django-qraft demo and verification suite.

One environment variable, ``Q_CLUSTER_NAME``, selects a cluster profile. Both
django-q2 and qraft read it and apply their own ``ALT_CLUSTERS`` entry, so a
worker process started with ``Q_CLUSTER_NAME=threaded`` drains the ``threaded``
broker lane *and* picks up qraft's threaded worker settings. Leaving it unset
gives the ``default`` profile, which is what the scenario runner itself uses.
"""

import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent

SECRET_KEY = "demo-only-not-for-production"
DEBUG = True
ALLOWED_HOSTS = ["*"]

INSTALLED_APPS = [
    "django.contrib.admin",
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    "django.contrib.messages",
    "django.contrib.staticfiles",
    "django_q",
    "qraft",
    "showcase",
]

MIDDLEWARE = [
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
]

ROOT_URLCONF = "demo.urls"
WSGI_APPLICATION = "demo.wsgi.application"

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [],
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                "django.template.context_processors.request",
                "django.contrib.auth.context_processors.auth",
                "django.contrib.messages.context_processors.messages",
            ],
        },
    },
]

STATIC_URL = "static/"
DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"
USE_TZ = True
TIME_ZONE = "UTC"


# --- Database -------------------------------------------------------------
#
# PostgreSQL is the default. The suite runs a dozen cluster processes against
# one database and leans on real row locks (`select_for_update`) in the
# throttle and parallel-workflow paths, which SQLite does not provide.
# `DEMO_DB=sqlite` still works for a quick look; the scenarios that need real
# locking say so when they run.

DEMO_DB = os.environ.get("DEMO_DB", "postgres")

if DEMO_DB == "sqlite":
    DATABASES = {
        "default": {
            "ENGINE": "django.db.backends.sqlite3",
            "NAME": BASE_DIR / "db.sqlite3",
            "OPTIONS": {
                "timeout": 30,
                "transaction_mode": "IMMEDIATE",
                "init_command": "PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;",
            },
        }
    }
else:
    DATABASES = {
        "default": {
            "ENGINE": "django.db.backends.postgresql",
            "NAME": os.environ.get("DEMO_PG_DB", "qraft_demo"),
            # Empty host means a local unix socket with peer auth, which is
            # what a stock Homebrew/apt PostgreSQL gives the current user.
            "HOST": os.environ.get("DEMO_PG_HOST", ""),
            "PORT": os.environ.get("DEMO_PG_PORT", ""),
            "USER": os.environ.get("DEMO_PG_USER", ""),
            "PASSWORD": os.environ.get("DEMO_PG_PASSWORD", ""),
            "CONN_MAX_AGE": 0,
        }
    }


# --- Django-Q2 ------------------------------------------------------------

Q_CLUSTER = {
    # PREFIX. Shared by every profile so payload signing stays compatible
    # across clusters, and so null-cluster Schedules (DLQ requeue) have one
    # unambiguous owner.
    "name": "default",
    "workers": int(os.environ.get("DEMO_WORKERS", "3")),
    "timeout": 60,
    # Qraft owns retries. The broker must not redeliver a failed or in-flight
    # message underneath it, so the visibility timeout is set far beyond any
    # scenario's lifetime: the orphan reaper is what reclaims a dead worker's
    # task, and that is the behaviour under test.
    "retry": 3600,
    # Keep every result row. The reaper treats "no django_q Task row" as part
    # of its orphan test, so trimming completed rows mid-run would corrupt it.
    "save_limit": 0,
    "catch_up": False,
    "orm": "default",
    "broker_class": "qraft.brokers.QraftOrmBroker",
    "label": "Qraft demo",
    "ALT_CLUSTERS": {
        "threaded": {"workers": 2},
        "baseline": {"workers": 2},
        "synchooks": {"workers": 1},
        "throttle-a": {"workers": 2},
        "throttle-b": {"workers": 2},
        # One worker and a one-deep hand-off queue, so completion order is a
        # faithful reading of dequeue order rather than of worker races.
        "lanes": {"workers": 1, "queue_limit": 1},
    },
}


# --- Qraft ----------------------------------------------------------------

QRAFT_CLUSTER = {
    "threads": 1,
    "max_inflight": 2,
    "sync_hooks": False,
    # Short lease/reap cycles keep the durability scenarios observable in
    # seconds. Production defaults are 30s/60s.
    "heartbeat_interval": 2.0,
    "reap_interval": 5.0,
    "reap_stale_after": 45.0,
    # The retention sweep is exercised explicitly by its own scenario, never
    # on a timer that could prune rows another scenario is still asserting on.
    "retention_days": None,
    "retry_defaults": {
        "max_attempts": 3,
        "delay": 2.0,
        "backoff": "exponential",
        "jitter": True,
        "jitter_max": 0.2,
    },
    "ALT_CLUSTERS": {
        "threaded": {"threads": 8, "max_inflight": 16},
        "baseline": {"threads": 1, "max_inflight": 2},
        "synchooks": {"sync_hooks": True},
        "throttle-a": {},
        "throttle-b": {},
        "lanes": {},
    },
}


# --- django.tasks (DEP 14, Django 6.0+) -----------------------------------

TASKS = {"default": {"BACKEND": "qraft.backend.QraftTaskBackend"}}


LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "cluster": {"format": "%(asctime)s %(levelname)-7s %(name)s %(message)s"},
    },
    "handlers": {
        "console": {"class": "logging.StreamHandler", "formatter": "cluster"},
    },
    "root": {"handlers": ["console"], "level": "WARNING"},
    "loggers": {
        "qraft": {"level": os.environ.get("DEMO_LOG_LEVEL", "INFO")},
        "qraft.dispatchers": {"level": os.environ.get("DEMO_LOG_LEVEL", "INFO")},
        "django-q": {"level": os.environ.get("DEMO_LOG_LEVEL", "INFO")},
        "showcase": {"level": "INFO"},
    },
}
