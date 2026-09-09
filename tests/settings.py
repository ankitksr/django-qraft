"""Test Django settings."""

import os
from urllib.parse import urlparse

DEBUG = True


def _database(url: str | None) -> dict:
    """
    In-memory SQLite by default; Postgres when QRAFT_TEST_DATABASE_URL is set.

    SQLite has no row locks, so the `postgres`-marked concurrency cases only
    run against the latter. The runner creates `test_<name>`, so the role
    needs CREATEDB.
    """
    if not url:
        return {"ENGINE": "django.db.backends.sqlite3", "NAME": ":memory:"}
    parsed = urlparse(url)
    return {
        "ENGINE": "django.db.backends.postgresql",
        "NAME": parsed.path.lstrip("/") or "qraft",
        "USER": parsed.username or "",
        "PASSWORD": parsed.password or "",
        "HOST": parsed.hostname or "localhost",
        "PORT": str(parsed.port or 5432),
    }


DATABASES = {"default": _database(os.environ.get("QRAFT_TEST_DATABASE_URL"))}

INSTALLED_APPS = [
    "django.contrib.contenttypes",
    "django.contrib.auth",
    "django_q",
    "qraft",
    # No django.contrib.admin on purpose: the dashboard must degrade its
    # admin deep-links gracefully, and the tests assert that.
    "qraft.dashboard",
]

ROOT_URLCONF = "tests.urls"

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [],
        "APP_DIRS": True,
        "OPTIONS": {},
    }
]

SECRET_KEY = "test-secret-key-for-testing-only"

USE_TZ = True

Q_CLUSTER = {
    "name": "test",
    "orm": "default",
    # django_q warns at import unless retry > timeout. Both are declared so the
    # suite runs warning-free: an unexplained warning on every run teaches
    # people to ignore warnings.
    "timeout": 30,
    "retry": 60,
    # Priority lanes are only drained by this broker; without it
    # priority_list_key() correctly refuses to route into them.
    "broker_class": "qraft.brokers.QraftOrmBroker",
}

# Only interpreted by Django >= 6.0 (django.tasks / DEP 14); ignored
# otherwise, so this is safe to declare unconditionally.
TASKS = {
    "default": {"BACKEND": "qraft.backend.QraftTaskBackend"},
}

QRAFT_CLUSTER = {
    "threads": 1,
    "max_inflight": 2,
    "grace_period": 10.0,
    "sync_hooks": False,
    "retry_defaults": {
        "max_attempts": 3,
        "delay": 30.0,
        "backoff": "exponential",
        "jitter": True,
        "jitter_max": 0.2,
    },
    "ALT_CLUSTERS": {
        "io-workers": {
            "threads": 8,
            "max_inflight": 16,
        }
    },
}
