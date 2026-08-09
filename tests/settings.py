"""Test Django settings."""

DEBUG = True

DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.sqlite3",
        "NAME": ":memory:",
    }
}

INSTALLED_APPS = [
    "django.contrib.contenttypes",
    "django.contrib.auth",
    "django_q",
    "qraft",
]

SECRET_KEY = "test-secret-key-for-testing-only"

USE_TZ = True

Q_CLUSTER = {
    "name": "test",
    "orm": "default",
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
