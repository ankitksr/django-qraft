"""
DB-coordinated token-bucket throttle.

Bucket state lives in `RateBucket` so N workers (across processes/machines)
share one rate limit instead of each enforcing its own in-memory bucket.

SQLite note: SQLite has no real row-level locking (select_for_update() is a
no-op there, and concurrent writers just serialize on the DB-file lock), so
under SQLite this is correct but not concurrency-safe across processes -
fine for tests/single-process use. Under Postgres, select_for_update()
takes a real row lock, making concurrent acquire() calls correct.
"""

import functools
import logging

from django.db import transaction
from django.utils import timezone

_logger = logging.getLogger("qraft")


class RateLimited(Exception):
    """Raised by `throttled()` when a task is denied tokens by its bucket."""


def acquire(key: str, rate: float, capacity: float, cost: float = 1.0) -> bool:
    """
    Attempt to withdraw `cost` tokens from the named bucket.

    Refills the bucket based on elapsed time since it was last touched, then
    withdraws `cost` tokens if enough are available.

    Args:
        key: Bucket identifier (e.g. provider/tenant name).
        rate: Tokens added per second.
        capacity: Maximum tokens the bucket can hold.
        cost: Tokens this call needs.

    Returns:
        True if tokens were withdrawn, False if the bucket didn't have enough.
    """
    from qraft.models.tasks import RateBucket

    # Seed brand-new buckets at full capacity (standard token-bucket
    # semantics) rather than the model's zero default, so the first caller
    # for a key isn't denied while waiting for a refill that never ran.
    RateBucket.objects.get_or_create(key=key, defaults={"tokens": capacity})

    with transaction.atomic():
        bucket = RateBucket.objects.select_for_update().get(key=key)

        now = timezone.now()
        elapsed = max(0.0, (now - bucket.updated_at).total_seconds())
        bucket.tokens = min(capacity, bucket.tokens + elapsed * rate)

        if bucket.tokens >= cost:
            bucket.tokens -= cost
            bucket.save(update_fields=["tokens", "updated_at"])
            return True

        bucket.save(update_fields=["tokens", "updated_at"])
        return False


def throttled(key: str, rate: float, capacity: float | None = None, cost: float = 1.0):
    """
    Decorator that gates a task function behind a DB token bucket.

    Raises `RateLimited` when the bucket is empty, so the task fails and
    qraft's retry path reschedules it with backoff.
    """
    bucket_capacity = capacity if capacity is not None else max(rate, cost)

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            if not acquire(key, rate, bucket_capacity, cost):
                raise RateLimited(f"Rate limit exceeded for bucket '{key}'")
            return func(*args, **kwargs)

        return wrapper

    return decorator
