"""Tests for qraft.throttle module."""

from datetime import timedelta

import pytest
from django.utils import timezone

from qraft.throttle import RateLimited, acquire, throttled


def _age_bucket(key, seconds):
    """Push a bucket's updated_at back in time to simulate elapsed refill time."""
    from qraft.models.tasks import RateBucket

    RateBucket.objects.filter(key=key).update(
        updated_at=timezone.now() - timedelta(seconds=seconds)
    )


@pytest.mark.django_db
class TestAcquire:
    def test_creates_bucket_and_grants_from_capacity(self):
        assert acquire("provider-a", rate=1.0, capacity=5.0, cost=1.0) is True

    def test_drains_bucket_until_denied(self):
        key = "provider-b"
        for _ in range(3):
            assert acquire(key, rate=0.0, capacity=3.0, cost=1.0) is True
        assert acquire(key, rate=0.0, capacity=3.0, cost=1.0) is False

    def test_denied_when_empty(self):
        key = "provider-c"
        acquire(key, rate=0.0, capacity=1.0, cost=1.0)  # drain the single token
        assert acquire(key, rate=0.0, capacity=1.0, cost=1.0) is False

    def test_refills_over_elapsed_time(self):
        key = "provider-d"
        acquire(key, rate=1.0, capacity=1.0, cost=1.0)  # drain to 0
        assert acquire(key, rate=1.0, capacity=1.0, cost=1.0) is False

        _age_bucket(key, seconds=5)
        assert acquire(key, rate=1.0, capacity=1.0, cost=1.0) is True

    def test_refill_capped_at_capacity(self):
        key = "provider-e"
        acquire(key, rate=1.0, capacity=2.0, cost=1.0)  # tokens: 2 -> 1

        _age_bucket(key, seconds=1000)  # would overflow refill without capping
        from qraft.models.tasks import RateBucket

        assert acquire(key, rate=1.0, capacity=2.0, cost=1.0) is True
        bucket = RateBucket.objects.get(key=key)
        assert bucket.tokens == pytest.approx(1.0)


@pytest.mark.django_db
class TestThrottledDecorator:
    def test_calls_through_when_tokens_available(self):
        calls = []

        @throttled("provider-f", rate=1.0, capacity=5.0)
        def task(x):
            calls.append(x)
            return x * 2

        assert task(3) == 6
        assert calls == [3]

    def test_raises_rate_limited_when_bucket_empty(self):
        @throttled("provider-g", rate=0.0, capacity=1.0)
        def task():
            return "ok"

        assert task() == "ok"
        with pytest.raises(RateLimited):
            task()
