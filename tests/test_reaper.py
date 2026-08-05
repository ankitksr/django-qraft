"""Tests for qraft.reaper module."""

from datetime import timedelta
from unittest.mock import patch

import pytest
from django.utils import timezone
from django_q.models import Task as Q2Task

from qraft.models import QraftTask, QraftTaskAttempt, TaskStatus
from qraft.reaper import reap_orphans

STALE_AFTER = 60

# Comfortably past max(3 * heartbeat_interval, MIN_HEARTBEAT_GRACE).
DEAD_HEARTBEAT_AGE = 600


def _backdate(attempt, seconds):
    QraftTaskAttempt.objects.filter(id=attempt.id).update(
        date_created=timezone.now() - timedelta(seconds=seconds)
    )


def _heartbeat(attempt, age_seconds):
    QraftTaskAttempt.objects.filter(id=attempt.id).update(
        heartbeat_at=timezone.now() - timedelta(seconds=age_seconds)
    )


def _running_task(**kwargs):
    kwargs.setdefault("func", "test.module.test_function")
    kwargs.setdefault("status", TaskStatus.RUNNING)
    return QraftTask.objects.create(**kwargs)


def _attempt(task, q2_task_id):
    return QraftTaskAttempt.objects.create(
        qraft_task=task, attempt_number=1, q2_task_id=q2_task_id, success=None
    )


RETRY_POLICY = {
    "max_attempts": 3,
    "base_delay": 10.0,
    "backoff_strategy": "fixed",
    "jitter": False,
    "jitter_max": 0.0,
    "retry_exceptions": [],
    "skip_exceptions": [],
}


@pytest.mark.django_db
class TestReapOrphans:
    def test_reaps_dead_heartbeat_and_schedules_retry_when_policy_allows(self):
        task = _running_task(retry_policy=RETRY_POLICY)
        attempt = _attempt(task, "dead-worker-task")
        _heartbeat(attempt, DEAD_HEARTBEAT_AGE)

        assert reap_orphans(stale_after=STALE_AFTER) == 1

        attempt.refresh_from_db()
        assert attempt.success is False
        assert attempt.exception_class == "OrphanedTask"
        assert attempt.date_completed is not None

        task.refresh_from_db()
        assert task.status == TaskStatus.PENDING  # retry scheduled

    def test_reaps_dead_heartbeat_and_fails_task_when_no_policy(self):
        task = _running_task(retry_policy={})
        attempt = _attempt(task, "dead-worker-task-2")
        _heartbeat(attempt, DEAD_HEARTBEAT_AGE)

        assert reap_orphans(stale_after=STALE_AFTER) == 1

        task.refresh_from_db()
        assert task.status == TaskStatus.FAILED

    def test_fresh_heartbeat_not_reaped_however_old_the_attempt(self):
        """The false-positive the lease exists to prevent: a legitimate
        long-running task, far past reap_stale_after, still heartbeating."""
        task = _running_task()
        attempt = _attempt(task, "long-running-task")
        _backdate(attempt, STALE_AFTER * 100)
        _heartbeat(attempt, 1)

        assert reap_orphans(stale_after=STALE_AFTER) == 0

        attempt.refresh_from_db()
        assert attempt.success is None
        task.refresh_from_db()
        assert task.status == TaskStatus.RUNNING

    def test_never_started_and_stale_is_reaped(self):
        task = _running_task(retry_policy={})
        attempt = _attempt(task, "delivered-then-crashed")
        _backdate(attempt, STALE_AFTER * 2)
        assert attempt.heartbeat_at is None

        assert reap_orphans(stale_after=STALE_AFTER) == 1

        attempt.refresh_from_db()
        assert attempt.exception_class == "OrphanedTask"

    def test_never_started_but_still_queued_is_not_reaped(self):
        task = _running_task()
        attempt = _attempt(task, "still-in-the-queue")
        _backdate(attempt, STALE_AFTER * 2)

        with patch(
            "qraft.reaper._queued_q2_task_ids", return_value={"still-in-the-queue"}
        ):
            assert reap_orphans(stale_after=STALE_AFTER) == 0

        attempt.refresh_from_db()
        assert attempt.success is None

    def test_never_started_not_reaped_when_queue_unreadable(self):
        task = _running_task()
        attempt = _attempt(task, "queue-unreadable")
        _backdate(attempt, STALE_AFTER * 2)

        with patch("qraft.reaper._queued_q2_task_ids", return_value=None):
            assert reap_orphans(stale_after=STALE_AFTER) == 0

        attempt.refresh_from_db()
        assert attempt.success is None

    def test_never_started_and_fresh_untouched(self):
        task = _running_task()
        attempt = _attempt(task, "just-enqueued")

        assert reap_orphans(stale_after=STALE_AFTER) == 0

        attempt.refresh_from_db()
        assert attempt.success is None
        task.refresh_from_db()
        assert task.status == TaskStatus.RUNNING

    def test_attempt_with_existing_q2_task_untouched(self):
        task = _running_task()
        attempt = _attempt(task, "q2-task-that-finished")
        _heartbeat(attempt, DEAD_HEARTBEAT_AGE)
        Q2Task.objects.create(
            id="q2-task-that-finished",
            name="test_task",
            func="test.module.test_function",
            started=timezone.now(),
            stopped=timezone.now(),
            success=True,
        )

        assert reap_orphans(stale_after=STALE_AFTER) == 0

        attempt.refresh_from_db()
        assert attempt.success is None
        task.refresh_from_db()
        assert task.status == TaskStatus.RUNNING


@pytest.mark.django_db
class TestQueuedTaskIds:
    def test_reads_task_ids_out_of_the_orm_queue(self):
        from django_q.brokers import get_broker
        from django_q.signing import SignedPackage

        from qraft.reaper import _queued_q2_task_ids

        broker = get_broker()
        broker.enqueue(SignedPackage.dumps({"id": "queued-1", "name": "t"}))

        assert "queued-1" in _queued_q2_task_ids()
