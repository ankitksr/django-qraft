"""Tests for qraft.reaper module."""

from datetime import timedelta

import pytest
from django.utils import timezone
from django_q.models import Task as Q2Task

from qraft.models import QraftTask, QraftTaskAttempt, TaskStatus
from qraft.reaper import reap_orphans

STALE_AFTER = 60


def _backdate(attempt, seconds):
    QraftTaskAttempt.objects.filter(id=attempt.id).update(
        date_created=timezone.now() - timedelta(seconds=seconds)
    )


@pytest.mark.django_db
class TestReapOrphans:
    def test_reaps_orphan_and_schedules_retry_when_policy_allows(self):
        task = QraftTask.objects.create(
            func="test.module.test_function",
            status=TaskStatus.RUNNING,
            retry_policy={
                "max_attempts": 3,
                "base_delay": 10.0,
                "backoff_strategy": "fixed",
                "jitter": False,
                "jitter_max": 0.0,
                "retry_exceptions": [],
                "skip_exceptions": [],
            },
        )
        attempt = QraftTaskAttempt.objects.create(
            qraft_task=task,
            attempt_number=1,
            q2_task_id="dead-worker-task",
            success=None,
        )
        _backdate(attempt, STALE_AFTER * 2)

        reaped = reap_orphans(stale_after=STALE_AFTER)

        assert reaped == 1
        attempt.refresh_from_db()
        assert attempt.success is False
        assert attempt.exception_class == "OrphanedTask"
        assert attempt.date_completed is not None

        task.refresh_from_db()
        assert task.status == TaskStatus.PENDING  # retry scheduled

    def test_reaps_orphan_and_fails_task_when_no_policy(self):
        task = QraftTask.objects.create(
            func="test.module.test_function",
            status=TaskStatus.RUNNING,
            retry_policy={},
        )
        attempt = QraftTaskAttempt.objects.create(
            qraft_task=task,
            attempt_number=1,
            q2_task_id="dead-worker-task-2",
            success=None,
        )
        _backdate(attempt, STALE_AFTER * 2)

        reaped = reap_orphans(stale_after=STALE_AFTER)

        assert reaped == 1
        task.refresh_from_db()
        assert task.status == TaskStatus.FAILED

    def test_fresh_running_attempt_untouched(self):
        task = QraftTask.objects.create(
            func="test.module.test_function", status=TaskStatus.RUNNING
        )
        attempt = QraftTaskAttempt.objects.create(
            qraft_task=task,
            attempt_number=1,
            q2_task_id="fresh-task",
            success=None,
        )
        # Not backdated - well within the staleness window.

        reaped = reap_orphans(stale_after=STALE_AFTER)

        assert reaped == 0
        attempt.refresh_from_db()
        assert attempt.success is None
        task.refresh_from_db()
        assert task.status == TaskStatus.RUNNING

    def test_attempt_with_existing_q2_task_untouched(self):
        task = QraftTask.objects.create(
            func="test.module.test_function", status=TaskStatus.RUNNING
        )
        attempt = QraftTaskAttempt.objects.create(
            qraft_task=task,
            attempt_number=1,
            q2_task_id="q2-task-that-finished",
            success=None,
        )
        _backdate(attempt, STALE_AFTER * 2)
        Q2Task.objects.create(
            id="q2-task-that-finished",
            name="test_task",
            func="test.module.test_function",
            started=timezone.now(),
            stopped=timezone.now(),
            success=True,
        )

        reaped = reap_orphans(stale_after=STALE_AFTER)

        assert reaped == 0
        attempt.refresh_from_db()
        assert attempt.success is None
        task.refresh_from_db()
        assert task.status == TaskStatus.RUNNING
