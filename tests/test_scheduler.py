"""Tests for qraft.scheduler: the dispatcher that owns delayed execution."""

from datetime import timedelta
from unittest.mock import Mock

import pytest
from django.db.models.query import QuerySet
from django.utils import timezone
from django_q.models import OrmQ
from django_q.signing import SignedPackage

from qraft import scheduler
from qraft.models import QraftTask, QraftTaskAttempt, TaskStatus
from qraft.models.tasks import AttemptState, TaskPriority

pytestmark = pytest.mark.django_db


def _task(**kwargs):
    defaults = {
        "func": "test.module.function",
        "task_args": [1, 2],
        "task_kwargs": {"key": "value"},
        "status": TaskStatus.PENDING,
    }
    return QraftTask.objects.create(**{**defaults, **kwargs})


def _due(qraft_task, seconds_ago: float = 1.0, **kwargs):
    """A SCHEDULED attempt whose due time has already passed."""
    return scheduler.schedule_attempt(
        qraft_task,
        1,
        timezone.now() - timedelta(seconds=seconds_ago),
        **kwargs,
    )


def _pack():
    """The single queued task pack, decoded."""
    return SignedPackage.loads(OrmQ.objects.get().payload)


class TestScheduleAttempt:
    def test_creates_a_scheduled_attempt_and_pends_the_task(self):
        task = _task(status=TaskStatus.FAILED)
        due_at = timezone.now() + timedelta(seconds=30)

        attempt = scheduler.schedule_attempt(task, 3, due_at, cluster="io-workers")

        assert attempt.attempt_number == 3
        assert attempt.state == AttemptState.SCHEDULED
        assert attempt.not_before == due_at
        assert attempt.cluster == "io-workers"
        assert attempt.q2_task_id is None
        task.refresh_from_db()
        assert task.status == TaskStatus.PENDING

    def test_inherited_cluster_reads_the_latest_attempt(self):
        task = _task()
        QraftTaskAttempt.objects.create(
            qraft_task=task, attempt_number=1, q2_task_id="a", cluster="first"
        )
        QraftTaskAttempt.objects.create(
            qraft_task=task, attempt_number=2, q2_task_id="b", cluster="second"
        )

        assert scheduler._inherited_cluster(task) == "second"

    def test_inherited_cluster_is_none_without_attempts(self):
        assert scheduler._inherited_cluster(_task()) is None


class TestDispatchDue:
    def test_leaves_an_attempt_that_is_not_due_yet(self):
        task = _task()
        scheduler.schedule_attempt(task, 1, timezone.now() + timedelta(hours=1))

        assert scheduler.dispatch_due() == 0
        assert not OrmQ.objects.exists()
        task.refresh_from_db()
        assert task.status == TaskStatus.PENDING

    def test_enqueues_a_due_attempt_and_stamps_the_q2_id(self):
        task = _task()
        attempt = _due(task)

        assert scheduler.dispatch_due() == 1

        attempt.refresh_from_db()
        assert attempt.state == AttemptState.QUEUED
        assert attempt.q2_task_id
        assert attempt.claimed_at is not None
        # The stamped id is the one actually queued, which is what lets the
        # hook handler resolve completion through the fast lookup.
        assert _pack()["id"] == attempt.q2_task_id

    def test_marks_the_task_running_once_it_is_queued(self):
        task = _task()
        _due(task)

        scheduler.dispatch_due()

        task.refresh_from_db()
        assert task.status == TaskStatus.RUNNING

    def test_does_not_resurrect_a_settled_task(self):
        task = _task()
        _due(task)
        QraftTask.objects.filter(pk=task.pk).update(status=TaskStatus.SUCCEEDED)

        scheduler.dispatch_due()

        task.refresh_from_db()
        assert task.status == TaskStatus.SUCCEEDED

    def test_enqueues_the_task_func_through_the_unwrapping_runner(self):
        _due(_task())

        scheduler.dispatch_due()

        pack = _pack()
        # A @task-decorated function's dotted path resolves to a non-callable
        # wrapper, so the runner is queued instead of the path itself.
        assert pack["func"] == "qraft.runner.run_task"
        assert pack["args"] == ("test.module.function", [1, 2], {"key": "value"})
        assert pack["hook"] == "qraft.hooks.qraft_hook_handler"
        # Qraft owns retries; the broker must not redeliver underneath it.
        assert pack["ack_failure"] is True

    def test_a_dispatch_override_replaces_func_and_args(self):
        _due(
            _task(),
            dispatch_func="qraft.backend.run_task_with_context",
            dispatch_args=["mod.fn", "task-id", "default", [], {}],
        )

        scheduler.dispatch_due()

        pack = _pack()
        assert pack["func"] == "qraft.backend.run_task_with_context"
        assert pack["args"] == ("mod.fn", "task-id", "default", [], {})

    def test_respects_the_batch_limit(self):
        task = _task()
        for number in range(1, 4):
            scheduler.schedule_attempt(
                task, number, timezone.now() - timedelta(seconds=1)
            )

        assert scheduler.dispatch_due(limit=2) == 2
        assert OrmQ.objects.count() == 2
        assert scheduler.dispatch_due(limit=2) == 1

    def test_dispatches_the_most_overdue_first(self):
        task = _task()
        late = scheduler.schedule_attempt(
            task, 1, timezone.now() - timedelta(seconds=60)
        )
        scheduler.schedule_attempt(task, 2, timezone.now() - timedelta(seconds=1))

        scheduler.dispatch_due(limit=1)

        late.refresh_from_db()
        assert late.state == AttemptState.QUEUED

    def test_a_broker_failure_leaves_the_attempt_reclaimable(self, monkeypatch):
        attempt = _due(_task())
        monkeypatch.setattr(
            scheduler, "_enqueue", Mock(side_effect=RuntimeError("broker down"))
        )

        assert scheduler.dispatch_due() == 0

        # The claim rolled back with the enqueue, so the next pass retries it
        # rather than stranding the attempt half-claimed.
        attempt.refresh_from_db()
        assert attempt.state == AttemptState.SCHEDULED
        assert attempt.q2_task_id is None


class TestNoDoubleEnqueue:
    def test_a_second_pass_does_not_enqueue_again(self):
        _due(_task())

        assert scheduler.dispatch_due() == 1
        assert scheduler.dispatch_due() == 0
        assert OrmQ.objects.count() == 1

    def test_a_lost_claim_never_enqueues(self, monkeypatch):
        """
        Two dispatchers reading the same row before either claims it.

        SQLite has no SKIP LOCKED, so this is the case correctness actually
        rests on: the compare-and-swap, not the read, decides ownership.
        """
        task = _task()
        attempt = _due(task)
        original_first = QuerySet.first
        stolen = []

        def steal_between_read_and_claim(self):
            row = original_first(self)
            if row is not None and row.pk == attempt.pk and not stolen:
                stolen.append(row.pk)
                QraftTaskAttempt.objects.filter(pk=attempt.pk).update(
                    state=AttemptState.QUEUED, claimed_at=timezone.now()
                )
            return row

        monkeypatch.setattr(QuerySet, "first", steal_between_read_and_claim)

        assert scheduler.dispatch_due() == 0
        assert stolen == [attempt.pk]
        assert not OrmQ.objects.exists()

    def test_the_claim_is_scoped_to_the_scheduled_state(self):
        task = _task()
        attempt = _due(task)
        QraftTaskAttempt.objects.filter(pk=attempt.pk).update(state=AttemptState.QUEUED)

        assert scheduler.dispatch_due() == 0
        assert not OrmQ.objects.exists()


class TestRoutingSurvivesTheDelay:
    def test_priority_lane_is_carried_to_the_enqueue(self):
        task = _task(priority=TaskPriority.HIGH)
        _due(task, cluster="io-workers")

        scheduler.dispatch_due()

        # The lane key is built from the target cluster, because django_q
        # ignores `cluster=` whenever an explicit broker is supplied.
        assert OrmQ.objects.get().key == "io-workers--high"

    def test_default_priority_uses_the_plain_cluster_lane(self):
        _due(_task(), cluster="io-workers")

        scheduler.dispatch_due()

        assert OrmQ.objects.get().key == "io-workers"

    def test_low_priority_lane_is_carried(self):
        _due(_task(priority=TaskPriority.LOW), cluster="io-workers")

        scheduler.dispatch_due()

        assert OrmQ.objects.get().key == "io-workers--low"

    def test_cluster_is_carried_to_the_enqueue(self):
        _due(_task(), cluster="io-workers")

        scheduler.dispatch_due()

        assert _pack()["cluster"] == "io-workers"

    def test_an_unrouted_attempt_falls_back_to_the_dispatching_cluster(self):
        from django_q.conf import Conf

        _due(_task(), cluster=None)

        scheduler.dispatch_due()

        # No recorded cluster means "whichever dispatcher claims it", and that
        # dispatcher routes the work to itself rather than leaving it null.
        assert _pack()["cluster"] == Conf.CLUSTER_NAME
        assert OrmQ.objects.get().key == Conf.CLUSTER_NAME


class TestCrashRecovery:
    def _claimed_but_unqueued(self, claimed_ago: float):
        task = _task()
        attempt = _due(task)
        QraftTaskAttempt.objects.filter(pk=attempt.pk).update(
            state=AttemptState.QUEUED,
            claimed_at=timezone.now() - timedelta(seconds=claimed_ago),
        )
        return attempt

    def test_rearms_a_claim_that_never_reached_the_broker(self):
        from qraft.reaper import rearm_stuck_claims

        attempt = self._claimed_but_unqueued(claimed_ago=3600)

        assert rearm_stuck_claims() == 1

        attempt.refresh_from_db()
        assert attempt.state == AttemptState.SCHEDULED
        assert attempt.claimed_at is None
        # And it dispatches cleanly on the next pass.
        assert scheduler.dispatch_due() == 1

    def test_leaves_a_fresh_claim_alone(self):
        from qraft.reaper import rearm_stuck_claims

        attempt = self._claimed_but_unqueued(claimed_ago=0)

        assert rearm_stuck_claims() == 0

        attempt.refresh_from_db()
        assert attempt.state == AttemptState.QUEUED

    def test_never_reaps_a_scheduled_attempt_however_overdue(self):
        from qraft.reaper import reap_orphans, rearm_stuck_claims

        task = _task(status=TaskStatus.RUNNING, retry_policy={"max_attempts": 3})
        attempt = scheduler.schedule_attempt(
            task, 1, timezone.now() - timedelta(days=7)
        )
        QraftTaskAttempt.objects.filter(pk=attempt.pk).update(
            date_created=timezone.now() - timedelta(days=7)
        )

        # A long-overdue SCHEDULED attempt means no dispatcher has run, not
        # that the work died. Reaping it would burn a retry on a run that
        # never happened.
        assert reap_orphans() == 0
        assert rearm_stuck_claims() == 0

        attempt.refresh_from_db()
        assert attempt.state == AttemptState.SCHEDULED
        assert attempt.success is None

    def test_reaper_staleness_runs_from_the_dispatch_not_the_row(self):
        from qraft.reaper import reap_orphans

        task = _task(status=TaskStatus.RUNNING, retry_policy={"max_attempts": 3})
        attempt = _due(task)
        scheduler.dispatch_due()
        # The row was created long ago (a long backoff), but it only reached a
        # broker a moment ago, so it is not stale.
        QraftTaskAttempt.objects.filter(pk=attempt.pk).update(
            date_created=timezone.now() - timedelta(days=2)
        )

        assert reap_orphans(stale_after=60) == 0


class TestLegacyScheduleBridge:
    def test_a_pre_upgrade_marker_delivery_still_resolves(self):
        """
        A Django-Q2 Schedule written by the previous version fires after the
        upgrade. It has no attempt row of its own; the marker is the only link.
        """
        from qraft.hooks import qraft_hook_handler

        task = _task(status=TaskStatus.RUNNING, retry_policy={})
        QraftTaskAttempt.objects.create(
            qraft_task=task, attempt_number=1, q2_task_id="legacy-1", success=False
        )

        q2_task = Mock()
        q2_task.id = "legacy-2"
        q2_task.name = f"qraft:{task.id}:2"
        q2_task.success = True
        q2_task.stopped = None
        q2_task.result = "ok"

        qraft_hook_handler(q2_task)

        attempt = QraftTaskAttempt.objects.get(qraft_task=task, attempt_number=2)
        assert attempt.q2_task_id == "legacy-2"
        assert attempt.success is True
        # Created by a broker delivery, so it was never the dispatcher's.
        assert attempt.state == AttemptState.QUEUED
        task.refresh_from_db()
        assert task.status == TaskStatus.SUCCEEDED

    def test_a_legacy_delivery_adopts_an_unclaimed_scheduled_attempt(self):
        """Both paths in flight for one attempt must not fork it in two."""
        from qraft.hooks import attempt_from_marker

        task = _task()
        scheduled = scheduler.schedule_attempt(task, 2, timezone.now())

        adopted = attempt_from_marker(f"qraft:{task.id}:2", "legacy-2")

        assert adopted.pk == scheduled.pk
        assert adopted.q2_task_id == "legacy-2"
        assert adopted.state == AttemptState.QUEUED
        assert task.attempts.count() == 1


class TestLoopTiming:
    def test_sleep_is_capped_by_the_next_due_attempt(self):
        scheduler.schedule_attempt(_task(), 1, timezone.now() + timedelta(seconds=2))

        # The poll interval must never be a floor on a shorter backoff.
        assert 1.0 < scheduler._seconds_until_due(30.0) <= 2.0

    def test_sleep_falls_back_to_the_interval_when_nothing_is_scheduled(self):
        assert scheduler._seconds_until_due(30.0) == 30.0

    def test_an_overdue_attempt_sleeps_the_minimum(self):
        scheduler.schedule_attempt(_task(), 1, timezone.now() - timedelta(hours=1))

        assert scheduler._seconds_until_due(30.0) == scheduler.MIN_SLEEP

    def test_notify_cuts_the_wait_short(self):
        # The seam a LISTEN/NOTIFY listener replaces the poll through.
        started = timezone.now()
        scheduler.notify()
        scheduler._wait(30.0)

        assert (timezone.now() - started).total_seconds() < 1.0
        assert not scheduler._wakeup.is_set()
