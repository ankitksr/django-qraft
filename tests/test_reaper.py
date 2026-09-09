"""Tests for qraft.reaper module."""

from datetime import timedelta
from unittest.mock import patch

import pytest
from django.utils import timezone
from django_q.models import Task as Q2Task

from qraft.models import HookDispatch, QraftTask, QraftTaskAttempt, TaskStatus
from qraft.reaper import reap_orphans, reconcile_finished, replay_unrouted

STALE_AFTER = 60

# Comfortably past max(3 * heartbeat_interval, min_heartbeat_grace).
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


def _q2_task(q2_task_id, success, stopped_age, result=None):
    stopped = timezone.now() - timedelta(seconds=stopped_age)
    return Q2Task.objects.create(
        id=q2_task_id,
        name="test_task",
        func="test.module.test_function",
        started=stopped - timedelta(seconds=1),
        stopped=stopped,
        success=success,
        result=result,
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

    def test_a_lost_result_is_named_apart_from_a_dead_worker(self):
        """`returned_at` splits "the work ran" from "the worker died in it".

        A wedged monitor leaves the function finished and its result stranded;
        a SIGKILL leaves it half-done. A retry policy is entitled to treat the
        two differently, so they cannot share one exception class.
        """
        task = _running_task(retry_policy=RETRY_POLICY)
        attempt = _attempt(task, "monitor-wedged-task")
        QraftTaskAttempt.objects.filter(id=attempt.id).update(
            returned_at=timezone.now()
        )
        _heartbeat(attempt, DEAD_HEARTBEAT_AGE)

        assert reap_orphans(stale_after=STALE_AFTER) == 1

        attempt.refresh_from_db()
        assert attempt.success is False
        assert attempt.exception_class == "ResultLost"

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

    def test_attempt_with_fresh_q2_task_untouched(self):
        """A saved completion inside the grace window belongs to the
        monitor's own hook delivery - not reaped, not yet replayed."""
        task = _running_task()
        attempt = _attempt(task, "q2-task-that-finished")
        _heartbeat(attempt, DEAD_HEARTBEAT_AGE)
        _q2_task("q2-task-that-finished", success=True, stopped_age=0)

        assert reap_orphans(stale_after=STALE_AFTER) == 0

        attempt.refresh_from_db()
        assert attempt.success is None
        task.refresh_from_db()
        assert task.status == TaskStatus.RUNNING

    def test_attempt_with_stale_q2_task_replayed_not_reaped(self):
        """The hook handler died after the monitor saved the Task row: the
        Q2-row exclusion must not park the attempt forever - the sweep
        replays the saved completion instead."""
        task = _running_task()
        attempt = _attempt(task, "q2-saved-hook-died")
        _heartbeat(attempt, DEAD_HEARTBEAT_AGE)
        _q2_task("q2-saved-hook-died", success=True, stopped_age=DEAD_HEARTBEAT_AGE)

        assert reap_orphans(stale_after=STALE_AFTER) == 0  # replayed, not reaped

        attempt.refresh_from_db()
        assert attempt.success is True
        assert attempt.exception_class is None
        task.refresh_from_db()
        assert task.status == TaskStatus.SUCCEEDED


@pytest.mark.django_db
class TestReconcileFinished:
    def test_stale_failed_completion_replayed_schedules_retry(self):
        """A replayed failure goes through the normal retry routing."""
        task = _running_task(retry_policy=RETRY_POLICY)
        attempt = _attempt(task, "q2-failed-hook-died")
        _q2_task(
            "q2-failed-hook-died",
            success=False,
            stopped_age=DEAD_HEARTBEAT_AGE,
            result="boom : Traceback\ndemo.tasks.TransientError: boom",
        )

        assert reconcile_finished() == 1

        attempt.refresh_from_db()
        assert attempt.success is False
        assert attempt.exception_class == "TransientError"
        task.refresh_from_db()
        assert task.status == TaskStatus.PENDING  # retry scheduled

    def test_fresh_completion_not_replayed(self):
        task = _running_task()
        _attempt(task, "q2-just-stopped")
        _q2_task("q2-just-stopped", success=True, stopped_age=0)

        assert reconcile_finished() == 0

    def test_replay_racing_genuine_resolution_is_noop(self):
        """The compare-and-set in the hook handler: an attempt resolved
        between the sweep's snapshot and the replay keeps its outcome."""
        from qraft import reaper
        from qraft.hooks import qraft_hook_handler

        task = _running_task()
        attempt = _attempt(task, "q2-raced-completion")
        _q2_task("q2-raced-completion", success=True, stopped_age=DEAD_HEARTBEAT_AGE)

        def race_then_replay(q2_task):
            QraftTaskAttempt.objects.filter(id=attempt.id).update(
                success=False,
                exception_class="OrphanedTask",
                date_completed=timezone.now(),
            )
            qraft_hook_handler(q2_task)

        with patch.object(reaper, "qraft_hook_handler", race_then_replay):
            reaper.reconcile_finished()

        attempt.refresh_from_db()
        assert attempt.success is False  # replay did not flip the outcome
        assert attempt.exception_class == "OrphanedTask"


@pytest.mark.django_db
class TestQueuedTaskIds:
    def test_reads_task_ids_out_of_the_orm_queue(self):
        from django_q.brokers import get_broker
        from django_q.signing import SignedPackage

        from qraft.reaper import _queued_q2_task_ids

        broker = get_broker()
        broker.enqueue(SignedPackage.dumps({"id": "queued-1", "name": "t"}))

        assert "queued-1" in _queued_q2_task_ids()

    def test_returns_none_on_non_orm_broker(self):
        """A Redis-style broker's queue is invisible here: report "unknown,
        don't reap" so long-queued unstarted tasks aren't duplicated."""
        from unittest.mock import Mock

        from qraft.reaper import _queued_q2_task_ids

        with patch("django_q.brokers.get_broker", return_value=Mock()):
            assert _queued_q2_task_ids() is None


class TestReplayUnrouted:
    """Resolved attempts whose post-commit routing died get it replayed."""

    def _resolved_attempt(self, success=True, age=600, routed=False, **task_extra):
        task = QraftTask.objects.create(
            func="demo.showcase.tasks.noop_task",
            status=TaskStatus.SUCCEEDED if success else TaskStatus.FAILED,
            **task_extra,
        )
        attempt = QraftTaskAttempt.objects.create(
            qraft_task=task,
            attempt_number=1,
            q2_task_id=f"q2-unrouted-{task.id.hex[:16]}",
            success=success,
            routed=routed,
            date_completed=timezone.now() - timedelta(seconds=age),
        )
        return task, attempt

    def test_replays_hook_dispatch_and_marks_routed(self, db):
        from qraft.models import HookDispatch

        task, attempt = self._resolved_attempt(success_hook="test.hooks.success")

        with patch("qraft.hooks.q2_async_task") as mock_async:
            mock_async.return_value = "q2-replayed-hook"
            assert replay_unrouted() == 1

        attempt.refresh_from_db()
        assert attempt.routed is True
        assert HookDispatch.objects.filter(
            qraft_task=task, hook_type="success"
        ).exists()

    def test_recent_resolutions_are_left_for_the_live_handler(self, db):
        self._resolved_attempt(age=0)
        assert replay_unrouted() == 0

    def test_routed_resolutions_are_skipped(self, db):
        self._resolved_attempt(routed=True)
        assert replay_unrouted() == 0

    def test_replays_workflow_routing(self, db):
        from qraft.models import QraftIterModel, WorkflowStatus

        iter_model = QraftIterModel.objects.create(
            func="demo.showcase.tasks.noop_task",
            total_count=1,
            status=WorkflowStatus.RUNNING,
        )
        _task, attempt = self._resolved_attempt(qraft_iter=iter_model)

        with patch("qraft.dispatchers.q2_async_task"):
            assert replay_unrouted() == 1

        iter_model.refresh_from_db()
        assert iter_model.completed_count == 1
        assert iter_model.status == WorkflowStatus.SUCCEEDED
        attempt.refresh_from_db()
        assert attempt.routed is True
        assert attempt.counted is True


@pytest.mark.django_db
class TestFlagStalls:
    """
    A stalled attempt is alive and not moving. The reaper says so and stops
    there: the attempt keeps running, so resolving it would let a retry write
    the same rows the original is still writing.
    """

    def _started(self, task, *, advanced_age=None, heartbeat_age=1, started_age=600):
        now = timezone.now()
        attempt = QraftTaskAttempt.objects.create(
            qraft_task=task, attempt_number=1, q2_task_id=f"q2-{task.id.hex[:24]}"
        )
        QraftTaskAttempt.objects.filter(id=attempt.id).update(
            date_started=now - timedelta(seconds=started_age),
            heartbeat_at=now - timedelta(seconds=heartbeat_age),
            progress_advanced_at=(
                None if advanced_age is None else now - timedelta(seconds=advanced_age)
            ),
            progress_reported_at=now,
        )
        attempt.refresh_from_db()
        return attempt

    def test_flags_a_live_attempt_that_stopped_advancing_exactly_once(
        self, signal_log, django_capture_on_commit_callbacks, recording_sink
    ):
        from qraft.reaper import flag_stalls

        task = _running_task(stall_after=60)
        attempt = self._started(task, advanced_age=300)

        with django_capture_on_commit_callbacks(execute=True):
            assert flag_stalls() == 1
        attempt.refresh_from_db()
        assert attempt.stall_suspected_at is not None
        # Observed, never acted on: no outcome, no retry, task still RUNNING.
        assert attempt.success is None
        assert QraftTask.objects.get(id=task.id).status == TaskStatus.RUNNING
        assert task.attempts.count() == 1

        assert len(signal_log["attempt_stall_suspected"]) == 1
        payload = signal_log["attempt_stall_suspected"][0]
        assert payload["attempt_id"] == str(attempt.id)
        assert payload["outcome"] is None
        assert "qraft.attempt.stall_suspected" in recording_sink.names()

        # Already flagged: the compare-and-swap makes a second sweep a no-op.
        with django_capture_on_commit_callbacks(execute=True):
            assert flag_stalls() == 0
        assert len(signal_log["attempt_stall_suspected"]) == 1

    def test_ignores_fresh_progress_stale_heartbeats_and_opted_out_tasks(self):
        from qraft.reaper import flag_stalls

        # Advancing inside the threshold.
        self._started(_running_task(stall_after=60), advanced_age=5)
        # Not advancing, but the worker is gone - that is the orphan sweep's
        # business, and running it first is what keeps the two disjoint.
        self._started(
            _running_task(stall_after=60),
            advanced_age=300,
            heartbeat_age=DEAD_HEARTBEAT_AGE,
        )
        # No stall_after: opted out.
        self._started(_running_task(), advanced_age=300)
        # Resolved already.
        resolved = self._started(_running_task(stall_after=60), advanced_age=300)
        QraftTaskAttempt.objects.filter(id=resolved.id).update(success=True)

        assert flag_stalls() == 0
        assert QraftTaskAttempt.objects.filter(
            stall_suspected_at__isnull=False
        ).exists() is False

    def test_an_attempt_that_never_reported_is_measured_from_its_start(self):
        from qraft.reaper import flag_stalls

        young = self._started(_running_task(stall_after=600), started_age=60)
        old = self._started(_running_task(stall_after=30), started_age=300)

        assert flag_stalls() == 1
        young.refresh_from_db()
        old.refresh_from_db()
        assert young.stall_suspected_at is None
        assert old.stall_suspected_at is not None

    def test_reap_orphans_runs_the_stall_sweep_after_the_orphan_sweep(self):
        task = _running_task(stall_after=60)
        attempt = self._started(task, advanced_age=300)

        reap_orphans(stale_after=STALE_AFTER)

        attempt.refresh_from_db()
        # Flagged as stalled, not reaped as an orphan: the heartbeat is fresh.
        assert attempt.stall_suspected_at is not None
        assert attempt.success is None

    def test_an_attempt_resolved_mid_sweep_is_not_flagged(self, signal_log):
        """
        The candidate query and the flag write are separate statements. Freeze
        the candidates, let the monitor resolve one exactly as it would in the
        gap, and the write must refuse it: observation never fires on an
        attempt that already finished.
        """
        from qraft import reaper

        task = _running_task(stall_after=60)
        attempt = self._started(task, advanced_age=300)
        original = reaper.QraftTaskAttempt.objects.filter

        class Frozen(list):
            """A candidate list read before the resolution, as the loop sees it."""

            def select_related(self, *args):
                return self

        def stale_candidates(*args, **kwargs):
            queryset = original(*args, **kwargs)
            if "qraft_task__stall_after__isnull" not in kwargs:
                return queryset
            candidates = Frozen(queryset.select_related("qraft_task"))
            original(id=attempt.id).update(success=True)
            QraftTask.objects.filter(id=task.id).update(status=TaskStatus.SUCCEEDED)
            return candidates

        reaper.QraftTaskAttempt.objects.filter = stale_candidates
        try:
            flagged = reaper.flag_stalls()
        finally:
            reaper.QraftTaskAttempt.objects.filter = original

        assert flagged == 0
        attempt.refresh_from_db()
        assert attempt.stall_suspected_at is None
        assert signal_log["attempt_stall_suspected"] == []


class TestHeartbeatGrace:
    """The grace period is a formula, and its floor is a setting."""

    def test_the_grace_is_the_larger_of_three_intervals_and_the_floor(
        self, settings, monkeypatch
    ):
        from qraft import conf
        from qraft.reaper import _heartbeat_grace

        assert conf.get_conf().min_heartbeat_grace == 90.0
        # Floor wins for a short interval, 3x wins for a long one.
        assert _heartbeat_grace(2.0) == 90.0
        assert _heartbeat_grace(60.0) == 180.0

        monkeypatch.setitem(settings.QRAFT_CLUSTER, "min_heartbeat_grace", 5.0)
        conf._cached_conf.cache_clear()
        try:
            assert _heartbeat_grace(1.0) == 5.0
            assert _heartbeat_grace(60.0) == 180.0
        finally:
            conf._cached_conf.cache_clear()


class TestReapedTaskFiresItsFailureHook:
    """
    A reaped task is a failed task, so its failure hook has to run.

    The reaper is the only path that settles an attempt whose worker died, and
    it is the only settlement path a task with a `failure_hook` and no
    workflow can reach after an OOM kill. It also sets `routed=True`, which
    takes the attempt out of `replay_unrouted()`'s query, so a hook missed
    here is missed permanently.
    """

    def test_an_orphan_reap_dispatches_the_failure_hook(self, db):
        task = _running_task(retry_policy={}, failure_hook="test.module.on_failure")
        attempt = _attempt(task, "orphan-with-failure-hook")
        _heartbeat(attempt, DEAD_HEARTBEAT_AGE)

        with patch("qraft.hooks.q2_async_task", return_value="q2-hook") as enqueue:
            assert reap_orphans(stale_after=STALE_AFTER) == 1

        task.refresh_from_db()
        assert task.status == TaskStatus.FAILED
        assert enqueue.call_args.args[0] == "test.module.on_failure"
        assert HookDispatch.objects.filter(
            qraft_task=task, hook_type="failure"
        ).exists()

    def test_an_exhausted_retry_policy_still_reaches_the_hook(self, db):
        task = _running_task(
            retry_policy={"max_attempts": 1}, failure_hook="test.module.on_failure"
        )
        attempt = _attempt(task, "exhausted-with-failure-hook")
        _heartbeat(attempt, DEAD_HEARTBEAT_AGE)

        with patch("qraft.hooks.q2_async_task", return_value="q2-hook"):
            assert reap_orphans(stale_after=STALE_AFTER) == 1

        task.refresh_from_db()
        assert task.status == TaskStatus.EXHAUSTED
        assert HookDispatch.objects.filter(
            qraft_task=task, hook_type="failure"
        ).exists()

    def test_a_reap_that_schedules_a_retry_fires_no_hook(self, db):
        task = _running_task(
            retry_policy={"max_attempts": 3}, failure_hook="test.module.on_failure"
        )
        attempt = _attempt(task, "retried-with-failure-hook")
        _heartbeat(attempt, DEAD_HEARTBEAT_AGE)

        with patch("qraft.hooks.q2_async_task", return_value="q2-hook"):
            assert reap_orphans(stale_after=STALE_AFTER) == 1

        task.refresh_from_db()
        assert task.status == TaskStatus.PENDING
        assert not HookDispatch.objects.filter(qraft_task=task).exists()
