"""Tests for qraft.lease (execution lease and heartbeat)."""

import os
import threading

import pytest
from django_q.signals import post_execute, pre_execute

from qraft import lease


@pytest.fixture(autouse=True)
def _no_leaked_threads():
    """Fail loudly if a test leaves a heartbeat registered."""
    yield
    with lease._stop_events_lock:
        lease._stop_events.clear()


@pytest.mark.django_db
class TestStampStart:
    def test_stamps_start_and_first_heartbeat(self, qraft_task_attempt):
        assert lease.stamp_start(qraft_task_attempt.q2_task_id) is True

        qraft_task_attempt.refresh_from_db()
        assert qraft_task_attempt.date_started is not None
        assert qraft_task_attempt.heartbeat_at is not None

    def test_false_for_plain_django_q_task(self, db):
        assert lease.stamp_start("not-a-qraft-task") is False

    def test_stamps_worker_pid(self, qraft_task_attempt):
        lease.stamp_start(qraft_task_attempt.q2_task_id)

        qraft_task_attempt.refresh_from_db()
        assert qraft_task_attempt.worker_pid == os.getpid()

    def test_stamps_the_current_thread_name_for_pool_threads(
        self, qraft_task_attempt, monkeypatch
    ):
        # A real second thread can't share the transactional test DB
        # connection (sqlite locks it), so rename the current thread instead
        # of spawning one - stamp_start only reads the name, not identity.
        original_name = threading.current_thread().name
        threading.current_thread().name = "qraft_worker_2"
        try:
            lease.stamp_start(qraft_task_attempt.q2_task_id)
        finally:
            threading.current_thread().name = original_name

        qraft_task_attempt.refresh_from_db()
        assert qraft_task_attempt.worker_thread == "qraft_worker_2"

    def test_main_thread_is_not_reported_as_a_worker_thread(self, qraft_task_attempt):
        """The standard (non-threaded) worker executes on MainThread, which
        isn't a pool thread worth reporting."""
        lease.stamp_start(qraft_task_attempt.q2_task_id)

        qraft_task_attempt.refresh_from_db()
        assert qraft_task_attempt.worker_thread is None


@pytest.mark.django_db
class TestTouch:
    def test_moves_the_heartbeat_forward(self, qraft_task_attempt):
        lease.stamp_start(qraft_task_attempt.q2_task_id)
        qraft_task_attempt.refresh_from_db()
        first = qraft_task_attempt.heartbeat_at

        assert lease.touch(qraft_task_attempt.q2_task_id) == 1
        qraft_task_attempt.refresh_from_db()
        assert qraft_task_attempt.heartbeat_at > first

    def test_no_op_once_the_attempt_is_resolved(self, qraft_task_attempt):
        qraft_task_attempt.success = True
        qraft_task_attempt.save(update_fields=["success"])

        assert lease.touch(qraft_task_attempt.q2_task_id) == 0


# transaction=True for the same reason test_e2e.py uses it: heartbeat_loop
# closes every connection on its way out, which the default transaction-wrapped
# mode cannot survive against a real database server.
@pytest.mark.django_db(transaction=True)
class TestHeartbeatLoop:
    def test_stops_on_the_stop_event_without_touching(self, qraft_task_attempt):
        stop_event = threading.Event()
        stop_event.set()

        lease.heartbeat_loop(
            qraft_task_attempt.q2_task_id, stop_event, interval=0, deadline=60
        )

        qraft_task_attempt.refresh_from_db()
        assert qraft_task_attempt.heartbeat_at is None

    def test_stops_once_the_attempt_is_resolved(self, qraft_task_attempt, monkeypatch):
        """The terminator that covers the standard (non-threaded) worker: the
        monitor resolves the attempt and the heartbeat UPDATE matches no rows."""
        real_touch = lease.touch
        resolved = []

        def _touch_then_resolve(q2_task_id):
            rows = real_touch(q2_task_id)
            if not resolved:
                resolved.append(True)
                type(qraft_task_attempt).objects.filter(
                    id=qraft_task_attempt.id
                ).update(success=True)
            return rows

        monkeypatch.setattr(lease, "touch", _touch_then_resolve)

        lease.heartbeat_loop(
            qraft_task_attempt.q2_task_id, threading.Event(), interval=0, deadline=60
        )

        qraft_task_attempt.refresh_from_db()
        assert qraft_task_attempt.heartbeat_at is not None

    def test_deadline_bounds_the_loop(self, qraft_task_attempt, monkeypatch):
        calls = []
        monkeypatch.setattr(lease, "touch", lambda q2_task_id: calls.append(1) or 1)

        lease.heartbeat_loop(
            qraft_task_attempt.q2_task_id,
            threading.Event(),
            interval=0.01,
            deadline=0.025,
        )

        assert len(calls) == 2  # stops before the 3rd interval crosses the deadline


@pytest.mark.django_db
class TestSignalReceivers:
    def test_pre_execute_opens_the_lease(self, qraft_task_attempt, monkeypatch):
        started = []
        monkeypatch.setattr(lease, "start_heartbeat", started.append)

        pre_execute.send(
            sender="django_q",
            func=lambda: None,
            task={"id": qraft_task_attempt.q2_task_id},
        )

        qraft_task_attempt.refresh_from_db()
        assert qraft_task_attempt.date_started is not None
        assert started == [qraft_task_attempt.q2_task_id]

    def test_no_heartbeat_for_plain_django_q_task(self, db, monkeypatch):
        started = []
        monkeypatch.setattr(lease, "start_heartbeat", started.append)

        pre_execute.send(
            sender="django_q", func=lambda: None, task={"id": "plain-q2-task"}
        )

        assert started == []

    def test_post_execute_stops_the_heartbeat(self):
        stop_event = threading.Event()
        with lease._stop_events_lock:
            lease._stop_events["finished-task"] = stop_event

        post_execute.send(sender="django_q", task={"id": "finished-task"})

        assert stop_event.is_set()


class TestStartStopHeartbeat:
    def test_one_thread_per_task_and_deregistered_on_exit(self, db, monkeypatch):
        release = threading.Event()
        monkeypatch.setattr(
            lease, "heartbeat_loop", lambda *a, **kw: release.wait(timeout=5)
        )

        first = lease.start_heartbeat("only-once")
        second = lease.start_heartbeat("only-once")
        assert first is not None
        assert second is None

        release.set()
        first.join(timeout=5)
        # The thread deregisters itself so a later attempt can heartbeat again.
        assert "only-once" not in lease._stop_events

    def test_stop_is_safe_for_unknown_ids(self):
        lease.stop_heartbeat(None)
        lease.stop_heartbeat("never-started")


@pytest.mark.django_db
class TestLeaseDeadline:
    def test_falls_back_to_the_default_ceiling_without_a_timeout(self, monkeypatch):
        from django_q.conf import Conf

        monkeypatch.setattr(Conf, "TIMEOUT", None)
        assert lease._lease_deadline_seconds() == lease.DEFAULT_MAX_LEASE_SECONDS

    def test_derives_from_the_task_timeout(self, monkeypatch):
        from django_q.conf import Conf

        monkeypatch.setattr(Conf, "TIMEOUT", 120)
        assert lease._lease_deadline_seconds() == 120 + lease.LEASE_DEADLINE_MARGIN


@pytest.mark.django_db
class TestMarkerLease:
    """
    Retries, DLQ requeues and deferred tasks arrive with no attempt row.

    Until the lease creates one and marks the task RUNNING, a worker that dies
    mid-attempt leaves nothing for the reaper to find.
    """

    def _pending_task(self):
        from qraft.models import QraftTask, TaskStatus

        return QraftTask.objects.create(
            func="demo.showcase.tasks.noop_task",
            status=TaskStatus.PENDING,
            retry_policy={"max_attempts": 3},
        )

    def test_creates_the_attempt_and_marks_the_task_running(self, db):
        from qraft.models import QraftTaskAttempt, TaskStatus

        qraft_task = self._pending_task()

        assert lease.open_marker_lease(f"qraft:{qraft_task.id}:2", "q2-retry-2") is True

        attempt = QraftTaskAttempt.objects.get(q2_task_id="q2-retry-2")
        assert attempt.qraft_task_id == qraft_task.id
        assert attempt.attempt_number == 2
        qraft_task.refresh_from_db()
        assert qraft_task.status == TaskStatus.RUNNING

    def test_pre_execute_opens_the_lease_for_a_retry(self, db, monkeypatch):
        from qraft.models import QraftTaskAttempt, TaskStatus

        started = []
        monkeypatch.setattr(lease, "start_heartbeat", started.append)
        qraft_task = self._pending_task()

        pre_execute.send(
            sender="django_q",
            func=lambda: None,
            task={"id": "q2-retry-3", "name": f"qraft:{qraft_task.id}:3"},
        )

        assert started == ["q2-retry-3"]
        attempt = QraftTaskAttempt.objects.get(q2_task_id="q2-retry-3")
        # Heartbeat stamped, so a dead worker is visible to the reaper.
        assert attempt.heartbeat_at is not None
        assert attempt.date_started is not None
        qraft_task.refresh_from_db()
        assert qraft_task.status == TaskStatus.RUNNING

    def test_reaper_reclaims_a_retry_whose_worker_died(self, db, monkeypatch):
        from datetime import timedelta

        from django.utils import timezone

        from qraft.models import QraftTaskAttempt
        from qraft.reaper import reap_orphans

        monkeypatch.setattr(lease, "start_heartbeat", lambda _id: None)
        qraft_task = self._pending_task()

        pre_execute.send(
            sender="django_q",
            func=lambda: None,
            task={"id": "q2-retry-dead", "name": f"qraft:{qraft_task.id}:2"},
        )

        # Worker dies: the heartbeat goes stale and no result is ever recorded.
        QraftTaskAttempt.objects.filter(q2_task_id="q2-retry-dead").update(
            heartbeat_at=timezone.now() - timedelta(hours=1)
        )

        assert reap_orphans() == 1
        attempt = QraftTaskAttempt.objects.get(q2_task_id="q2-retry-dead")
        assert attempt.success is False
        assert attempt.exception_class == "OrphanedTask"

    def test_does_not_resurrect_a_settled_task(self, db):
        from qraft.models import QraftTask, TaskStatus

        qraft_task = QraftTask.objects.create(
            func="demo.showcase.tasks.noop_task",
            status=TaskStatus.SUCCEEDED,
        )

        assert lease.open_marker_lease(f"qraft:{qraft_task.id}:2", "q2-dupe") is True

        qraft_task.refresh_from_db()
        assert qraft_task.status == TaskStatus.SUCCEEDED

    def test_ignores_a_task_without_a_marker(self, db):
        from qraft.models import QraftTaskAttempt

        assert lease.open_marker_lease("some-user-task-name", "q2-plain") is False
        assert lease.open_marker_lease(None, "q2-plain") is False
        assert not QraftTaskAttempt.objects.filter(q2_task_id="q2-plain").exists()


@pytest.mark.django_db
class TestFirstStartAnnouncement:
    def test_task_started_and_pickup_fire_once_under_duplicate_delivery(
        self,
        qraft_task_attempt,
        signal_log,
        recording_sink,
        django_capture_on_commit_callbacks,
    ):
        from datetime import timedelta

        from django.utils import timezone

        from qraft.context import current_context

        qraft_task_attempt.enqueued_at = timezone.now() - timedelta(seconds=3)
        qraft_task_attempt.save(update_fields=["enqueued_at"])
        task = {"id": qraft_task_attempt.q2_task_id, "name": "t"}

        with django_capture_on_commit_callbacks(execute=True):
            lease._on_pre_execute_lease(sender="django_q", func=None, task=task)
            qraft_task_attempt.refresh_from_db()
            first_beat = qraft_task_attempt.heartbeat_at
            lease._on_pre_execute_lease(sender="django_q", func=None, task=task)

        assert len(signal_log["task_started"]) == 1
        payload = signal_log["task_started"][0]
        assert payload["attempt_id"] == str(qraft_task_attempt.id)
        assert payload["outcome"] is None
        assert recording_sink.names("counter") == ["qraft.attempt.started"]
        assert len(recording_sink.values("qraft.attempt.pickup")) == 1
        assert recording_sink.values("qraft.attempt.pickup")[0] >= 3

        # Re-entry inside one delivery is a no-op, not a second execution.
        qraft_task_attempt.refresh_from_db()
        assert qraft_task_attempt.heartbeat_at >= first_beat
        assert qraft_task_attempt.execution_count == 1
        assert current_context()["attempt_id"] == str(qraft_task_attempt.id)
        lease.stop_heartbeat(qraft_task_attempt.q2_task_id)

    def test_stamp_start_is_false_for_plain_q2_tasks_and_after_resolution(
        self, qraft_task_attempt
    ):
        from qraft import context

        assert lease.stamp_start("not-ours") is False
        assert lease.stamp_start(qraft_task_attempt.q2_task_id) is True

        qraft_task_attempt.success = True
        qraft_task_attempt.save(update_fields=["success"])
        # A new delivery of the same message, which is what a redelivery is.
        context.clear_context()
        assert lease.stamp_start(qraft_task_attempt.q2_task_id) is False


@pytest.mark.django_db
class TestRedeliveryGuard:
    """One attempt executes once, whatever the broker redelivers."""

    def test_a_first_delivery_claims_and_runs(self, qraft_task_attempt):
        from qraft import context
        from qraft.runner import guard_delivery

        context._current_q2_task_id.set(qraft_task_attempt.q2_task_id)
        assert lease.claim_delivery(qraft_task_attempt.q2_task_id) == (
            lease.FIRST_DELIVERY
        )
        guard_delivery()  # does not raise

        qraft_task_attempt.refresh_from_db()
        assert qraft_task_attempt.execution_count == 1
        assert qraft_task_attempt.date_started is not None

    def test_a_second_delivery_is_refused_and_never_calls_the_function(
        self, qraft_task_attempt, recording_sink
    ):
        from qraft import context
        from qraft.runner import RedeliveredAttempt, run_task

        lease.stamp_start(qraft_task_attempt.q2_task_id)
        qraft_task_attempt.refresh_from_db()
        started = qraft_task_attempt.date_started
        beat = qraft_task_attempt.heartbeat_at
        lease.stop_heartbeat(qraft_task_attempt.q2_task_id)

        # A redelivery arrives in a fresh execution context.
        context.clear_context()
        context._current_q2_task_id.set(qraft_task_attempt.q2_task_id)

        with pytest.raises(RedeliveredAttempt):
            run_task("tests.test_lease._never_called", [], {})

        assert _CALLS == []
        qraft_task_attempt.refresh_from_db()
        # The refused delivery neither restarts the attempt nor refreshes the
        # lease - refreshing it is exactly what hid the stuck task before.
        assert qraft_task_attempt.date_started == started
        assert qraft_task_attempt.heartbeat_at == beat
        assert qraft_task_attempt.execution_count == 1
        assert recording_sink.names("counter") == [
            "qraft.attempt.started",
            "qraft.attempt.redelivered",
        ]

    def test_the_refusal_resolves_through_the_retry_policy(self, qraft_task_attempt):
        from qraft.hooks import qraft_hook_handler
        from qraft.models import QraftTaskAttempt
        from qraft.models.tasks import AttemptState

        lease.stamp_start(qraft_task_attempt.q2_task_id)
        lease.stop_heartbeat(qraft_task_attempt.q2_task_id)

        q2_task = type(
            "Q2Task",
            (),
            {
                "id": qraft_task_attempt.q2_task_id,
                "name": "t",
                "success": False,
                "result": (
                    "q2 task x is a repeat delivery : Traceback\n"
                    "qraft.runner.RedeliveredAttempt: refused"
                ),
                "stopped": None,
            },
        )()
        qraft_hook_handler(q2_task)

        qraft_task_attempt.refresh_from_db()
        assert qraft_task_attempt.success is False
        assert qraft_task_attempt.exception_class == "RedeliveredAttempt"
        # The retry policy, not the broker's delivery loop, decides attempt 2.
        second = QraftTaskAttempt.objects.get(
            qraft_task=qraft_task_attempt.qraft_task, attempt_number=2
        )
        assert second.state == AttemptState.SCHEDULED

    def test_a_higher_allowance_admits_the_repeat(
        self, qraft_task_attempt, settings, monkeypatch
    ):
        from qraft import conf, context

        monkeypatch.setitem(settings.QRAFT_CLUSTER, "max_executions_per_attempt", 2)
        conf._cached_conf.cache_clear()
        try:
            lease.stamp_start(qraft_task_attempt.q2_task_id)
            lease.stop_heartbeat(qraft_task_attempt.q2_task_id)
            context.clear_context()
            assert lease.claim_delivery(qraft_task_attempt.q2_task_id) == (
                lease.REPEAT_DELIVERY
            )
            context.clear_context()
            assert lease.claim_delivery(qraft_task_attempt.q2_task_id) == (
                lease.REFUSED_DELIVERY
            )
        finally:
            conf._cached_conf.cache_clear()

    def test_a_plain_django_q_task_is_never_refused(self, db):
        from qraft import context
        from qraft.runner import run_task

        context._current_q2_task_id.set("not-a-qraft-delivery")
        assert lease.claim_delivery("not-a-qraft-delivery") == lease.NO_ATTEMPT
        assert run_task("tests.test_lease._returns_ok", [], {}) == "ok"

    def test_a_refused_marker_delivery_starts_no_heartbeat(self, qraft_task_attempt):
        """
        A Qraft-dispatched attempt's task_name IS a marker, so a refused
        redelivery of one reaches `open_marker_lease`. Starting a heartbeat
        there would refresh the lease of an attempt that will not run - which
        is the exact thing that hid a redelivery loop from the reaper.
        """
        from qraft import context
        from qraft.retry import QRAFT_MARKER_FMT, QRAFT_MARKER_PREFIX

        marker = QRAFT_MARKER_FMT.format(
            prefix=QRAFT_MARKER_PREFIX,
            task_id=qraft_task_attempt.qraft_task_id,
            attempt=qraft_task_attempt.attempt_number,
        )
        task = {"id": qraft_task_attempt.q2_task_id, "name": marker}

        lease._on_pre_execute_lease(sender="django_q", func=None, task=task)
        lease.stop_heartbeat(qraft_task_attempt.q2_task_id)
        with lease._stop_events_lock:
            lease._stop_events.clear()
        qraft_task_attempt.refresh_from_db()
        beat = qraft_task_attempt.heartbeat_at

        context.clear_context()
        lease._on_pre_execute_lease(sender="django_q", func=None, task=task)

        with lease._stop_events_lock:
            assert lease._stop_events == {}
        qraft_task_attempt.refresh_from_db()
        assert qraft_task_attempt.heartbeat_at == beat

    def test_the_payload_flags_a_redelivery(self, qraft_task_attempt):
        from qraft import signals

        qraft_task_attempt.execution_count = 2
        payload = signals.attempt_payload(
            qraft_task_attempt, qraft_task_attempt.qraft_task, "failed"
        )
        assert payload["execution_count"] == 2
        assert payload["redelivered"] is True


_CALLS: list = []


def _never_called():
    _CALLS.append("ran")


def _returns_ok():
    return "ok"


def _raises_boom():
    raise ValueError("boom")


@pytest.mark.django_db
class TestCloseLease:
    """The lease ends when the function returns, not when the monitor says so.

    A dead Django-Q2 monitor can leave the worker blocked in
    `result_queue.put()` with the task already finished. The heartbeat thread
    knows nothing about that and keeps the lease fresh, so the reaper reads the
    attempt as alive and never resolves it.
    """

    def test_a_returned_task_stops_heartbeating_and_records_when_it_returned(
        self, qraft_task_attempt
    ):
        from qraft import context
        from qraft.runner import run_task

        context._current_q2_task_id.set(qraft_task_attempt.q2_task_id)
        lease.stamp_start(qraft_task_attempt.q2_task_id)
        lease.start_heartbeat(qraft_task_attempt.q2_task_id)
        # Held directly: the heartbeat thread unregisters itself once it stops,
        # so the dict entry is gone by the time the assertion runs.
        stop_event = lease._stop_events[qraft_task_attempt.q2_task_id]

        assert run_task("tests.test_lease._returns_ok", [], {}) == "ok"

        qraft_task_attempt.refresh_from_db()
        assert qraft_task_attempt.returned_at is not None
        # Still unresolved: the result has not reached the monitor yet. That is
        # the state the reaper has to be able to act on.
        assert qraft_task_attempt.success is None
        assert stop_event.is_set()

    def test_a_raising_task_closes_its_lease_too(self, qraft_task_attempt):
        from qraft import context
        from qraft.runner import run_task

        context._current_q2_task_id.set(qraft_task_attempt.q2_task_id)
        lease.stamp_start(qraft_task_attempt.q2_task_id)

        with pytest.raises(ValueError):
            run_task("tests.test_lease._raises_boom", [], {})

        qraft_task_attempt.refresh_from_db()
        assert qraft_task_attempt.returned_at is not None

    def test_a_plain_django_q_task_is_left_alone(self, db):
        from qraft import context
        from qraft.runner import run_task

        context.clear_context()
        # No q2 task id bound: nothing to stamp, and no lease to close.
        assert run_task("tests.test_lease._returns_ok", [], {}) == "ok"

    def test_a_stamp_failure_never_costs_the_caller_its_result(
        self, qraft_task_attempt, monkeypatch
    ):
        from qraft import context
        from qraft.runner import run_task

        context._current_q2_task_id.set(qraft_task_attempt.q2_task_id)

        def _explode(*a, **kw):
            raise RuntimeError("db gone")

        monkeypatch.setattr(lease, "stop_heartbeat", _explode)
        with pytest.raises(RuntimeError):
            run_task("tests.test_lease._returns_ok", [], {})
        # The stamp itself is the part that must not lose the result; the
        # heartbeat stop is the last statement and has nothing after it.
        qraft_task_attempt.refresh_from_db()
        assert qraft_task_attempt.returned_at is not None
