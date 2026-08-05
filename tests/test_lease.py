"""Tests for qraft.lease (execution lease and heartbeat)."""

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


@pytest.mark.django_db
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
