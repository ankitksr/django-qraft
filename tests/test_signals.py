"""Tests for qraft.signals: send sites, payload shape, robustness, replays."""

from types import MappingProxyType
from unittest.mock import Mock, patch

import pytest

from qraft import signals
from qraft.hooks import qraft_hook_handler
from qraft.models import HookDispatch, QraftTask, QraftTaskAttempt, TaskStatus

pytestmark = pytest.mark.django_db


def _q2(attempt, success=True, result=None):
    task = Mock()
    task.id = attempt.q2_task_id
    task.success = success
    task.result = result or ("ok" if success else "boom : Traceback\nValueError: x")
    task.stopped = None
    return task


class TestHookHandlerSends:
    def test_completion_fires_each_signal_once_with_an_immutable_id_payload(
        self, qraft_task, qraft_task_attempt, signal_log, django_capture_on_commit_callbacks
    ):
        qraft_task.subject_type, qraft_task.subject_id = "worksheet", "4117"
        qraft_task.save(update_fields=["subject_type", "subject_id"])
        q2_task = _q2(qraft_task_attempt)

        with (
            patch("qraft.hooks.q2_async_task", return_value="hook-1"),
            django_capture_on_commit_callbacks(execute=True),
        ):
            qraft_hook_handler(q2_task)
            # A duplicate delivery of the same result is dropped by the
            # success__isnull compare-and-set, so it announces nothing.
            qraft_hook_handler(q2_task)

        assert len(signal_log["attempt_finished"]) == 1
        assert len(signal_log["task_settled"]) == 1
        payload = signal_log["task_settled"][0]
        assert isinstance(payload, MappingProxyType)
        with pytest.raises(TypeError):
            payload["outcome"] = "tampered"
        assert payload["task_id"] == str(qraft_task.id)
        assert payload["attempt_id"] == str(qraft_task_attempt.id)
        assert payload["attempt_number"] == 1
        assert payload["outcome"] == "succeeded"
        assert payload["status"] == TaskStatus.SUCCEEDED
        assert payload["subject_type"] == "worksheet"
        assert payload["subject_id"] == "4117"
        assert payload["exception_class"] is None
        assert all(not hasattr(value, "pk") for value in payload.values())

    def test_retry_fires_attempt_finished_but_not_task_settled(
        self, qraft_task, qraft_task_attempt, signal_log, django_capture_on_commit_callbacks
    ):
        with django_capture_on_commit_callbacks(execute=True):
            qraft_hook_handler(_q2(qraft_task_attempt, success=False))

        assert len(signal_log["attempt_finished"]) == 1
        assert signal_log["attempt_finished"][0]["outcome"] == "failed"
        assert signal_log["attempt_finished"][0]["exception_class"] == "ValueError"
        assert signal_log["task_settled"] == []
        assert QraftTaskAttempt.objects.filter(attempt_number=2).exists()

    def test_raising_receiver_is_isolated_and_logged(
        self, qraft_task, qraft_task_attempt, django_capture_on_commit_callbacks, caplog
    ):
        def broken(sender, payload, **kwargs):
            raise RuntimeError("observer bug")

        signals.task_settled.connect(broken, weak=False)
        try:
            with (
                patch("qraft.hooks.q2_async_task", return_value="hook-2"),
                django_capture_on_commit_callbacks(execute=True),
                caplog.at_level("WARNING", logger="qraft"),
            ):
                qraft_hook_handler(_q2(qraft_task_attempt))
        finally:
            signals.task_settled.disconnect(broken)

        # Completion routing and hook dispatch survived the broken observer.
        assert QraftTask.objects.get(id=qraft_task.id).status == TaskStatus.SUCCEEDED
        assert HookDispatch.objects.filter(qraft_task=qraft_task).count() == 1
        assert "broken" in caplog.text and "observer bug" in caplog.text

    def test_send_is_deferred_to_commit(self, qraft_task, signal_log):
        from django.db import transaction

        with transaction.atomic():
            signals.send(signals.task_settled, QraftTask, {"task_id": "x"})
            assert signal_log["task_settled"] == []
        # The test transaction never commits, so nothing fires here either;
        # what matters is that the send did not happen inside the block.


class TestReaperSends:
    def test_reaped_attempt_reports_orphaned_and_settles_without_policy(
        self, signal_log, django_capture_on_commit_callbacks
    ):
        from datetime import timedelta

        from django.utils import timezone

        from qraft.reaper import reap_orphans

        task = QraftTask.objects.create(func="t.f", status=TaskStatus.RUNNING)
        QraftTaskAttempt.objects.create(
            qraft_task=task,
            attempt_number=1,
            q2_task_id="q2-orphan",
            heartbeat_at=timezone.now() - timedelta(hours=1),
        )
        with django_capture_on_commit_callbacks(execute=True):
            assert reap_orphans() == 1

        assert [p["outcome"] for p in signal_log["attempt_finished"]] == ["orphaned"]
        assert signal_log["task_settled"][0]["status"] == TaskStatus.FAILED
