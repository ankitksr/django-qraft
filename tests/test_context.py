"""Tests for qraft.context module."""

import pytest
from django_q.signals import pre_execute

from qraft.context import (
    _current_q2_task_id,
    _on_pre_execute,
    aggregate_usage,
    current_attempt,
    record_usage,
    report_progress,
)


@pytest.fixture(autouse=True)
def _reset_context():
    """Ensure the contextvar doesn't leak between tests."""
    token = _current_q2_task_id.set(None)
    yield
    _current_q2_task_id.reset(token)


def _enter_task(q2_task_id):
    """Simulate django_q's pre_execute signal for a given task id."""
    pre_execute.send(sender="django_q", func=lambda: None, task={"id": q2_task_id})


class TestCurrentAttempt:
    def test_none_outside_task(self):
        assert current_attempt() is None

    def test_none_for_unknown_task_id(self, db):
        _enter_task("unknown-task-id")
        assert current_attempt() is None

    def test_resolves_attempt_via_signal(self, qraft_task_attempt):
        _enter_task(qraft_task_attempt.q2_task_id)
        resolved = current_attempt()
        assert resolved is not None
        assert resolved.id == qraft_task_attempt.id

    def test_receiver_sets_contextvar_directly(self, qraft_task_attempt):
        _on_pre_execute(
            sender="django_q",
            func=lambda: None,
            task={"id": qraft_task_attempt.q2_task_id},
        )
        assert current_attempt().id == qraft_task_attempt.id


class TestRecordUsage:
    def test_noop_outside_task(self, qraft_task_attempt):
        record_usage(input_tokens=10)
        qraft_task_attempt.refresh_from_db()
        assert qraft_task_attempt.usage is None

    def test_sets_usage_on_current_attempt(self, qraft_task_attempt):
        _enter_task(qraft_task_attempt.q2_task_id)
        record_usage(model="gpt-4", input_tokens=100, cost=0.01)

        qraft_task_attempt.refresh_from_db()
        assert qraft_task_attempt.usage == {
            "model": "gpt-4",
            "input_tokens": 100,
            "cost": 0.01,
        }

    def test_repeated_calls_add_numeric_fields(self, qraft_task_attempt):
        _enter_task(qraft_task_attempt.q2_task_id)
        record_usage(model="gpt-4", input_tokens=100, cost=0.01)
        record_usage(model="gpt-4", input_tokens=50, cost=0.005)

        qraft_task_attempt.refresh_from_db()
        assert qraft_task_attempt.usage["input_tokens"] == 150
        assert qraft_task_attempt.usage["cost"] == pytest.approx(0.015)
        assert qraft_task_attempt.usage["model"] == "gpt-4"

    def test_non_numeric_fields_overwrite(self, qraft_task_attempt):
        _enter_task(qraft_task_attempt.q2_task_id)
        record_usage(model="gpt-4")
        record_usage(model="gpt-4-turbo")

        qraft_task_attempt.refresh_from_db()
        assert qraft_task_attempt.usage["model"] == "gpt-4-turbo"


class TestReportProgress:
    def test_noop_outside_task(self, qraft_task):
        report_progress(current=1, total=10)
        qraft_task.refresh_from_db()
        assert qraft_task.progress is None

    def test_writes_progress_payload(self, qraft_task_attempt, qraft_task):
        _enter_task(qraft_task_attempt.q2_task_id)
        report_progress(current=2, total=10, message="working", stage="parse")

        qraft_task.refresh_from_db()
        assert qraft_task.progress == {
            "current": 2,
            "total": 10,
            "message": "working",
            "stage": "parse",
        }

    def test_merges_across_calls(self, qraft_task_attempt, qraft_task):
        _enter_task(qraft_task_attempt.q2_task_id)
        report_progress(current=1, total=10)
        report_progress(current=2)

        qraft_task.refresh_from_db()
        assert qraft_task.progress == {"current": 2, "total": 10}


class TestAggregateUsage:
    def test_sums_across_attempts(self, qraft_task, qraft_task_attempt):
        from qraft.models import QraftTaskAttempt

        qraft_task_attempt.usage = {"input_tokens": 100, "model": "gpt-4"}
        qraft_task_attempt.save(update_fields=["usage"])

        QraftTaskAttempt.objects.create(
            qraft_task=qraft_task,
            attempt_number=2,
            q2_task_id="test-task-id-456",
            usage={"input_tokens": 50, "model": "gpt-4-turbo"},
        )

        totals = aggregate_usage(qraft_task)
        assert totals["input_tokens"] == 150
        assert totals["model"] == "gpt-4-turbo"

    def test_skips_none_usage(self, qraft_task, qraft_task_attempt):
        assert aggregate_usage(qraft_task) == {}
