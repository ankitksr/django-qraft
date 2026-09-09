"""Tests for qraft.logging.QraftContextFilter."""

import logging

import pytest

from qraft import context
from qraft.logging import QraftContextFilter


@pytest.fixture(autouse=True)
def _reset_context():
    token = context._current_q2_task_id.set(None)
    bound = context._bound_context.set(None)
    yield
    context._current_q2_task_id.reset(token)
    context._bound_context.reset(bound)


def _record():
    return logging.LogRecord("qraft", logging.INFO, __file__, 1, "msg", (), None)


class TestQraftContextFilter:
    def test_attributes_are_present_and_empty_outside_a_task(self):
        record = _record()
        assert QraftContextFilter().filter(record) is True
        assert record.qraft_task_id == ""
        assert record.qraft_attempt_id == ""
        assert record.qraft_attempt_number == ""
        assert record.qraft_run_id == ""
        assert record.qraft_stage == ""
        assert record.qraft_subject_type == ""
        assert record.qraft_subject_id == ""

    def test_attributes_follow_the_bound_and_lazily_resolved_attempt(
        self, qraft_task, qraft_task_attempt
    ):
        qraft_task.subject_type, qraft_task.subject_id = "worksheet", "4117"
        qraft_task.save(update_fields=["subject_type", "subject_id"])

        context.bind_attempt(qraft_task_attempt, qraft_task)
        record = _record()
        QraftContextFilter().filter(record)
        assert record.qraft_task_id == str(qraft_task.id)
        assert record.qraft_attempt_id == str(qraft_task_attempt.id)
        assert record.qraft_attempt_number == 1
        assert record.qraft_subject_type == "worksheet"
        assert record.qraft_subject_id == "4117"

        # Only the q2 id known (pre_execute ran, lease has not bound yet).
        context._bound_context.set(None)
        context._current_q2_task_id.set(qraft_task_attempt.q2_task_id)
        record = _record()
        QraftContextFilter().filter(record)
        assert record.qraft_attempt_id == str(qraft_task_attempt.id)
        assert context.current_attempt_id() == str(qraft_task_attempt.id)

        context._current_q2_task_id.set("unknown")
        context._bound_context.set(None)
        assert context.current_attempt_id() is None
