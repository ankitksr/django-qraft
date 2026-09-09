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
        usage = qraft_task_attempt.usage
        # Money is stored as its decimal string form whatever the caller
        # passed, so it is never summed as a float.
        assert {key: usage[key] for key in ("model", "input_tokens", "cost")} == {
            "model": "gpt-4",
            "input_tokens": 100,
            "cost": "0.01",
        }
        # The totals are joined by one per-increment entry, so an attempt that
        # calls two models stays priceable.
        assert [entry["model"] for entry in usage["entries"]] == ["gpt-4"]

    def test_repeated_calls_add_numeric_fields(self, qraft_task_attempt):
        _enter_task(qraft_task_attempt.q2_task_id)
        record_usage(model="gpt-4", input_tokens=100, cost=0.01)
        record_usage(model="gpt-4", input_tokens=50, cost=0.005)

        qraft_task_attempt.refresh_from_db()
        assert qraft_task_attempt.usage["input_tokens"] == 150
        # Exactly 0.015, not 0.014999999999999999: float costs take the same
        # Decimal path a Decimal cost does.
        assert qraft_task_attempt.usage["cost"] == "0.015"
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
            "attempt_id": str(qraft_task_attempt.id),
        }

    def test_uuid_and_datetime_extras_survive_both_write_paths(
        self, qraft_task_attempt, qraft_task
    ):
        """
        The Postgres path builds its payload with raw SQL, so it needs the
        encoder the JSON column declares; without it the same call that works
        on SQLite raises TypeError there.
        """
        from uuid import uuid4

        item_id = uuid4()
        _enter_task(qraft_task_attempt.q2_task_id)
        assert report_progress(current=1, total=2, item_id=item_id) is True

        qraft_task_attempt.refresh_from_db()
        assert qraft_task_attempt.progress["item_id"] == str(item_id)

    def test_merges_across_calls(self, qraft_task_attempt, qraft_task):
        _enter_task(qraft_task_attempt.q2_task_id)
        report_progress(current=1, total=10)
        report_progress(current=2)

        qraft_task.refresh_from_db()
        assert qraft_task.progress == {
            "current": 2,
            "total": 10,
            "attempt_id": str(qraft_task_attempt.id),
        }


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


class TestAttemptOwnedProgress:
    def test_attempt_owns_progress_and_advanced_is_distinct_from_reported(
        self, qraft_task_attempt, qraft_task
    ):
        _enter_task(qraft_task_attempt.q2_task_id)
        assert report_progress(current=1, total=10) is True
        qraft_task_attempt.refresh_from_db()
        first_reported = qraft_task_attempt.progress_reported_at
        first_advanced = qraft_task_attempt.progress_advanced_at
        assert qraft_task_attempt.progress == {"current": 1, "total": 10}
        assert first_reported is not None and first_advanced == first_reported

        # Same numbers, new message: reported moves, advanced does not.
        report_progress(current=1, message="waiting for provider")
        qraft_task_attempt.refresh_from_db()
        assert qraft_task_attempt.progress_reported_at > first_reported
        assert qraft_task_attempt.progress_advanced_at == first_advanced

        report_progress(current=2)
        qraft_task_attempt.refresh_from_db()
        assert qraft_task_attempt.progress_advanced_at > first_advanced
        qraft_task.refresh_from_db()
        assert qraft_task.progress["current"] == 2
        assert qraft_task.progress["attempt_id"] == str(qraft_task_attempt.id)

    def test_superseded_attempt_writes_itself_but_not_the_snapshot(
        self, qraft_task_attempt, qraft_task
    ):
        from qraft.models import QraftTaskAttempt

        retry = QraftTaskAttempt.objects.create(
            qraft_task=qraft_task, attempt_number=2, q2_task_id="test-task-id-2"
        )
        _enter_task(retry.q2_task_id)
        report_progress(current=0, total=10)

        # Attempt 1 is still running (a stall) and reports its old 90%.
        _enter_task(qraft_task_attempt.q2_task_id)
        report_progress(current=9, total=10)

        qraft_task_attempt.refresh_from_db()
        assert qraft_task_attempt.progress == {"current": 9, "total": 10}
        qraft_task.refresh_from_db()
        assert qraft_task.progress == {
            "current": 0,
            "total": 10,
            "attempt_id": str(retry.id),
        }

    def test_coalescing_skips_unchanged_writes_inside_the_interval(
        self, qraft_task_attempt, monkeypatch
    ):
        from qraft.conf import get_conf

        monkeypatch.setattr(get_conf(), "progress_min_interval", 60.0)
        _enter_task(qraft_task_attempt.q2_task_id)
        assert report_progress(current=1, total=10, message="a") is True
        assert report_progress(message="b") is False
        assert report_progress(current=2) is True  # advanced: always written
        assert report_progress(message="c", force=True) is True
        qraft_task_attempt.refresh_from_db()
        assert qraft_task_attempt.progress["message"] == "c"

    def test_async_helpers_write_through(self, qraft_task_attempt, qraft_task):
        from asgiref.sync import async_to_sync

        from qraft.context import arecord_usage, areport_progress

        _enter_task(qraft_task_attempt.q2_task_id)
        assert async_to_sync(areport_progress)(current=3, total=4) is True
        async_to_sync(arecord_usage)(input_tokens=5)
        qraft_task_attempt.refresh_from_db()
        assert qraft_task_attempt.progress == {"current": 3, "total": 4}
        assert qraft_task_attempt.usage == {"input_tokens": 5}


class TestDecimalUsage:
    def test_decimal_cost_is_stored_as_text_and_summed_exactly(
        self, qraft_task, qraft_task_attempt
    ):
        from decimal import Decimal

        from qraft.models import QraftTaskAttempt

        _enter_task(qraft_task_attempt.q2_task_id)
        record_usage(model="m", input_tokens=1, cost=Decimal("0.1"))
        record_usage(model="m", input_tokens=2, cost=Decimal("0.2"))
        qraft_task_attempt.refresh_from_db()
        usage = qraft_task_attempt.usage
        assert {key: usage[key] for key in ("model", "input_tokens", "cost")} == {
            "model": "m",
            "input_tokens": 3,
            "cost": "0.3",
        }
        assert [entry["estimated_cost"] for entry in usage["entries"]] == ["0.1", "0.2"]

        QraftTaskAttempt.objects.create(
            qraft_task=qraft_task,
            attempt_number=2,
            q2_task_id="test-task-id-dec",
            usage={"input_tokens": 1, "cost": "0.7"},
        )
        totals = aggregate_usage(qraft_task)
        assert {key: totals[key] for key in ("model", "input_tokens", "cost")} == {
            "model": "m",
            "input_tokens": 4,
            "cost": "1.0",
        }


@pytest.mark.django_db
class TestConsumeBudget:
    """A run's provider allowance, spent one request at a time."""

    def _run(self, budgets=None):
        from qraft import runs

        return runs.start(
            ("worksheet", 1), ["ai"], budgets=budgets or {"openai_requests": 2}
        )

    def test_each_call_decrements_and_the_last_one_raises(self):
        from qraft.context import BudgetExhausted, consume_budget, remaining_budget

        run_id = self._run()

        assert consume_budget("openai_requests", run_id=run_id) == 1
        assert consume_budget("openai_requests", run_id=run_id) == 0
        assert remaining_budget("openai_requests", run_id=run_id) == 0
        with pytest.raises(BudgetExhausted):
            consume_budget("openai_requests", run_id=run_id)
        # The refusal spends nothing: the balance never goes negative.
        assert remaining_budget("openai_requests", run_id=run_id) == 0

    def test_a_spend_larger_than_the_balance_is_refused_whole(self):
        from qraft.context import BudgetExhausted, consume_budget, remaining_budget

        run_id = self._run()

        with pytest.raises(BudgetExhausted):
            consume_budget("openai_requests", 3, run_id=run_id)
        assert remaining_budget("openai_requests", run_id=run_id) == 2
        assert consume_budget("openai_requests", 2, run_id=run_id) == 0

    def test_an_undeclared_key_and_no_run_are_both_unmetered(self, db):
        from qraft.context import consume_budget, remaining_budget

        run_id = self._run()

        assert consume_budget("anthropic_requests", run_id=run_id) is None
        assert remaining_budget("anthropic_requests", run_id=run_id) is None
        assert consume_budget("openai_requests") is None

    def test_it_charges_the_executing_attempts_run(self):
        from qraft import context
        from qraft.context import consume_budget
        from qraft.models import QraftTask, QraftTaskAttempt, TaskStatus

        run_id = self._run()
        task = QraftTask.objects.create(
            func="tests.e2e_tasks.succeed",
            task_args=[],
            task_kwargs={},
            status=TaskStatus.RUNNING,
            run_id=run_id,
            stage="ai",
        )
        attempt = QraftTaskAttempt.objects.create(
            qraft_task=task, attempt_number=1, q2_task_id="q2-budget"
        )
        context.bind_attempt(attempt, task)

        assert consume_budget("openai_requests") == 1

    def test_a_fractional_budget_is_refused_at_start(self):
        from qraft import runs

        with pytest.raises(runs.RunError, match="whole number"):
            runs.start(("worksheet", 1), ["ai"], budgets={"openai_requests": 2.5})

    def test_a_non_positive_spend_is_a_programming_error(self):
        from qraft.context import consume_budget

        with pytest.raises(ValueError):
            consume_budget("openai_requests", 0, run_id=self._run())
