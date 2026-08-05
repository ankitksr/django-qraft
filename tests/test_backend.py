"""
Tests for qraft.backend.QraftTaskBackend (django.tasks / DEP 14 engine).

Requires Django 6.0+ (django.tasks). Skipped entirely otherwise so the main
suite stays green on older Django installs.
"""

import ast
from datetime import timedelta
from unittest.mock import Mock, patch

import pytest

pytest.importorskip("django.tasks")

from django.tasks import task_backends  # noqa: E402
from django.tasks.base import Task, TaskContext, TaskResultStatus  # noqa: E402
from django.tasks.exceptions import TaskResultDoesNotExist  # noqa: E402
from django.utils import timezone  # noqa: E402
from django_q.models import Schedule  # noqa: E402

from qraft.backend import run_task_with_context  # noqa: E402
from qraft.hooks import qraft_hook_handler  # noqa: E402
from qraft.models import QraftTask, QraftTaskAttempt, TaskStatus  # noqa: E402


def sample_task_func(*args, **kwargs):
    """Module-level stand-in task function for django.tasks validation."""


CONTEXT_CALLS: list = []


def context_task_func(context, *args, **kwargs):
    """Module-level stand-in `takes_context=True` task function."""
    CONTEXT_CALLS.append((context, args, kwargs))
    return "done"


@pytest.fixture
def backend():
    return task_backends["default"]


def _make_django_task(**overrides):
    kwargs = dict(
        priority=0,
        func=sample_task_func,
        backend="default",
        queue_name="default",
        run_after=None,
    )
    kwargs.update(overrides)
    return Task(**kwargs)


@pytest.mark.django_db
class TestEnqueue:
    @patch("qraft.tasks.q2_async_task")
    def test_creates_qraft_task_and_returns_result(self, mock_q2, backend):
        mock_q2.return_value = "q2-task-1"

        task = _make_django_task()
        result = backend.enqueue(task, (1, 2), {"x": "y"})

        qraft_task = QraftTask.objects.get()
        assert result.id == str(qraft_task.id)
        # Qraft marks a QraftTask RUNNING as soon as it's queued (in-flight
        # with the broker), so a fresh enqueue never reports READY here.
        assert result.status == TaskResultStatus.RUNNING
        assert result.args == [1, 2]
        assert result.kwargs == {"x": "y"}
        assert qraft_task.task_args == [1, 2]
        assert qraft_task.task_kwargs == {"x": "y"}
        assert QraftTaskAttempt.objects.get().q2_task_id == "q2-task-1"

    @pytest.mark.parametrize(
        "priority,expected_lane",
        [(10, "high"), (0, "default"), (-10, "low")],
    )
    @patch("qraft.tasks.q2_async_task")
    def test_priority_maps_to_qraft_lane(
        self, mock_q2, backend, priority, expected_lane
    ):
        mock_q2.return_value = "q2-task-priority"

        task = _make_django_task(priority=priority)
        backend.enqueue(task, (), {})

        assert QraftTask.objects.get().priority == expected_lane


@pytest.mark.django_db
class TestGetResult:
    def _make_task(self, **overrides):
        defaults = dict(
            func="tests.test_backend.sample_task_func",
            task_args=[1],
            task_kwargs={"a": "b"},
            status=TaskStatus.PENDING,
        )
        defaults.update(overrides)
        return QraftTask.objects.create(**defaults)

    def test_pending_maps_to_ready(self, backend):
        qraft_task = self._make_task(status=TaskStatus.PENDING)

        result = backend.get_result(str(qraft_task.id))

        assert result.status == TaskResultStatus.READY
        assert result.args == [1]
        assert result.kwargs == {"a": "b"}

    def test_running_maps_to_running(self, backend):
        qraft_task = self._make_task(status=TaskStatus.RUNNING)
        QraftTaskAttempt.objects.create(
            qraft_task=qraft_task, attempt_number=1, q2_task_id="q2-1"
        )

        result = backend.get_result(str(qraft_task.id))

        assert result.status == TaskResultStatus.RUNNING

    def test_succeeded_maps_to_successful(self, backend):
        qraft_task = self._make_task(status=TaskStatus.SUCCEEDED)
        QraftTaskAttempt.objects.create(
            qraft_task=qraft_task,
            attempt_number=1,
            q2_task_id="q2-2",
            success=True,
        )

        result = backend.get_result(str(qraft_task.id))

        assert result.status == TaskResultStatus.SUCCESSFUL

    @pytest.mark.parametrize("status", [TaskStatus.FAILED, TaskStatus.EXHAUSTED])
    def test_failed_and_exhausted_map_to_failed(self, backend, status):
        qraft_task = self._make_task(status=status)
        QraftTaskAttempt.objects.create(
            qraft_task=qraft_task,
            attempt_number=1,
            q2_task_id="q2-3",
            success=False,
            exception_class="ValueError",
        )

        result = backend.get_result(str(qraft_task.id))

        assert result.status == TaskResultStatus.FAILED
        assert result.errors
        assert result.errors[0].exception_class_path == "ValueError"

    def test_unknown_id_raises_does_not_exist(self, backend):
        with pytest.raises(TaskResultDoesNotExist):
            backend.get_result("00000000-0000-0000-0000-000000000000")

    def test_malformed_id_raises_does_not_exist(self, backend):
        with pytest.raises(TaskResultDoesNotExist):
            backend.get_result("not-a-uuid")


@pytest.mark.django_db
class TestRunAfter:
    def test_creates_schedule_and_pending_qraft_task(self, backend):
        run_after = timezone.now() + timedelta(minutes=5)
        task = _make_django_task(run_after=run_after)

        result = backend.enqueue(task, (1, 2), {"x": "y"})

        qraft_task = QraftTask.objects.get()
        assert qraft_task.status == TaskStatus.PENDING
        assert qraft_task.func == "tests.test_backend.sample_task_func"
        assert qraft_task.task_args == [1, 2]
        assert qraft_task.task_kwargs == {"x": "y"}
        assert qraft_task.attempts.count() == 0
        assert result.id == str(qraft_task.id)

        schedule = Schedule.objects.get()
        # The schedule fires the unwrapping runner, not the dotted path:
        # the @task decorator makes the module attribute a non-callable
        # Task wrapper.
        assert schedule.func == "qraft.backend.run_task"
        schedule_args = ast.literal_eval(schedule.args)
        assert schedule_args[0] == "tests.test_backend.sample_task_func"
        assert schedule.schedule_type == Schedule.ONCE
        assert schedule.next_run == run_after
        assert schedule.hook == "qraft.hooks.qraft_hook_handler"

        schedule_kwargs = ast.literal_eval(schedule.kwargs)
        marker = schedule_kwargs["q_options"]["task_name"]
        assert marker == f"qraft:{qraft_task.id}:1"

    def test_get_result_is_ready_before_the_schedule_fires(self, backend):
        run_after = timezone.now() + timedelta(minutes=5)
        task = _make_django_task(run_after=run_after)
        backend.enqueue(task, (), {})
        qraft_task = QraftTask.objects.get()

        result = backend.get_result(str(qraft_task.id))

        assert result.status == TaskResultStatus.READY
        assert result.started_at is None

    def test_schedule_firing_creates_attempt_and_succeeds(self, backend):
        run_after = timezone.now() + timedelta(minutes=5)
        task = _make_django_task(run_after=run_after)
        backend.enqueue(task, (), {})
        qraft_task = QraftTask.objects.get()

        # Simulate the Schedule firing: Django-Q2 would run the target
        # function then call the hook with a Task carrying the marker
        # task_name - exactly what tests/test_hooks.py does for retries.
        marker = f"qraft:{qraft_task.id}:1"
        fired_task = Mock(
            id="q2-deferred-1",
            success=True,
            result="ok",
            stopped=timezone.now(),
        )
        fired_task.name = marker  # `name=` in Mock() sets the mock's repr, not an attr
        qraft_hook_handler(fired_task)

        result = backend.get_result(str(qraft_task.id))

        assert result.status == TaskResultStatus.SUCCESSFUL
        assert QraftTaskAttempt.objects.get().q2_task_id == "q2-deferred-1"


@pytest.mark.django_db
class TestTakesContext:
    @patch("qraft.backend.q2_async_task")
    def test_enqueue_queues_context_wrapper_not_the_real_target(self, mock_q2, backend):
        mock_q2.return_value = "q2-ctx-1"
        task = _make_django_task(func=context_task_func, takes_context=True)

        result = backend.enqueue(task, (1,), {"k": "v"})

        qraft_task = QraftTask.objects.get()
        # QraftTask metadata records the real target, not the wrapper.
        assert qraft_task.func == "tests.test_backend.context_task_func"
        assert qraft_task.task_args == [1]
        assert qraft_task.task_kwargs == {"k": "v"}
        assert result.id == str(qraft_task.id)

        # The wrapper is what's actually dispatched to Django-Q2.
        dispatched_func, dispatched_args = (
            mock_q2.call_args[0][0],
            mock_q2.call_args[0][1:],
        )
        assert dispatched_func == "qraft.backend.run_task_with_context"
        assert dispatched_args == (
            "tests.test_backend.context_task_func",
            str(qraft_task.id),
            "default",
            [1],
            {"k": "v"},
        )

    def test_wrapper_injects_task_context_with_attempt_number(self, backend):
        CONTEXT_CALLS.clear()
        qraft_task = QraftTask.objects.create(
            func="tests.test_backend.context_task_func",
            task_args=[1],
            task_kwargs={"k": "v"},
            status=TaskStatus.RUNNING,
        )
        QraftTaskAttempt.objects.create(
            qraft_task=qraft_task, attempt_number=1, q2_task_id="q2-ctx-2"
        )

        return_value = run_task_with_context(
            "tests.test_backend.context_task_func",
            str(qraft_task.id),
            "default",
            [1],
            {"k": "v"},
        )

        assert return_value == "done"
        assert len(CONTEXT_CALLS) == 1
        context, args, kwargs = CONTEXT_CALLS[0]
        assert isinstance(context, TaskContext)
        assert context.attempt == 1
        assert args == (1,)
        assert kwargs == {"k": "v"}
