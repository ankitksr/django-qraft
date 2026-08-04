"""
Tests for qraft.backend.QraftTaskBackend (django.tasks / DEP 14 engine).

Requires Django 6.0+ (django.tasks). Skipped entirely otherwise so the main
suite stays green on older Django installs.
"""

import pytest

pytest.importorskip("django.tasks")

from unittest.mock import patch  # noqa: E402

from django.tasks import task_backends  # noqa: E402
from django.tasks.base import Task, TaskResultStatus  # noqa: E402
from django.tasks.exceptions import TaskResultDoesNotExist  # noqa: E402

from qraft.models import QraftTask, QraftTaskAttempt, TaskStatus  # noqa: E402


def sample_task_func(*args, **kwargs):
    """Module-level stand-in task function for django.tasks validation."""


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
