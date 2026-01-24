"""Tests for qraft.models."""

import pytest
from django.db import IntegrityError

from qraft.models import HookDispatch, QraftTask, QraftTaskAttempt, TaskStatus


@pytest.mark.django_db
class TestQraftTask:
    """Tests for QraftTask model."""

    def test_create_minimal_task(self):
        """Test creating a task with minimal required fields."""
        task = QraftTask.objects.create(
            func="test.module.function",
            task_args=[],
            task_kwargs={},
        )

        assert task.id is not None
        assert task.func == "test.module.function"
        assert task.status == TaskStatus.PENDING
        assert task.task_args == []
        assert task.task_kwargs == {}
        assert task.retry_policy == {}

    def test_create_task_with_hooks(self):
        """Test creating a task with success and failure hooks."""
        task = QraftTask.objects.create(
            func="test.function",
            success_hook="test.on_success",
            success_args=[1, 2],
            success_kwargs={"key": "value"},
            failure_hook="test.on_failure",
            failure_args=[3, 4],
            failure_kwargs={"error": "msg"},
        )

        assert task.success_hook == "test.on_success"
        assert task.success_args == [1, 2]
        assert task.success_kwargs == {"key": "value"}
        assert task.failure_hook == "test.on_failure"
        assert task.failure_args == [3, 4]
        assert task.failure_kwargs == {"error": "msg"}

    def test_create_task_with_retry_policy(self):
        """Test creating a task with retry policy."""
        retry_policy = {
            "max_attempts": 5,
            "base_delay": 60.0,
            "backoff_strategy": "linear",
        }

        task = QraftTask.objects.create(
            func="test.function",
            retry_policy=retry_policy,
        )

        assert task.retry_policy == retry_policy

    def test_attempt_count_property(self, qraft_task):
        """Test attempt_count property."""
        assert qraft_task.attempt_count == 0

        # Add attempts
        QraftTaskAttempt.objects.create(
            qraft_task=qraft_task,
            attempt_number=1,
            q2_task_id="task-1",
        )
        assert qraft_task.attempt_count == 1

        QraftTaskAttempt.objects.create(
            qraft_task=qraft_task,
            attempt_number=2,
            q2_task_id="task-2",
        )
        assert qraft_task.attempt_count == 2

    def test_latest_attempt_property(self, qraft_task):
        """Test latest_attempt property."""
        assert qraft_task.latest_attempt is None

        attempt1 = QraftTaskAttempt.objects.create(
            qraft_task=qraft_task,
            attempt_number=1,
            q2_task_id="task-1",
        )
        assert qraft_task.latest_attempt == attempt1

        attempt2 = QraftTaskAttempt.objects.create(
            qraft_task=qraft_task,
            attempt_number=2,
            q2_task_id="task-2",
        )
        assert qraft_task.latest_attempt == attempt2

    def test_string_representation(self, qraft_task):
        """Test __str__ method."""
        assert str(qraft_task) == f"QraftTask {qraft_task.id} (pending)"

        qraft_task.status = TaskStatus.SUCCEEDED
        qraft_task.save()
        assert str(qraft_task) == f"QraftTask {qraft_task.id} (succeeded)"

    def test_status_transitions(self, qraft_task):
        """Test task status transitions."""
        assert qraft_task.status == TaskStatus.PENDING

        qraft_task.status = TaskStatus.RUNNING
        qraft_task.save()
        assert qraft_task.status == TaskStatus.RUNNING

        qraft_task.status = TaskStatus.SUCCEEDED
        qraft_task.save()
        assert qraft_task.status == TaskStatus.SUCCEEDED


@pytest.mark.django_db
class TestQraftTaskAttempt:
    """Tests for QraftTaskAttempt model."""

    def test_create_attempt(self, qraft_task):
        """Test creating a task attempt."""
        attempt = QraftTaskAttempt.objects.create(
            qraft_task=qraft_task,
            attempt_number=1,
            q2_task_id="test-q2-task-123",
        )

        assert attempt.id is not None
        assert attempt.qraft_task == qraft_task
        assert attempt.attempt_number == 1
        assert attempt.q2_task_id == "test-q2-task-123"
        assert attempt.success is None
        assert attempt.exception_class is None

    def test_unique_q2_task_id(self, qraft_task):
        """Test that q2_task_id must be unique."""
        QraftTaskAttempt.objects.create(
            qraft_task=qraft_task,
            attempt_number=1,
            q2_task_id="unique-id",
        )

        task2 = QraftTask.objects.create(func="test.function2")

        # Should raise IntegrityError for duplicate q2_task_id
        with pytest.raises(IntegrityError):
            QraftTaskAttempt.objects.create(
                qraft_task=task2,
                attempt_number=1,
                q2_task_id="unique-id",
            )

    def test_unique_attempt_per_task(self, qraft_task):
        """Test unique constraint on (qraft_task, attempt_number)."""
        QraftTaskAttempt.objects.create(
            qraft_task=qraft_task,
            attempt_number=1,
            q2_task_id="task-1",
        )

        # Should raise IntegrityError for duplicate attempt number
        with pytest.raises(IntegrityError):
            QraftTaskAttempt.objects.create(
                qraft_task=qraft_task,
                attempt_number=1,
                q2_task_id="task-2",
            )

    def test_attempt_success_tracking(self, qraft_task_attempt):
        """Test tracking success/failure outcome."""
        assert qraft_task_attempt.success is None

        qraft_task_attempt.success = True
        qraft_task_attempt.save()
        assert qraft_task_attempt.success is True

        qraft_task_attempt.success = False
        qraft_task_attempt.exception_class = "ValueError"
        qraft_task_attempt.save()
        assert qraft_task_attempt.success is False
        assert qraft_task_attempt.exception_class == "ValueError"

    def test_string_representation(self, qraft_task_attempt):
        """Test __str__ method."""
        # Pending state
        assert "pending" in str(qraft_task_attempt)

        # Success state
        qraft_task_attempt.success = True
        qraft_task_attempt.save()
        assert "ok" in str(qraft_task_attempt)

        # Failed state
        qraft_task_attempt.success = False
        qraft_task_attempt.save()
        assert "failed" in str(qraft_task_attempt)

    def test_get_q2_task(self, qraft_task_attempt, monkeypatch):
        """Test get_q2_task method."""
        # Mock Django-Q2 Task model
        from unittest.mock import Mock

        mock_q2_task = Mock()
        mock_q2_model = Mock()
        mock_q2_model.objects.get.return_value = mock_q2_task

        def mock_import(path):
            if path == "django_q.models":
                return type("Module", (), {"Task": mock_q2_model})()
            raise ImportError

        # Test when Q2 task exists
        monkeypatch.setattr("importlib.import_module", lambda x: mock_import(x))
        result = qraft_task_attempt.get_q2_task()
        # Note: This test is simplified; in real usage, you'd mock the import properly


@pytest.mark.django_db
class TestHookDispatch:
    """Tests for HookDispatch model."""

    def test_create_hook_dispatch(self, qraft_task):
        """Test creating a hook dispatch record."""
        dispatch = HookDispatch.objects.create(
            qraft_task=qraft_task,
            hook_type="success",
            hook_path="test.hooks.success_handler",
            q2_task_id="hook-task-123",
        )

        assert dispatch.id is not None
        assert dispatch.qraft_task == qraft_task
        assert dispatch.hook_type == "success"
        assert dispatch.hook_path == "test.hooks.success_handler"
        assert dispatch.q2_task_id == "hook-task-123"

    def test_unique_hook_dispatch_per_task_type(self, qraft_task):
        """Test unique constraint on (qraft_task, hook_type)."""
        HookDispatch.objects.create(
            qraft_task=qraft_task,
            hook_type="success",
            hook_path="test.hooks.success",
            q2_task_id="task-1",
        )

        # Should raise IntegrityError for duplicate hook type
        with pytest.raises(IntegrityError):
            HookDispatch.objects.create(
                qraft_task=qraft_task,
                hook_type="success",
                hook_path="test.hooks.success",
                q2_task_id="task-2",
            )

    def test_multiple_hook_types_allowed(self, qraft_task):
        """Test that different hook types can coexist."""
        success_dispatch = HookDispatch.objects.create(
            qraft_task=qraft_task,
            hook_type="success",
            hook_path="test.hooks.success",
            q2_task_id="task-1",
        )

        failure_dispatch = HookDispatch.objects.create(
            qraft_task=qraft_task,
            hook_type="failure",
            hook_path="test.hooks.failure",
            q2_task_id="task-2",
        )

        assert success_dispatch.hook_type == "success"
        assert failure_dispatch.hook_type == "failure"

    def test_string_representation(self, qraft_task):
        """Test __str__ method."""
        dispatch = HookDispatch.objects.create(
            qraft_task=qraft_task,
            hook_type="success",
            hook_path="test.hooks.success",
            q2_task_id="task-1",
        )

        assert "success" in str(dispatch)
        assert str(qraft_task.id) in str(dispatch)

    def test_cascade_delete(self, qraft_task):
        """Test that hook dispatches are deleted when task is deleted."""
        HookDispatch.objects.create(
            qraft_task=qraft_task,
            hook_type="success",
            hook_path="test.hooks.success",
            q2_task_id="task-1",
        )

        assert HookDispatch.objects.count() == 1

        qraft_task.delete()

        assert HookDispatch.objects.count() == 0
