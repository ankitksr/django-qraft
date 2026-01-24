"""Tests for qraft.tasks module."""

from unittest.mock import MagicMock, patch

import pytest

from qraft.models import QraftTask, QraftTaskAttempt, TaskStatus
from qraft.tasks import async_task


@pytest.mark.django_db
class TestAsyncTask:
    """Tests for async_task function."""

    @patch("qraft.tasks.q2_async_task")
    def test_minimal_task_creation(self, mock_q2_async):
        """Test creating a task with minimal parameters."""
        mock_q2_async.return_value = "q2-task-123"

        result = async_task("test.module.function", 1, 2, 3)

        assert result == "q2-task-123"

        # Verify QraftTask was created
        qraft_task = QraftTask.objects.get()
        assert qraft_task.func == "test.module.function"
        assert qraft_task.task_args == [1, 2, 3]
        assert qraft_task.task_kwargs == {}
        assert qraft_task.status == TaskStatus.RUNNING

        # Verify attempt was created
        attempt = QraftTaskAttempt.objects.get()
        assert attempt.qraft_task == qraft_task
        assert attempt.attempt_number == 1
        assert attempt.q2_task_id == "q2-task-123"

    @patch("qraft.tasks.q2_async_task")
    def test_task_with_kwargs(self, mock_q2_async):
        """Test creating a task with keyword arguments."""
        mock_q2_async.return_value = "q2-task-456"

        async_task("test.function", 1, 2, key1="value1", key2="value2")

        qraft_task = QraftTask.objects.get()
        assert qraft_task.task_kwargs == {"key1": "value1", "key2": "value2"}

    @patch("qraft.tasks.q2_async_task")
    def test_task_with_success_hook(self, mock_q2_async):
        """Test creating a task with success hook."""
        mock_q2_async.return_value = "q2-task-789"

        async_task(
            "test.function",
            qraft_options={
                "success_hook": "test.hooks.on_success",
                "success_args": [1, 2],
                "success_kwargs": {"key": "value"},
            },
        )

        qraft_task = QraftTask.objects.get()
        assert qraft_task.success_hook == "test.hooks.on_success"
        assert qraft_task.success_args == [1, 2]
        assert qraft_task.success_kwargs == {"key": "value"}

    @patch("qraft.tasks.q2_async_task")
    def test_task_with_failure_hook(self, mock_q2_async):
        """Test creating a task with failure hook."""
        mock_q2_async.return_value = "q2-task-999"

        async_task(
            "test.function",
            qraft_options={
                "failure_hook": "test.hooks.on_failure",
                "failure_args": [3, 4],
                "failure_kwargs": {"error": "msg"},
            },
        )

        qraft_task = QraftTask.objects.get()
        assert qraft_task.failure_hook == "test.hooks.on_failure"
        assert qraft_task.failure_args == [3, 4]
        assert qraft_task.failure_kwargs == {"error": "msg"}

    @patch("qraft.tasks.q2_async_task")
    def test_task_with_retry_policy(self, mock_q2_async):
        """Test creating a task with retry policy."""
        mock_q2_async.return_value = "q2-task-retry"

        async_task(
            "test.function",
            qraft_options={
                "max_attempts": 5,
                "base_delay": 60.0,
                "backoff_strategy": "linear",
                "jitter": False,
            },
        )

        qraft_task = QraftTask.objects.get()
        assert qraft_task.retry_policy["max_attempts"] == 5
        assert qraft_task.retry_policy["base_delay"] == 60.0
        assert qraft_task.retry_policy["backoff_strategy"] == "linear"
        assert qraft_task.retry_policy["jitter"] is False

    @patch("qraft.tasks.q2_async_task")
    def test_task_with_callable_func(self, mock_q2_async):
        """Test creating a task with a callable function."""
        mock_q2_async.return_value = "q2-task-callable"

        def test_function():
            pass

        async_task(test_function)

        qraft_task = QraftTask.objects.get()
        # Should extract module.name from callable
        assert "test_function" in qraft_task.func

    @patch("qraft.tasks.q2_async_task")
    def test_legacy_hook_deprecation_warning(self, mock_q2_async):
        """Test deprecation warning for legacy hook parameter."""
        mock_q2_async.return_value = "q2-task-legacy"

        with pytest.warns(DeprecationWarning, match="hook.*deprecated"):
            async_task("test.function", hook="test.legacy_hook")

        qraft_task = QraftTask.objects.get()
        # Legacy hook should be converted to success_hook
        assert qraft_task.success_hook == "test.legacy_hook"

    @patch("qraft.tasks.q2_async_task")
    def test_legacy_hook_ignored_with_qraft_hooks(self, mock_q2_async):
        """Test legacy hook is ignored when qraft hooks are provided."""
        mock_q2_async.return_value = "q2-task-override"

        with pytest.warns(DeprecationWarning, match="Ignoring legacy"):
            async_task(
                "test.function",
                hook="test.legacy_hook",
                qraft_options={"success_hook": "test.new_hook"},
            )

        qraft_task = QraftTask.objects.get()
        assert qraft_task.success_hook == "test.new_hook"

    @patch("qraft.tasks.q2_async_task")
    def test_preserves_task_name(self, mock_q2_async):
        """Test that user's task_name is preserved."""
        mock_q2_async.return_value = "q2-task-name"

        async_task("test.function", task_name="My Custom Task")

        # Verify task_name was passed to q2_async_task
        call_kwargs = mock_q2_async.call_args[1]
        assert call_kwargs["task_name"] == "My Custom Task"

    @patch("qraft.tasks.q2_async_task")
    def test_passes_django_q2_parameters(self, mock_q2_async):
        """Test that Django-Q2 parameters are passed through."""
        mock_q2_async.return_value = "q2-task-params"

        async_task(
            "test.function",
            group="my-group",
            timeout=300,
            save=True,
            ack_failure=True,
        )

        call_kwargs = mock_q2_async.call_args[1]
        assert call_kwargs["group"] == "my-group"
        assert call_kwargs["timeout"] == 300
        assert call_kwargs["save"] is True
        assert call_kwargs["ack_failure"] is True

    @patch("qraft.tasks.q2_async_task")
    def test_sets_qraft_hook_handler(self, mock_q2_async):
        """Test that qraft_hook_handler is set as the hook."""
        mock_q2_async.return_value = "q2-task-hook"

        async_task("test.function")

        call_kwargs = mock_q2_async.call_args[1]
        assert call_kwargs["hook"] == "qraft.hooks.qraft_hook_handler"

    @patch("qraft.tasks.q2_async_task")
    def test_no_retry_policy_when_not_specified(self, mock_q2_async):
        """Test that retry_policy is empty when not specified."""
        mock_q2_async.return_value = "q2-task-no-retry"

        async_task("test.function", qraft_options={})

        qraft_task = QraftTask.objects.get()
        assert qraft_task.retry_policy == {}

    @patch("qraft.tasks.q2_async_task")
    def test_mixed_retry_and_hook_options(self, mock_q2_async):
        """Test task with both retry and hook options."""
        mock_q2_async.return_value = "q2-task-mixed"

        async_task(
            "test.function",
            1,
            2,
            key="value",
            qraft_options={
                "success_hook": "test.on_success",
                "failure_hook": "test.on_failure",
                "max_attempts": 4,
                "base_delay": 45.0,
                "backoff_strategy": "exponential",
            },
        )

        qraft_task = QraftTask.objects.get()

        # Verify hooks
        assert qraft_task.success_hook == "test.on_success"
        assert qraft_task.failure_hook == "test.on_failure"

        # Verify retry policy
        assert qraft_task.retry_policy["max_attempts"] == 4
        assert qraft_task.retry_policy["base_delay"] == 45.0
        assert qraft_task.retry_policy["backoff_strategy"] == "exponential"

        # Verify task args
        assert qraft_task.task_args == [1, 2]
        assert qraft_task.task_kwargs == {"key": "value"}
