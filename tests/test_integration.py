"""
Integration tests for django-qraft.

These tests verify end-to-end workflows combining multiple components.
"""

from unittest.mock import patch

import pytest

from qraft.models import HookDispatch, QraftTask, QraftTaskAttempt, TaskStatus
from qraft.tasks import async_task


@pytest.mark.django_db
class TestEndToEndWorkflows:
    """Integration tests for complete task workflows."""

    @patch("qraft.tasks.q2_async_task")
    def test_simple_task_creation_workflow(self, mock_q2_async):
        """Test creating a simple task with success hook."""
        mock_q2_async.return_value = "q2-task-123"

        # Create task with success hook
        task_id = async_task(
            "test.module.my_function",
            1,
            2,
            3,
            key="value",
            qraft_options={
                "success_hook": "test.hooks.on_success",
                "success_args": ["arg1"],
                "success_kwargs": {"kwarg1": "value1"},
            },
        )

        assert task_id == "q2-task-123"

        # Verify QraftTask was created
        qraft_task = QraftTask.objects.get()
        assert qraft_task.func == "test.module.my_function"
        assert qraft_task.task_args == [1, 2, 3]
        assert qraft_task.task_kwargs == {"key": "value"}
        assert qraft_task.success_hook == "test.hooks.on_success"
        assert qraft_task.status == TaskStatus.RUNNING

        # Verify attempt was created
        attempt = QraftTaskAttempt.objects.get()
        assert attempt.qraft_task == qraft_task
        assert attempt.attempt_number == 1
        assert attempt.q2_task_id == "q2-task-123"

    @patch("qraft.tasks.q2_async_task")
    def test_task_with_retry_and_hooks(self, mock_q2_async):
        """Test creating a task with both retry policy and hooks."""
        mock_q2_async.return_value = "q2-task-456"

        task_id = async_task(
            "test.module.flaky_function",
            qraft_options={
                "success_hook": "test.hooks.on_success",
                "failure_hook": "test.hooks.on_failure",
                "max_attempts": 3,
                "base_delay": 60.0,
                "backoff_strategy": "exponential",
                "jitter": True,
            },
        )

        assert task_id == "q2-task-456"

        # Verify task has both hooks and retry policy
        qraft_task = QraftTask.objects.get()
        assert qraft_task.success_hook == "test.hooks.on_success"
        assert qraft_task.failure_hook == "test.hooks.on_failure"
        assert qraft_task.retry_policy["max_attempts"] == 3
        assert qraft_task.retry_policy["base_delay"] == 60.0
        assert qraft_task.retry_policy["backoff_strategy"] == "exponential"

    @patch("qraft.hooks.q2_async_task")
    @patch("qraft.hooks.get_conf")
    def test_hook_dispatch_workflow(self, mock_get_conf, mock_q2_async, qraft_task, qraft_task_attempt):
        """Test complete hook dispatching workflow."""
        from unittest.mock import Mock

        from qraft.hooks import HookDispatcher

        # Configure for async hooks
        mock_conf = Mock()
        mock_conf.sync_hooks = False
        mock_get_conf.return_value = mock_conf

        # Configure task with success hook
        qraft_task.success_hook = "test.hooks.on_success"
        qraft_task.save()

        # Mock Q2 task
        q2_task = Mock()
        q2_task.success = True
        q2_task.id = qraft_task_attempt.q2_task_id

        # Mock hook task ID
        mock_q2_async.return_value = "hook-task-789"

        # Dispatch hook
        dispatcher = HookDispatcher(qraft_task, qraft_task_attempt)
        dispatcher.dispatch(q2_task)

        # Verify hook was queued
        mock_q2_async.assert_called_once()

        # Verify HookDispatch record was created
        hook_dispatch = HookDispatch.objects.get()
        assert hook_dispatch.qraft_task == qraft_task
        assert hook_dispatch.hook_type == "success"
        assert hook_dispatch.q2_task_id == "hook-task-789"

    def test_retry_workflow(self, qraft_task, qraft_task_attempt):
        """Test complete retry workflow."""
        from unittest.mock import Mock

        from qraft.hooks import HookDispatcher

        # Configure task with retry policy
        qraft_task.retry_policy = {
            "max_attempts": 3,
            "base_delay": 10.0,
            "backoff_strategy": "fixed",
            "jitter": False,
            "jitter_max": 0.0,
            "retry_exceptions": [],
            "skip_exceptions": [],
        }
        qraft_task.save()

        # Mark attempt as failed
        qraft_task_attempt.success = False
        qraft_task_attempt.exception_class = "ValueError"
        qraft_task_attempt.save()

        # Mock failed Q2 task
        q2_task = Mock()
        q2_task.success = False
        q2_task.id = qraft_task_attempt.q2_task_id

        # Dispatch (should schedule retry)
        dispatcher = HookDispatcher(qraft_task, qraft_task_attempt)
        result = dispatcher._handle_retry(q2_task)

        assert result is True

        # Verify task status is PENDING (for retry)
        qraft_task.refresh_from_db()
        assert qraft_task.status == TaskStatus.PENDING

        # Verify Schedule was created
        from django_q.models import Schedule

        schedule = Schedule.objects.get()
        assert schedule.func == qraft_task.func
        assert str(qraft_task.id) in schedule.name

    def test_retry_exhaustion_workflow(self, qraft_task, qraft_task_attempt):
        """Test workflow when retries are exhausted."""
        from unittest.mock import Mock

        from qraft.hooks import HookDispatcher

        # Configure task with max_attempts=1 (already on first attempt)
        qraft_task.retry_policy = {
            "max_attempts": 1,
            "base_delay": 10.0,
            "backoff_strategy": "fixed",
            "jitter": False,
            "jitter_max": 0.0,
            "retry_exceptions": [],
            "skip_exceptions": [],
        }
        qraft_task.save()

        # Mark attempt as failed
        qraft_task_attempt.success = False
        qraft_task_attempt.exception_class = "ValueError"
        qraft_task_attempt.save()

        # Mock failed Q2 task
        q2_task = Mock()
        q2_task.success = False
        q2_task.id = qraft_task_attempt.q2_task_id

        # Dispatch (should exhaust retries)
        dispatcher = HookDispatcher(qraft_task, qraft_task_attempt)
        result = dispatcher._handle_retry(q2_task)

        assert result is False

        # Verify task status is EXHAUSTED
        qraft_task.refresh_from_db()
        assert qraft_task.status == TaskStatus.EXHAUSTED

    @patch("qraft.tasks.q2_async_task")
    def test_full_lifecycle_success(self, mock_q2_async):
        """Test full lifecycle: create task -> complete successfully -> dispatch hook."""
        from unittest.mock import Mock

        from qraft.hooks import qraft_hook_handler

        mock_q2_async.return_value = "q2-task-success"

        # Step 1: Create task
        task_id = async_task(
            "test.function",
            qraft_options={"success_hook": "test.hooks.on_success"},
        )

        qraft_task = QraftTask.objects.get()
        attempt = QraftTaskAttempt.objects.get()

        # Step 2: Simulate task completion
        q2_task = Mock()
        q2_task.id = attempt.q2_task_id
        q2_task.name = "test_task"
        q2_task.success = True
        q2_task.result = "success result"
        q2_task.stopped = None

        # Step 3: Process hook handler
        with patch("qraft.hooks.q2_async_task") as mock_hook_async:
            mock_hook_async.return_value = "hook-task-123"

            with patch("qraft.hooks.get_conf") as mock_conf:
                mock_conf.return_value.sync_hooks = False
                qraft_hook_handler(q2_task)

        # Verify final state
        qraft_task.refresh_from_db()
        assert qraft_task.status == TaskStatus.SUCCEEDED

        attempt.refresh_from_db()
        assert attempt.success is True

        # Verify hook was dispatched
        hook_dispatch = HookDispatch.objects.get()
        assert hook_dispatch.hook_type == "success"

    @patch("qraft.tasks.q2_async_task")
    def test_full_lifecycle_failure_with_retry(self, mock_q2_async):
        """Test full lifecycle: create task -> fail -> retry -> succeed."""
        from unittest.mock import Mock

        from qraft.hooks import qraft_hook_handler

        mock_q2_async.return_value = "q2-task-retry"

        # Step 1: Create task with retry policy
        task_id = async_task(
            "test.function",
            qraft_options={
                "max_attempts": 2,
                "base_delay": 10.0,
                "backoff_strategy": "fixed",
                "jitter": False,
            },
        )

        qraft_task = QraftTask.objects.get()
        attempt1 = QraftTaskAttempt.objects.get()

        # Step 2: Simulate first attempt failure
        q2_task_fail = Mock()
        q2_task_fail.id = attempt1.q2_task_id
        q2_task_fail.name = "test_task"
        q2_task_fail.success = False
        q2_task_fail.result = "Error : ValueError: test error"
        q2_task_fail.stopped = None

        qraft_hook_handler(q2_task_fail)

        # Verify retry was scheduled
        qraft_task.refresh_from_db()
        assert qraft_task.status == TaskStatus.PENDING
        assert qraft_task.attempt_count == 1

        # Step 3: Simulate retry execution (would be scheduled by Django-Q2)
        # Create second attempt (normally created by hook handler on retry)
        attempt2 = QraftTaskAttempt.objects.create(
            qraft_task=qraft_task,
            attempt_number=2,
            q2_task_id="q2-task-retry-2",
        )

        # Step 4: Simulate second attempt success
        q2_task_success = Mock()
        q2_task_success.id = attempt2.q2_task_id
        q2_task_success.name = f"qraft:{qraft_task.id}:2"
        q2_task_success.success = True
        q2_task_success.result = "success"
        q2_task_success.stopped = None

        with patch("qraft.hooks.HookDispatcher") as mock_dispatcher:
            qraft_hook_handler(q2_task_success)

        # Verify final state
        qraft_task.refresh_from_db()
        assert qraft_task.status == TaskStatus.SUCCEEDED
        assert qraft_task.attempt_count == 2

        attempt2.refresh_from_db()
        assert attempt2.success is True
