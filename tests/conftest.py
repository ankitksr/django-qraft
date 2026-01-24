"""
Pytest configuration and shared fixtures for django-qraft tests.
"""

import os
import sys
from pathlib import Path
from unittest.mock import Mock

import pytest

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


@pytest.fixture
def qraft_task(db):
    """Create a test QraftTask instance."""
    from qraft.models import QraftTask, TaskStatus

    return QraftTask.objects.create(
        func="test.module.test_function",
        task_args=[1, 2, 3],
        task_kwargs={"key": "value"},
        success_hook="test.hooks.success",
        success_args=[],
        success_kwargs={},
        failure_hook="test.hooks.failure",
        failure_args=[],
        failure_kwargs={},
        retry_policy={
            "max_attempts": 3,
            "base_delay": 10.0,
            "backoff_strategy": "exponential",
            "jitter": False,
            "jitter_max": 0.2,
            "retry_exceptions": [],
            "skip_exceptions": [],
        },
        status=TaskStatus.PENDING,
    )


@pytest.fixture
def qraft_task_attempt(db, qraft_task):
    """Create a test QraftTaskAttempt instance."""
    from qraft.models import QraftTaskAttempt

    return QraftTaskAttempt.objects.create(
        qraft_task=qraft_task,
        attempt_number=1,
        q2_task_id="test-task-id-123",
        success=None,
    )


@pytest.fixture
def mock_q2_task_success():
    """Create a mock successful Django-Q2 task."""
    task = Mock()
    task.id = "q2-task-123"
    task.name = "test_task"
    task.success = True
    task.result = "success result"
    task.stopped = None
    return task


@pytest.fixture
def mock_q2_task_failure():
    """Create a mock failed Django-Q2 task."""
    task = Mock()
    task.id = "q2-task-456"
    task.name = "test_task"
    task.success = False
    task.result = "Error message : Traceback\nValueError: test error"
    task.stopped = None
    return task


@pytest.fixture
def retry_policy():
    """Create a test RetryPolicy instance."""
    from qraft.retry import RetryPolicy

    return RetryPolicy(
        max_attempts=3,
        base_delay=10.0,
        backoff_strategy="exponential",
        jitter=False,
        jitter_max=0.2,
    )
