"""
Pytest configuration and shared fixtures for django-qraft tests.
"""

import os
import sys
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


def pytest_collection_modifyitems(config, items):
    """Skip `postgres`-marked cases unless the suite runs against Postgres."""
    if os.environ.get("QRAFT_TEST_DATABASE_URL", "").startswith("postgres"):
        return
    skip = pytest.mark.skip(reason="needs QRAFT_TEST_DATABASE_URL (Postgres)")
    for item in items:
        if "postgres" in item.keywords:
            item.add_marker(skip)


@pytest.fixture(autouse=True)
def _fresh_execution_context():
    """
    Each test starts with no bound attempt and no delivery claim.

    The claim is memoised on a ContextVar keyed by q2 task id, and fixtures
    reuse ids across tests; without this a later test would inherit an
    earlier one's verdict instead of taking its own compare-and-swap.
    """
    from qraft import context

    context.clear_context()
    yield
    context.clear_context()


@pytest.fixture(autouse=True)
def _fresh_metrics_sink():
    """Each test sees the configured sink and a zeroed health counter."""
    from qraft import metrics

    metrics.reset_sink()
    yield
    metrics.reset_sink()


@pytest.fixture
def _disable_hook_validation():
    """Disable hook path import validation for tests using fake module paths."""
    with (
        patch("qraft.base._validate_hook_path"),
        patch("qraft.batch._validate_hook_path"),
        patch("qraft.chain._validate_hook_path"),
        patch("qraft.iter._validate_hook_path"),
    ):
        yield


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


class RecordingSink:
    """Metrics sink that keeps every emission for assertions."""

    def __init__(self):
        self.calls: list[tuple[str, str, float, dict]] = []

    def counter(self, name, value=1, **labels):
        self.calls.append(("counter", name, value, labels))

    def histogram(self, name, value, **labels):
        self.calls.append(("histogram", name, value, labels))

    def gauge(self, name, value, **labels):
        self.calls.append(("gauge", name, value, labels))

    def names(self, kind=None) -> list[str]:
        return [name for k, name, _, _ in self.calls if kind is None or k == kind]

    def labels(self, name) -> list[dict]:
        return [labels for _, n, _, labels in self.calls if n == name]

    def values(self, name) -> list[float]:
        return [value for _, n, value, _ in self.calls if n == name]


@pytest.fixture
def recording_sink(monkeypatch):
    """Route every metric emission into a RecordingSink for the test."""
    from qraft import metrics

    sink = RecordingSink()
    monkeypatch.setattr(metrics, "get_sink", lambda: sink)
    return sink


@pytest.fixture
def signal_log():
    """
    Connect recorders to every qraft signal; yields {signal_name: [payloads]}.

    Sends ride `transaction.on_commit`, so pair this with
    `django_capture_on_commit_callbacks(execute=True)` around the action.
    """
    from qraft import signals

    names = (
        "task_started",
        "attempt_finished",
        "task_settled",
        "workflow_settled",
        "attempt_stall_suspected",
        "run_settled",
        "run_overdue",
    )
    log: dict[str, list] = {name: [] for name in names}
    receivers = []
    for name in names:

        def receiver(sender, payload, _name=name, **kwargs):
            log[_name].append(payload)

        getattr(signals, name).connect(receiver, weak=False)
        receivers.append((name, receiver))
    yield log
    for name, receiver in receivers:
        getattr(signals, name).disconnect(receiver)
