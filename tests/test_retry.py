"""Tests for qraft.retry module."""

from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest

from qraft.models import QraftTask, QraftTaskAttempt, TaskStatus
from qraft.retry import RetryPolicy, handle_task_retry


class TestRetryPolicy:
    """Tests for RetryPolicy class."""

    def test_init_with_defaults(self):
        """Test initialization with default values from config."""
        policy = RetryPolicy()

        assert policy.max_attempts == 3  # From test config
        assert policy.base_delay == 30.0
        assert policy.backoff_strategy == "exponential"
        assert policy.jitter is True
        assert policy.jitter_max == 0.2

    def test_init_with_custom_values(self):
        """Test initialization with custom values."""
        policy = RetryPolicy(
            max_attempts=5,
            base_delay=60.0,
            backoff_strategy="linear",
            jitter=False,
            jitter_max=0.5,
        )

        assert policy.max_attempts == 5
        assert policy.base_delay == 60.0
        assert policy.backoff_strategy == "linear"
        assert policy.jitter is False
        assert policy.jitter_max == 0.5

    def test_init_with_exception_filters(self):
        """Test initialization with exception filters."""
        policy = RetryPolicy(
            retry_exceptions=["ValueError", "TypeError"],
            skip_exceptions=["KeyError"],
        )

        assert policy.retry_exceptions == {"ValueError", "TypeError"}
        assert policy.skip_exceptions == {"KeyError"}

    def test_validation_max_attempts(self):
        """Test validation of max_attempts."""
        with pytest.raises(ValueError, match="max_attempts must be between 1 and 10"):
            RetryPolicy(max_attempts=0)

        with pytest.raises(ValueError, match="max_attempts must be between 1 and 10"):
            RetryPolicy(max_attempts=11)

    def test_validation_jitter_max(self):
        """Test validation of jitter_max."""
        with pytest.raises(ValueError, match="jitter_max must be between 0.0 and 1.0"):
            RetryPolicy(jitter_max=-0.1)

        with pytest.raises(ValueError, match="jitter_max must be between 0.0 and 1.0"):
            RetryPolicy(jitter_max=1.5)

    def test_validation_backoff_strategy(self):
        """Test validation of backoff strategy."""
        with pytest.raises(ValueError, match="Invalid backoff strategy"):
            RetryPolicy(backoff_strategy="invalid")

    def test_from_dict(self):
        """Test creating policy from dictionary."""
        data = {
            "max_attempts": 4,
            "base_delay": 45.0,
            "backoff_strategy": "fixed",
            "jitter": True,
            "jitter_max": 0.3,
            "retry_exceptions": ["ValueError"],
            "skip_exceptions": ["KeyError"],
        }

        policy = RetryPolicy.from_dict(data)

        assert policy.max_attempts == 4
        assert policy.base_delay == 45.0
        assert policy.backoff_strategy == "fixed"
        assert policy.jitter is True
        assert policy.jitter_max == 0.3
        assert policy.retry_exceptions == {"ValueError"}
        assert policy.skip_exceptions == {"KeyError"}

    @pytest.mark.django_db
    def test_from_task(self):
        """Test creating policy from QraftTask."""
        task = QraftTask.objects.create(
            func="test.function",
            retry_policy={
                "max_attempts": 5,
                "base_delay": 20.0,
                "backoff_strategy": "linear",
                "jitter": False,
                "jitter_max": 0.1,
                "retry_exceptions": [],
                "skip_exceptions": [],
            },
        )

        policy = RetryPolicy.from_task(task)

        assert policy.max_attempts == 5
        assert policy.base_delay == 20.0
        assert policy.backoff_strategy == "linear"

    def test_from_options(self):
        """Test creating policy from qraft_options."""
        qraft_options = {
            "max_attempts": 4,
            "base_delay": 15.0,
            "backoff_strategy": "exponential",
            "some_other_option": "ignored",
        }

        policy = RetryPolicy.from_options(qraft_options)

        assert policy is not None
        assert policy.max_attempts == 4
        assert policy.base_delay == 15.0
        assert policy.backoff_strategy == "exponential"

    def test_from_options_no_retry_options(self):
        """Test from_options returns None when no retry options."""
        qraft_options = {"some_other_option": "value"}

        policy = RetryPolicy.from_options(qraft_options)

        assert policy is None

    def test_to_dict(self):
        """Test converting policy to dictionary."""
        policy = RetryPolicy(
            max_attempts=5,
            base_delay=60.0,
            backoff_strategy="linear",
            jitter=True,
            jitter_max=0.4,
            retry_exceptions=["ValueError", "TypeError"],
            skip_exceptions=["KeyError"],
        )

        data = policy.to_dict()

        assert data["max_attempts"] == 5
        assert data["base_delay"] == 60.0
        assert data["backoff_strategy"] == "linear"
        assert data["jitter"] is True
        assert data["jitter_max"] == 0.4
        # Sets are converted to lists, order doesn't matter
        assert set(data["retry_exceptions"]) == {"ValueError", "TypeError"}
        assert set(data["skip_exceptions"]) == {"KeyError"}

    def test_should_retry_within_max_attempts(self):
        """Test should_retry returns True within max attempts."""
        policy = RetryPolicy(max_attempts=3)

        assert policy.should_retry(1) is True
        assert policy.should_retry(2) is True

    def test_should_retry_exceeds_max_attempts(self):
        """Test should_retry returns False when max attempts reached."""
        policy = RetryPolicy(max_attempts=3)

        assert policy.should_retry(3) is False
        assert policy.should_retry(4) is False

    def test_should_retry_with_allowed_exception(self):
        """Test should_retry with exception in retry list."""
        policy = RetryPolicy(
            max_attempts=3,
            retry_exceptions=["ValueError", "TypeError"],
        )

        assert policy.should_retry(1, "ValueError") is True
        assert policy.should_retry(1, ValueError("test")) is True

    def test_should_retry_with_disallowed_exception(self):
        """Test should_retry with exception not in retry list."""
        policy = RetryPolicy(
            max_attempts=3,
            retry_exceptions=["ValueError"],
        )

        assert policy.should_retry(1, "KeyError") is False
        assert policy.should_retry(1, KeyError("test")) is False

    def test_should_retry_with_skip_exception(self):
        """Test should_retry with exception in skip list."""
        policy = RetryPolicy(
            max_attempts=3,
            skip_exceptions=["KeyError"],
        )

        assert policy.should_retry(1, "KeyError") is False
        assert policy.should_retry(1, KeyError("test")) is False

    def test_should_retry_skip_takes_precedence(self):
        """Test that skip_exceptions takes precedence over retry_exceptions."""
        policy = RetryPolicy(
            max_attempts=3,
            retry_exceptions=["ValueError"],
            skip_exceptions=["ValueError"],
        )

        assert policy.should_retry(1, "ValueError") is False

    def test_calculate_delay_fixed(self):
        """Test delay calculation with fixed backoff."""
        policy = RetryPolicy(
            base_delay=10.0,
            backoff_strategy="fixed",
            jitter=False,
        )

        assert policy.calculate_delay(1) == 10
        assert policy.calculate_delay(2) == 10
        assert policy.calculate_delay(3) == 10

    def test_calculate_delay_linear(self):
        """Test delay calculation with linear backoff."""
        policy = RetryPolicy(
            base_delay=10.0,
            backoff_strategy="linear",
            jitter=False,
        )

        assert policy.calculate_delay(1) == 10
        assert policy.calculate_delay(2) == 20
        assert policy.calculate_delay(3) == 30

    def test_calculate_delay_exponential(self):
        """Test delay calculation with exponential backoff."""
        policy = RetryPolicy(
            base_delay=10.0,
            backoff_strategy="exponential",
            jitter=False,
        )

        assert policy.calculate_delay(1) == 10   # 10 * 2^0
        assert policy.calculate_delay(2) == 20   # 10 * 2^1
        assert policy.calculate_delay(3) == 40   # 10 * 2^2
        assert policy.calculate_delay(4) == 80   # 10 * 2^3

    def test_calculate_delay_with_jitter(self):
        """Test delay calculation with jitter."""
        policy = RetryPolicy(
            base_delay=100.0,
            backoff_strategy="fixed",
            jitter=True,
            jitter_max=0.2,
        )

        # Jitter should add randomness within ±20% of base delay
        delays = [policy.calculate_delay(1) for _ in range(10)]

        # All delays should be positive
        assert all(d > 0 for d in delays)

        # Delays should vary (not all the same)
        assert len(set(delays)) > 1

        # Delays should be within expected range (80-120 seconds)
        assert all(80 <= d <= 120 for d in delays)

    def test_next_eta(self):
        """Test ETA calculation for next retry."""
        policy = RetryPolicy(
            base_delay=60.0,
            backoff_strategy="fixed",
            jitter=False,
        )

        before = datetime.now(timezone.utc)
        eta = policy.next_eta(1)
        after = datetime.now(timezone.utc)

        # ETA should be approximately 60 seconds in the future
        expected_min = before + timedelta(seconds=60)
        expected_max = after + timedelta(seconds=60)

        assert expected_min <= eta <= expected_max

    @pytest.mark.django_db
    def test_schedule_retry(self):
        """Test scheduling a retry using Django-Q2 Schedule."""
        from django_q.models import Schedule

        task = QraftTask.objects.create(
            func="test.module.function",
            task_args=[1, 2],
            task_kwargs={"key": "value"},
            retry_policy={
                "max_attempts": 3,
                "base_delay": 10.0,
                "backoff_strategy": "fixed",
                "jitter": False,
                "jitter_max": 0.0,
                "retry_exceptions": [],
                "skip_exceptions": [],
            },
        )

        # Create initial attempt
        QraftTaskAttempt.objects.create(
            qraft_task=task,
            attempt_number=1,
            q2_task_id="task-1",
            success=False,
        )

        policy = RetryPolicy.from_task(task)
        schedule_id = policy.schedule_retry(task, current_attempt=1)

        # Verify schedule was created
        schedule = Schedule.objects.get(id=schedule_id)
        assert schedule.func == "test.module.function"
        assert schedule.schedule_type == Schedule.ONCE
        assert "qraft_retry:" in schedule.name
        assert str(task.id) in schedule.name

        # Verify task status updated to PENDING
        task.refresh_from_db()
        assert task.status == TaskStatus.PENDING

    @pytest.mark.django_db
    def test_schedule_retry_with_backoff(self):
        """Test that retry scheduling uses correct delay."""
        from django_q.models import Schedule

        task = QraftTask.objects.create(
            func="test.function",
            retry_policy={
                "max_attempts": 3,
                "base_delay": 60.0,
                "backoff_strategy": "exponential",
                "jitter": False,
                "jitter_max": 0.0,
                "retry_exceptions": [],
                "skip_exceptions": [],
            },
        )

        # Create initial attempt
        QraftTaskAttempt.objects.create(
            qraft_task=task,
            attempt_number=1,
            q2_task_id="task-1",
            success=False,
        )

        policy = RetryPolicy.from_task(task)

        before = datetime.now(timezone.utc)
        schedule_id = policy.schedule_retry(task, current_attempt=1)
        after = datetime.now(timezone.utc)

        schedule = Schedule.objects.get(id=schedule_id)

        # For attempt 1, exponential backoff: 60 * 2^0 = 60 seconds
        expected_min = before + timedelta(seconds=60)
        expected_max = after + timedelta(seconds=60)

        assert expected_min <= schedule.next_run <= expected_max


class TestHandleTaskRetry:
    """Tests for handle_task_retry function."""

    @pytest.mark.django_db
    def test_schedules_retry_when_policy_allows(self):
        """Test that handle_task_retry schedules a retry when policy allows."""
        task = QraftTask.objects.create(
            func="test.function",
            retry_policy={
                "max_attempts": 3,
                "base_delay": 10.0,
                "backoff_strategy": "fixed",
                "jitter": False,
                "jitter_max": 0.0,
                "retry_exceptions": [],
                "skip_exceptions": [],
            },
        )

        attempt = QraftTaskAttempt.objects.create(
            qraft_task=task,
            attempt_number=1,
            q2_task_id="task-1",
            success=False,
            exception_class="ValueError",
        )

        result = handle_task_retry(task, attempt)

        assert result is True

        # Verify task status is PENDING (for retry)
        task.refresh_from_db()
        assert task.status == TaskStatus.PENDING

    @pytest.mark.django_db
    def test_marks_exhausted_when_retries_done(self):
        """Test that handle_task_retry marks task as EXHAUSTED when retries are done."""
        task = QraftTask.objects.create(
            func="test.function",
            retry_policy={
                "max_attempts": 1,
                "base_delay": 10.0,
                "backoff_strategy": "fixed",
                "jitter": False,
                "jitter_max": 0.0,
                "retry_exceptions": [],
                "skip_exceptions": [],
            },
        )

        attempt = QraftTaskAttempt.objects.create(
            qraft_task=task,
            attempt_number=1,
            q2_task_id="task-1",
            success=False,
            exception_class="ValueError",
        )

        result = handle_task_retry(task, attempt)

        assert result is False

        # Verify task status is EXHAUSTED
        task.refresh_from_db()
        assert task.status == TaskStatus.EXHAUSTED

    @pytest.mark.django_db
    def test_returns_false_when_no_policy(self):
        """Test that handle_task_retry returns False when no retry policy exists."""
        task = QraftTask.objects.create(
            func="test.function",
            retry_policy={},
        )

        attempt = QraftTaskAttempt.objects.create(
            qraft_task=task,
            attempt_number=1,
            q2_task_id="task-1",
            success=False,
            exception_class="ValueError",
        )

        result = handle_task_retry(task, attempt)

        assert result is False
