"""
Rich retry policy implementation for Django-Qraft.
Supports multiple backoff strategies, jitter, and exception-specific retry logic.
"""

import logging
import random
from datetime import datetime, timedelta, timezone
from typing import Any

from django.db import transaction

from .conf import RetryBackoff, conf

logger = logging.getLogger("qraft")


class RetryPolicy:
    """
    Retry policy with support for multiple backoff strategies, jitter,
    and conditional retry based on exception types.

    Uses global settings from conf.retry_defaults as defaults.
    """

    def __init__(
        self,
        max_attempts: int | None = None,
        base_delay: float | None = None,
        backoff_strategy: str | None = None,
        jitter: bool | None = None,
        jitter_max: float | None = None,
        retry_exceptions: list[str] | None = None,
        skip_exceptions: list[str] | None = None,
    ):
        """
        Initialize retry policy with defaults from global retry settings.

        Args:
            max_attempts: Maximum retry attempts (default: conf.retry_defaults.max_attempts)
            base_delay: Base delay in seconds (default: conf.retry_defaults.delay)
            backoff_strategy: Backoff strategy (default: conf.retry_defaults.backoff)
            jitter: Add random jitter (default: conf.retry_defaults.jitter)
            jitter_max: Max jitter fraction (default: conf.retry_defaults.jitter_max)
            retry_exceptions: Exception names to retry on (None = retry all)
            skip_exceptions: Exception names to never retry
        """
        # Use global retry default settings
        self.max_attempts = (
            max_attempts
            if max_attempts is not None
            else conf.retry_defaults.max_attempts
        )
        self.base_delay = (
            base_delay if base_delay is not None else conf.retry_defaults.delay
        )
        self.backoff_strategy = (
            backoff_strategy
            if backoff_strategy is not None
            else conf.retry_defaults.backoff.value
        )
        self.jitter = jitter if jitter is not None else conf.retry_defaults.jitter
        self.jitter_max = (
            jitter_max if jitter_max is not None else conf.retry_defaults.jitter_max
        )
        self.retry_exceptions = set(retry_exceptions or [])
        self.skip_exceptions = set(skip_exceptions or [])

        # Validation
        if not 1 <= self.max_attempts <= 10:
            raise ValueError("max_attempts must be between 1 and 10")
        if not 0.0 <= self.jitter_max <= 1.0:
            raise ValueError("jitter_max must be between 0.0 and 1.0")
        if self.backoff_strategy not in {strategy.value for strategy in RetryBackoff}:
            raise ValueError(f"Invalid backoff strategy: {self.backoff_strategy}")

    @classmethod
    def from_dict(cls, data: dict[str, Any]):
        """Create retry policy from dictionary (JSONField data)."""
        return cls(**data)

    @classmethod
    def from_task(cls, task):
        """Create retry policy from QraftTask's retry_policy JSONField."""
        return cls.from_dict(task.retry_policy or {})

    @classmethod
    def from_options(cls, qraft_options: dict[str, Any]):
        """
        Create retry policy from qraft_options dict passed to async_task.

        Extracts retry-related keys from the options dict. Returns None if
        no retry options are specified.

        Args:
            qraft_options: The qraft_options dict from async_task

        Returns:
            RetryPolicy instance or None if no retry options specified
        """
        # Keys that are retry-related
        retry_keys = {
            "max_attempts",
            "base_delay",
            "backoff_strategy",
            "jitter",
            "jitter_max",
            "retry_exceptions",
            "skip_exceptions",
        }

        # Extract only retry-related options
        retry_opts = {k: v for k, v in qraft_options.items() if k in retry_keys}

        # If no retry options specified, return None
        if not retry_opts:
            return None

        return cls(**retry_opts)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSONField storage."""
        return {
            "max_attempts": self.max_attempts,
            "base_delay": self.base_delay,
            "backoff_strategy": self.backoff_strategy,
            "jitter": self.jitter,
            "jitter_max": self.jitter_max,
            "retry_exceptions": list(self.retry_exceptions),
            "skip_exceptions": list(self.skip_exceptions),
        }

    def should_retry(
        self, current_attempt: int, exception: Exception | str | None = None
    ) -> bool:
        """
        Determine if a task should be retried.

        Args:
            current_attempt: Current attempt number
            exception: Exception instance or exception class name string

        Returns:
            True if task should be retried
        """
        if current_attempt >= self.max_attempts:
            logger.debug("Max attempts (%d) reached", self.max_attempts)
            return False

        if exception is None:
            return True

        # Handle both Exception instances and string class names
        if isinstance(exception, str):
            exc_name = exception
        else:
            exc_name = exception.__class__.__name__

        # Check skip list first
        if exc_name in self.skip_exceptions:
            logger.debug("Exception %s in skip list", exc_name)
            return False

        # If retry_exceptions are specified, only retry those exceptions
        if self.retry_exceptions:
            should_retry = exc_name in self.retry_exceptions
            logger.debug(
                "Exception %s %s in retry list",
                exc_name,
                "is" if should_retry else "not",
            )
            return should_retry

        # Default: retry all exceptions not in skip list
        return True

    def calculate_delay(self, attempt_number: int) -> float:
        """
        Calculate delay for the next retry attempt.

        Args:
            attempt_number: Current attempt number (1-based)

        Returns:
            Delay in seconds
        """
        match self.backoff_strategy:
            case "linear":
                delay = self.base_delay * attempt_number
            case "exponential":
                delay = self.base_delay * (2 ** (attempt_number - 1))
            case _:
                delay = self.base_delay

        # Add jitter if enabled
        if self.jitter and delay > 0:
            jitter_amount = delay * self.jitter_max
            jitter = random.uniform(-jitter_amount, jitter_amount)
            delay = max(1, delay + jitter)  # Minimum 1 second delay

        return int(delay)

    def next_eta(self, attempt_number: int) -> datetime:
        """
        Calculate ETA for the next retry attempt.

        Args:
            attempt_number: Current attempt number (1-based)

        Returns:
            UTC datetime when task should be retried
        """
        delay_seconds = self.calculate_delay(attempt_number)
        return datetime.now(timezone.utc) + timedelta(seconds=delay_seconds)

    def schedule_retry(self, qraft_task) -> int:
        """
        Schedule a retry for a failed task using Django-Q2's Schedule model.

        Schedules the original task function directly (no wrapper). Uses
        q_options to pass task_name for Qraft linkage - the scheduler extracts
        q_options from kwargs and passes it to async_task.

        Args:
            qraft_task: QraftTask model instance

        Returns:
            Schedule ID
        """
        from django_q.models import Schedule

        from .models import TaskStatus

        current_attempt = qraft_task.attempt_count
        next_attempt = current_attempt + 1
        eta = self.next_eta(current_attempt)

        # Build kwargs with q_options containing task_name for Qraft linkage
        # The scheduler extracts q_options and passes it to async_task
        retry_kwargs = {
            **qraft_task.task_kwargs,
            "q_options": {"task_name": f"qraft:{qraft_task.id}:{next_attempt}"},
        }

        with transaction.atomic():
            schedule = Schedule.objects.create(
                name=f"qraft_retry:{qraft_task.id}:{next_attempt}",
                func=qraft_task.func,
                args=repr(tuple(qraft_task.task_args)),
                kwargs=repr(retry_kwargs),
                hook="qraft.hooks.qraft_hook_handler",
                schedule_type=Schedule.ONCE,
                next_run=eta,
            )

            qraft_task.status = TaskStatus.PENDING
            qraft_task.save(update_fields=["status", "date_updated"])

        logger.info(
            "Scheduled retry %d/%d for QraftTask %s at %s (schedule_id=%s, delay=%ds)",
            next_attempt,
            self.max_attempts,
            qraft_task.id,
            eta,
            schedule.id,
            self.calculate_delay(current_attempt),
        )

        return schedule.id
