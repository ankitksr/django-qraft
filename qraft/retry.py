"""
Rich retry policy implementation for Django-Qraft.
Supports multiple backoff strategies, jitter, and exception-specific retry logic.
"""

import logging
import random
import re
from datetime import datetime, timedelta, timezone
from typing import Any

from . import metrics
from .conf import RetryBackoff, executing_cluster, get_conf

logger = logging.getLogger("qraft")

# Minimum delay in seconds when jitter is applied (prevents zero/negative)
MIN_JITTER_DELAY = 1

# Task-name marker. Linkage runs off the attempt's q2_task_id now; the marker
# survives as the name a dispatched attempt is queued under, and as the only
# way to resolve a pre-1.3 Schedule delivery still in flight across an upgrade
# (see qraft.hooks.attempt_from_marker).
QRAFT_MARKER_PREFIX = "qraft"
QRAFT_MARKER_FMT = "{prefix}:{task_id}:{attempt}"

# Exception class names commonly raised by provider SDKs for rate limiting /
# transient overload. These are retryable even under a restrictive
# retry_exceptions allowlist, since backing off on them is almost always
# the right default.
RATE_LIMIT_EXCEPTIONS = frozenset(
    {
        "RateLimited",
        "RateLimitError",
        "TooManyRequests",
        "TooManyRequestsError",
        "ThrottlingException",
        "ResourceExhausted",
        "ServiceUnavailableError",
        "OverloadedError",
        "APIStatusError429",
    }
)

# Patterns for "retry after N seconds" hints in provider error strings, e.g.
# "Retry-After: 12", "retry_after=12", "try again in 12 seconds".
_RETRY_AFTER_PATTERNS = (
    re.compile(r"retry[-_ ]after[:=\s]+(\d+(?:\.\d+)?)", re.IGNORECASE),
    re.compile(r"try again in (\d+(?:\.\d+)?)\s*s(?:econds?)?", re.IGNORECASE),
)


def parse_retry_after(text: str) -> float | None:
    """
    Extract a "retry after N seconds" hint from a provider error string.

    Returns the raw parsed value in seconds, uncapped - callers are
    responsible for applying any maximum delay.

    Args:
        text: Error message or traceback text to search.

    Returns:
        Parsed delay in seconds, or None if no pattern matched.
    """
    if not text or not isinstance(text, str):
        return None

    for pattern in _RETRY_AFTER_PATTERNS:
        match = pattern.search(text)
        if match:
            # Floor to 1s: a provider hint of "0" or a sub-second value
            # must never produce a zero/negative retry delay.
            return max(1.0, float(match.group(1)))

    return None


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
        rate_limit_exceptions: list[str] | None = None,
        rate_limit_max_delay: float = 300.0,
    ):
        """
        Initialize retry policy with defaults from global retry settings.

        Args:
            max_attempts: Total executions, including the first - not retry
                count. max_attempts=4 means 1 initial attempt + 3 retries.
                (default: conf.retry_defaults.max_attempts)
            base_delay: Base delay in seconds (default: conf.retry_defaults.delay)
            backoff_strategy: Backoff strategy (default: conf.retry_defaults.backoff)
            jitter: Add random jitter (default: conf.retry_defaults.jitter)
            jitter_max: Max jitter fraction (default: conf.retry_defaults.jitter_max)
            retry_exceptions: Exception names to retry on (None = retry all)
            skip_exceptions: Exception names to never retry
            rate_limit_exceptions: Exception names treated as rate-limit errors
                (None = use RATE_LIMIT_EXCEPTIONS default set)
            rate_limit_max_delay: Cap in seconds for rate-limit-driven delays
        """
        # Use global retry default settings
        defaults = get_conf().retry_defaults
        self.max_attempts = (
            max_attempts if max_attempts is not None else defaults.max_attempts
        )
        self.base_delay = base_delay if base_delay is not None else defaults.delay
        self.backoff_strategy = (
            backoff_strategy if backoff_strategy is not None else defaults.backoff.value
        )
        self.jitter = jitter if jitter is not None else defaults.jitter
        self.jitter_max = jitter_max if jitter_max is not None else defaults.jitter_max
        self.retry_exceptions = set(retry_exceptions or [])
        self.skip_exceptions = set(skip_exceptions or [])
        self.rate_limit_exceptions = (
            set(rate_limit_exceptions)
            if rate_limit_exceptions is not None
            else set(RATE_LIMIT_EXCEPTIONS)
        )
        self.rate_limit_max_delay = rate_limit_max_delay

        # Validation
        if not 1 <= self.max_attempts <= 10:
            raise ValueError("max_attempts must be between 1 and 10")
        if not 0.0 <= self.jitter_max <= 1.0:
            raise ValueError("jitter_max must be between 0.0 and 1.0")
        if self.backoff_strategy not in {strategy.value for strategy in RetryBackoff}:
            raise ValueError(f"Invalid backoff strategy: {self.backoff_strategy}")
        if self.base_delay < 0:
            raise ValueError("base_delay must be >= 0")
        if self.rate_limit_max_delay <= 0:
            raise ValueError("rate_limit_max_delay must be > 0")

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
            "rate_limit_exceptions",
            "rate_limit_max_delay",
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
            "rate_limit_exceptions": list(self.rate_limit_exceptions),
            "rate_limit_max_delay": self.rate_limit_max_delay,
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

        # Rate-limit errors are retryable even under a restrictive allowlist
        if exc_name in self.rate_limit_exceptions:
            logger.debug("Exception %s is a rate-limit error, retrying", exc_name)
            return True

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

    def is_rate_limit(self, exception: Exception | str | None) -> bool:
        """Whether an exception (instance or class name) is a rate-limit error."""
        if exception is None:
            return False
        exc_name = (
            exception if isinstance(exception, str) else exception.__class__.__name__
        )
        return exc_name in self.rate_limit_exceptions

    def calculate_delay(
        self,
        attempt_number: int,
        retry_after: float | None = None,
        is_rate_limit: bool = False,
    ) -> float:
        """
        Calculate delay for the next retry attempt.

        Args:
            attempt_number: Current attempt number (1-based)
            retry_after: Provider-supplied delay hint (e.g. from a
                Retry-After header). Takes precedence over everything else.
            is_rate_limit: Whether this retry follows a rate-limit error.
                Ignored if retry_after is given.

        Returns:
            Delay in seconds
        """
        if retry_after is not None:
            # Honor the provider's hint as a floor - only add a small positive
            # jitter, never reduce below what it asked for. The cap is
            # applied after jitter, so a hint near the cap can't slip past it
            # (capping before jitter let a 300s cap yield up to 330s).
            delay = retry_after + random.uniform(0, retry_after * 0.1)
            return int(min(delay, self.rate_limit_max_delay))

        if is_rate_limit:
            # Rate limits back off exponentially regardless of the configured
            # strategy - a fixed/linear policy shouldn't hammer a 429.
            delay = min(
                self.base_delay * (2 ** (attempt_number - 1)),
                self.rate_limit_max_delay,
            )
        else:
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
            delay = max(MIN_JITTER_DELAY, delay + jitter)

        if is_rate_limit:
            delay = min(delay, self.rate_limit_max_delay)

        return int(delay)

    def next_eta(
        self,
        attempt_number: int,
        retry_after: float | None = None,
        is_rate_limit: bool = False,
    ) -> datetime:
        """
        Calculate ETA for the next retry attempt.

        Args:
            attempt_number: Current attempt number (1-based)
            retry_after: Provider-supplied delay hint, see calculate_delay
            is_rate_limit: Whether this retry follows a rate-limit error

        Returns:
            UTC datetime when task should be retried
        """
        delay_seconds = self.calculate_delay(attempt_number, retry_after, is_rate_limit)
        return datetime.now(timezone.utc) + timedelta(seconds=delay_seconds)

    def schedule_retry(
        self,
        qraft_task,
        current_attempt: int,
        retry_after: float | None = None,
        is_rate_limit: bool = False,
    ) -> str:
        """
        Schedule a retry for a failed task as a SCHEDULED attempt row.

        The row carries the exact ETA, so the delay this policy computed is
        the delay served - Django-Q2's scheduler is not involved and its
        hardcoded 30-second cycle no longer rounds sub-30-second backoff up.

        Args:
            qraft_task: QraftTask model instance
            current_attempt: The attempt number that just failed (concrete value,
                avoids race condition with COUNT queries)
            retry_after: Provider-supplied delay hint, see calculate_delay
            is_rate_limit: Whether this retry follows a rate-limit error

        Returns:
            str: id of the scheduled QraftTaskAttempt
        """
        from .scheduler import _inherited_cluster, schedule_attempt

        next_attempt = current_attempt + 1
        delay_seconds = self.calculate_delay(
            current_attempt, retry_after, is_rate_limit
        )
        eta = datetime.now(timezone.utc) + timedelta(seconds=delay_seconds)

        # The failed attempt's own cluster, not this process's: the monitor
        # that runs this happens to be the right cluster today, but the
        # attempt records where the work actually belongs.
        attempt = schedule_attempt(
            qraft_task,
            next_attempt,
            eta,
            cluster=_inherited_cluster(qraft_task) or executing_cluster(),
        )

        logger.info(
            "Scheduled retry %d/%d for QraftTask %s at %s (attempt_id=%s, delay=%ds)",
            next_attempt,
            self.max_attempts,
            qraft_task.id,
            eta,
            attempt.id,
            delay_seconds,
        )

        return str(attempt.id)


def handle_task_retry(qraft_task, attempt, result_text: str | None = None) -> bool:
    """
    Handle retry logic for a failed task.

    Checks the task's retry policy and either schedules a retry or marks
    the task as exhausted.

    Args:
        qraft_task: QraftTask that failed
        attempt: QraftTaskAttempt with exception info
        result_text: Raw task result/traceback text, used to look for a
            provider "retry after" hint when the failure is a rate limit

    Returns:
        True if retry was scheduled, False if exhausted or no policy
    """
    from .models import TaskStatus

    policy = qraft_task.retry_policy
    if not policy:
        return False

    retry_policy = RetryPolicy.from_dict(policy)

    # Use exception class from the attempt (already extracted and stored)
    exc_class_name = attempt.exception_class
    current_attempt = attempt.attempt_number

    if retry_policy.should_retry(current_attempt, exc_class_name):
        is_rate_limit = retry_policy.is_rate_limit(exc_class_name)
        retry_after = parse_retry_after(result_text) if is_rate_limit else None
        # Schedule retry with backoff delay
        retry_policy.schedule_retry(
            qraft_task, current_attempt, retry_after, is_rate_limit
        )
        metrics.counter_on_commit(
            "qraft.retry.scheduled",
            func=qraft_task.func,
            exception_class=exc_class_name,
        )
        return True

    # All retries exhausted
    qraft_task.status = TaskStatus.EXHAUSTED
    qraft_task.save(update_fields=["status", "date_updated"])

    logger.info(
        "QraftTask %s exhausted all %d retry attempts",
        qraft_task.id,
        current_attempt,
    )

    return False
