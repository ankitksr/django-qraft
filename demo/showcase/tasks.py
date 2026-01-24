"""
Demo tasks showcasing Qraft's features.

Each task is designed to demonstrate a specific capability:
- slow_task: I/O-bound work (benefits from threading)
- flaky_task: Random failures (tests hooks)
- countdown_task: Fails N times then succeeds (tests retries)
"""

import logging
import random
import time

_log = logging.getLogger("showcase")

# Track retry attempts per task (in-memory, resets on worker restart)
_attempts: dict[str, int] = {}


class TransientError(Exception):
    """A recoverable error that should trigger retries."""

    pass


class PermanentError(Exception):
    """A non-recoverable error that should NOT be retried."""

    pass


# =============================================================================
# Tasks
# =============================================================================


def slow_task(duration: float = 1.0) -> dict:
    """
    Simulates I/O-bound work (API calls, DB queries, file I/O).

    This is where Qraft's threading shines - multiple slow_tasks can
    run concurrently within a single worker process.
    """
    time.sleep(duration)
    return {"slept": duration}


def flaky_task(fail_rate: float = 0.5) -> dict:
    """
    Randomly fails based on fail_rate (0.0 to 1.0).

    Useful for testing dual-phase hooks - you'll see both success
    and failure hooks fire depending on the outcome.
    """
    if random.random() < fail_rate:
        raise TransientError(f"Random failure (rate={fail_rate})")
    return {"status": "success", "fail_rate": fail_rate}


def countdown_task(task_id: str, fail_times: int = 2) -> dict:
    """
    Fails exactly `fail_times` before succeeding.

    Perfect for testing retry policies - set max_attempts > fail_times
    to see the task eventually succeed after retries.

    Args:
        task_id: Unique identifier to track attempts across retries
        fail_times: Number of times to fail before succeeding
    """
    _attempts[task_id] = _attempts.get(task_id, 0) + 1
    attempt = _attempts[task_id]

    if attempt <= fail_times:
        _log.info(
            "countdown_task %s: attempt %d/%d FAILED", task_id, attempt, fail_times
        )
        raise TransientError(f"Attempt {attempt}/{fail_times}")

    _log.info("countdown_task %s: attempt %d SUCCESS", task_id, attempt)
    del _attempts[task_id]  # Clean up
    return {"task_id": task_id, "attempts": attempt}


def permanent_fail_task() -> None:
    """
    Always raises PermanentError.

    Use with skip_exceptions=["PermanentError"] to test skipping retries.
    """
    raise PermanentError("This error should not be retried")


# =============================================================================
# Hooks
# =============================================================================


def on_success(task_id: str) -> None:
    """Success hook - called when a task completes successfully."""
    _log.info("SUCCESS HOOK: task_id=%s", task_id)


def on_failure(task_id: str) -> None:
    """Failure hook - called when a task fails (after all retries exhausted)."""
    _log.info("FAILURE HOOK: task_id=%s", task_id)
