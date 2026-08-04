"""
Demo tasks showcasing Qraft's features.

Each task is designed to demonstrate a specific capability:
- noop_task: Simple no-op (tests workflows)
- slow_task: I/O-bound work (benefits from threading)
- flaky_task: Random failures (tests hooks)
- countdown_task: Fails N times then succeeds (tests retries)
"""

import logging
import random
import time

_log = logging.getLogger("showcase")

class TransientError(Exception):
    """A recoverable error that should trigger retries."""

    pass


class PermanentError(Exception):
    """A non-recoverable error that should NOT be retried."""

    pass


# =============================================================================
# Tasks
# =============================================================================


def noop_task(value=None) -> dict:
    """
    A simple no-op task that immediately returns.

    Used for testing workflow primitives without I/O overhead.
    """
    return {"value": value}


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

    Attempt tracking uses QraftTaskAttempt records in the database so it
    works correctly across multiple worker processes. When attempt N runs,
    attempts 1..N-1 already exist (created by the hook handler after each
    prior attempt completed), so ``count() + 1`` gives the current number.

    Args:
        task_id: Unique identifier to track attempts across retries
        fail_times: Number of times to fail before succeeding
    """
    from qraft.models import QraftTask

    qraft_task = (
        QraftTask.objects
        .filter(func="showcase.tasks.countdown_task", task_args__0=task_id)
        .latest("date_created")
    )
    attempt = qraft_task.attempts.count() + 1

    if attempt <= fail_times:
        _log.info(
            "countdown_task %s: attempt %d/%d FAILED", task_id, attempt, fail_times
        )
        raise TransientError(f"Attempt {attempt}/{fail_times}")

    _log.info("countdown_task %s: attempt %d SUCCESS", task_id, attempt)
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


def on_cancelled(workflow_id: str) -> None:
    """Cancellation hook - called when a workflow is cancelled."""
    _log.info("CANCELLED HOOK: workflow_id=%s", workflow_id)


def on_progress(
    *,
    workflow_id: str,
    workflow_type: str,
    completed_count: int,
    total_count: int,
    success_count: int,
    failure_count: int,
) -> None:
    """Progress hook - called after each task in a parallel workflow completes."""
    _log.info(
        "PROGRESS: %s %s — %d/%d done (S:%d F:%d)",
        workflow_type, workflow_id,
        completed_count, total_count, success_count, failure_count,
    )
