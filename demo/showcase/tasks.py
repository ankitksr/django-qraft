"""
Demo tasks showcasing Qraft's features.

Each task is designed to demonstrate a specific capability:
- noop_task: Simple no-op (tests workflows)
- slow_task: I/O-bound work (benefits from threading)
- flaky_task: Random failures (tests hooks)
- countdown_task: Fails N times then succeeds (tests retries)
- throttled_task: DB token bucket + rate-limit-aware retries
- llm_task: Token/cost accounting and progress reporting
- charge_task: Idempotency-key deduplication
- review_task / publish_task: Approval-gated chain steps
- summarize_task: django.tasks (@task) API demo target
- progress_task: Reports incremental progress over several steps
"""

import logging
import random
import time

from django.tasks import task

from qraft.context import record_usage, report_progress
from qraft.throttle import throttled

_log = logging.getLogger("showcase")

# Bucket key shared by the rate-limit demo. Tiny rate + capacity 1 means the
# first task through takes the only token and everything after it is denied.
RATE_BUCKET_KEY = "mock-llm-provider"
RATE_BUCKET_RATE = 0.01
RATE_BUCKET_CAPACITY = 1.0


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

    qraft_task = QraftTask.objects.filter(
        func="showcase.tasks.countdown_task", task_args__0=task_id
    ).latest("date_created")
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


@throttled(key=RATE_BUCKET_KEY, rate=RATE_BUCKET_RATE, capacity=RATE_BUCKET_CAPACITY)
def throttled_task(label: str) -> dict:
    """
    Gated behind a shared DB token bucket.

    Raises `qraft.throttle.RateLimited` when the bucket is empty. RateLimited
    is a known rate-limit exception, so Qraft reschedules it with rate-limit
    backoff instead of counting it as an ordinary hard failure.
    """
    _log.info("throttled_task %s: token acquired, calling mock-llm", label)
    return {"label": label, "provider": "mock-llm"}


def llm_task(prompt: str, calls: int = 2) -> dict:
    """
    Simulates an agent loop against a mock LLM, recording usage per call.

    `record_usage()` accumulates numeric fields across calls within the same
    attempt, so the totals below are the sum of all `calls` iterations.
    """
    total_in = total_out = 0
    for step in range(calls):
        report_progress(
            current=step + 1, total=calls, message=f"calling mock-llm ({prompt})"
        )
        input_tokens = 120 + 10 * step
        output_tokens = 40 + 5 * step
        record_usage(
            model="mock-llm",
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            cost=round((input_tokens * 3e-6) + (output_tokens * 1.5e-5), 6),
        )
        total_in += input_tokens
        total_out += output_tokens

    _log.info(
        "llm_task %s: %d calls, %d in / %d out tokens",
        prompt,
        calls,
        total_in,
        total_out,
    )
    return {"prompt": prompt, "calls": calls}


def charge_task(customer: str, amount: float) -> dict:
    """
    Stands in for a non-repeatable side effect (charging Mockco's card).

    Enqueue it with an `idempotency_key` so a duplicate request never
    produces a second charge.
    """
    _log.info("charge_task: charging %s %.2f", customer, amount)
    return {"customer": customer, "amount": amount}


@task
def summarize_task(document: str) -> dict:
    """
    Enqueued via the official django.tasks API (`summarize_task.enqueue(...)`),
    engined by `qraft.backend.QraftTaskBackend` on top of the normal Qraft
    pipeline (QraftTask/QraftTaskAttempt rows, hook handler, etc).
    """
    _log.info("summarize_task: summarizing %s", document)
    return {"document": document, "summary": f"{document} (mock-llm summary)"}


def progress_task(steps: int = 5, delay: float = 0.5) -> dict:
    """
    Slow task that reports incremental progress via report_progress().

    A caller can poll `QraftTask.progress` while this runs to watch the
    current/total counters change in near real time.
    """
    for step in range(1, steps + 1):
        report_progress(current=step, total=steps, message=f"step {step}/{steps}")
        time.sleep(delay)
    return {"steps": steps}


def review_task(document: str) -> dict:
    """Draft step of the approval chain: produces something a human signs off."""
    _log.info("review_task: drafted %s", document)
    return {"document": document, "state": "drafted"}


def publish_task(document: str) -> dict:
    """Approval-gated step: only runs after chain.approve()."""
    _log.info("publish_task: published %s", document)
    return {"document": document, "state": "published"}


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
        workflow_type,
        workflow_id,
        completed_count,
        total_count,
        success_count,
        failure_count,
    )
