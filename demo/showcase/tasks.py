"""
Task functions for the demo scenarios.

Everything here runs inside a worker process. The only way a scenario can see
what happened is an `Event` row, so every task records one before it does
anything else.
"""

import asyncio
import logging
import os
import random
import time

from django.db.models import F

from qraft.context import record_usage, report_progress
from qraft.throttle import throttled
from showcase.models import Control, Event

_logger = logging.getLogger("showcase")

# Shared token bucket for the throttle scenario. Fixed at import time so every
# worker process in every cluster agrees on the same limit.
THROTTLE_KEY = "mock-provider"
THROTTLE_RATE = 2.0
THROTTLE_CAPACITY = 2.0


class TransientError(Exception):
    """Recoverable failure. Retried by default."""


class PermanentError(Exception):
    """Unrecoverable failure. Used to prove skip_exceptions/retry_exceptions."""


class RateLimitError(Exception):
    """
    Provider rate-limit failure.

    The class name is in `qraft.retry.RATE_LIMIT_EXCEPTIONS`, so qraft treats
    it as retryable even under a restrictive allow-list and looks for a
    Retry-After hint in the message.
    """


def record(run: str, kind: str, name: str, **payload) -> Event:
    """Write one observable fact to the database for a scenario to read back."""
    return Event.objects.create(run=run, kind=kind, name=name, payload=payload)


def bump(key: str) -> int:
    """Increment a cross-process counter and return its new value."""
    Control.objects.get_or_create(key=key)
    Control.objects.filter(key=key).update(counter=F("counter") + 1)
    return Control.objects.values_list("counter", flat=True).get(key=key)


def flag(key: str, default=None):
    """Read a switch a scenario set from the other process."""
    row = Control.objects.filter(key=key).first()
    return row.value if row else default


# --- basic tasks ----------------------------------------------------------


def ping(run: str = "boot") -> dict:
    """Readiness probe: proves a cluster is draining its lane."""
    return {"pid": os.getpid()}


def ok_task(run: str, label: str, value=None) -> dict:
    """Succeed, and say so."""
    record(run, Event.TASK, label, pid=os.getpid(), value=value)
    return {"label": label, "value": value}


def sleep_task(run: str, label: str, seconds: float = 0.5) -> dict:
    """I/O-bound stand-in: holds a worker slot without burning CPU."""
    time.sleep(seconds)
    record(run, Event.TASK, label, pid=os.getpid(), seconds=seconds)
    return {"label": label, "slept": seconds}


def fail_task(run: str, label: str, error: str = "TransientError") -> None:
    """Always fail, with a chosen exception class."""
    attempt = bump(f"{run}:{label}")
    record(run, Event.TASK, label, pid=os.getpid(), attempt=attempt, outcome="raise")
    raise _ERRORS[error](f"{label} failed on attempt {attempt}")


def flaky_task(run: str, label: str, fail_times: int = 2) -> dict:
    """Fail the first `fail_times` attempts, then succeed."""
    attempt = bump(f"{run}:{label}")
    record(run, Event.TASK, label, pid=os.getpid(), attempt=attempt)
    if attempt <= fail_times:
        raise TransientError(f"{label} failed on attempt {attempt}")
    return {"label": label, "attempt": attempt}


async def async_flaky_task(run: str, label: str, fail_times: int = 1) -> dict:
    """
    Coroutine twin of `flaky_task`.

    The Event is only recorded after a real `await`, so its existence proves
    the worker awaited the coroutine rather than dropping it unawaited.

    Uses the async ORM throughout: inside a running event loop Django's sync
    ORM raises SynchronousOnlyOperation, and an async task has one - that
    rule applies to user coroutines exactly as it does here.
    """
    await asyncio.sleep(0.05)
    key = f"{run}:{label}"
    await Control.objects.aget_or_create(key=key)
    await Control.objects.filter(key=key).aupdate(counter=F("counter") + 1)
    attempt = (await Control.objects.aget(key=key)).counter
    await Event.objects.acreate(
        run=run,
        kind=Event.TASK,
        name=label,
        payload={"pid": os.getpid(), "attempt": attempt, "awaited": True},
    )
    if attempt <= fail_times:
        raise TransientError(f"{label} failed on attempt {attempt}")
    return {"label": label, "attempt": attempt}


def rate_limited_task(run: str, label: str, retry_after: int = 5, fail_times: int = 1):
    """
    Fail with a provider-style rate-limit error carrying a Retry-After hint.

    qraft parses the hint out of the traceback text and uses it as the retry
    delay instead of the configured backoff.
    """
    attempt = bump(f"{run}:{label}")
    record(run, Event.TASK, label, pid=os.getpid(), attempt=attempt)
    if attempt <= fail_times:
        raise RateLimitError(
            f"{label} throttled by provider, Retry-After: {retry_after}"
        )
    return {"label": label, "attempt": attempt}


def crash_task(run: str, label: str, hold: float = 120.0) -> dict:
    """
    Publish this worker's PID, then hold the slot.

    The durability scenarios read the PID back and send a real SIGKILL, so the
    orphan reaper has a genuinely dead worker to reclaim.
    """
    attempt = bump(f"{run}:{label}")
    record(run, Event.TASK, label, pid=os.getpid(), attempt=attempt)
    if flag(f"{run}:{label}:crash", {}).get("stop_crashing"):
        return {"label": label, "attempt": attempt, "survived": True}
    time.sleep(hold)
    return {"label": label, "attempt": attempt, "survived": True}


def crash_on_attempt(
    run: str, label: str, crash_attempt: int = 2, hold: float = 90.0
) -> dict:
    """
    Fail normally, then hang on `crash_attempt` so that attempt can be killed.

    The attempt that hangs is reached through a dispatched retry, so its
    QraftTaskAttempt row was created SCHEDULED by qraft's scheduler rather
    than by async_task(). That is the path the reaper has to be able to see.
    """
    attempt = bump(f"{run}:{label}")
    record(run, Event.TASK, label, pid=os.getpid(), attempt=attempt)
    if attempt < crash_attempt:
        raise TransientError(f"{label} failed on attempt {attempt}")
    if attempt == crash_attempt and not flag(f"{run}:{label}:crash", {}).get(
        "stop_crashing"
    ):
        time.sleep(hold)
    return {"label": label, "attempt": attempt}


@throttled(key=THROTTLE_KEY, rate=THROTTLE_RATE, capacity=THROTTLE_CAPACITY)
def throttled_task(run: str, label: str) -> dict:
    """Gated behind a database token bucket shared by every cluster."""
    record(
        run,
        Event.TASK,
        label,
        pid=os.getpid(),
        cluster=os.environ.get("Q_CLUSTER_NAME", "default"),
    )
    return {"label": label}


# --- AI-workload tasks ----------------------------------------------------


def llm_task(run: str, label: str, calls: int = 2) -> dict:
    """Mock LLM call loop that reports token usage and cost per call."""
    record(run, Event.TASK, label, pid=os.getpid())
    for index in range(calls):
        report_progress(current=index + 1, total=calls, message=f"call {index + 1}")
        record_usage(
            model="mock-llm-v1",
            input_tokens=100,
            output_tokens=25,
            cost_usd=0.002,
        )
    return {"label": label, "calls": calls}


def fake_api_task(
    run: str, label: str, seconds: float = 300.0, fail_pct: float = 0.0
) -> dict:
    """
    Mock external API call for the soak panel: a long hold with visible vitals.

    Sleeps `seconds` in slices, reports progress each slice (the lease thread
    keeps the heartbeat fresh on its own), and — with probability `fail_pct` —
    raises a TransientError partway through, so the watcher can see the retry
    path fire under load. Records a little mock usage on success.
    """
    attempt = bump(f"{run}:{label}")
    record(run, Event.TASK, label, pid=os.getpid(), attempt=attempt, seconds=seconds)
    steps = max(4, int(seconds // 15))
    fail_at = None
    if fail_pct and random.random() * 100 < fail_pct:
        fail_at = random.randint(1, max(1, steps // 2))
    for step in range(1, steps + 1):
        report_progress(current=step, total=steps, message=f"api call {step}/{steps}")
        if step == fail_at:
            raise TransientError(f"{label} upstream 502 on attempt {attempt}")
        time.sleep(seconds / steps)
    record_usage(model="mock-api", cost_usd=0.01)
    return {"label": label, "attempt": attempt, "seconds": seconds}


def progress_task(run: str, label: str, steps: int = 4, delay: float = 0.3) -> dict:
    """Report progress step by step so a watcher can see it move."""
    record(run, Event.TASK, label, pid=os.getpid())
    for step in range(1, steps + 1):
        report_progress(current=step, total=steps, message=f"step {step}/{steps}")
        time.sleep(delay)
    return {"label": label, "steps": steps}


# --- workflow steps -------------------------------------------------------


def step_task(run: str, label: str, step: int) -> dict:
    """A chain step. The Event ordering is what proves sequential execution."""
    record(run, Event.STEP, label, pid=os.getpid(), step=step)
    return {"label": label, "step": step}


def gated_step_task(run: str, label: str, step: int) -> dict:
    """
    A chain step that fails until a scenario clears its gate.

    Used by the resume scenario: fail, fix the cause, resume from the failed
    step rather than from the top of the chain.
    """
    attempt = bump(f"{run}:{label}")
    record(run, Event.STEP, label, pid=os.getpid(), step=step, attempt=attempt)
    if not flag(f"{run}:gate", {}).get("open"):
        raise TransientError(f"{label} blocked: gate closed")
    return {"label": label, "step": step, "attempt": attempt}


_ERRORS = {
    "TransientError": TransientError,
    "PermanentError": PermanentError,
    "RateLimitError": RateLimitError,
    "ValueError": ValueError,
}
