"""
Real task and hook functions for the end-to-end tests.

These live in their own importable module because the end-to-end path runs
them the way production does: a dotted path resolved inside a worker, and a
retry re-imported from `QraftTask.func` on the next attempt. A function
defined inside a test body would not survive either step.

CALLS records what actually executed. The end-to-end tests run everything in
one process, so a module-level record is enough; `reset()` clears it between
tests.
"""

import asyncio
import time

CALLS: dict[str, list] = {}

# func name -> how many more of its calls must fail before it succeeds.
FAIL_BUDGET: dict[str, int] = {}


def reset() -> None:
    """Clear recorded calls and failure budgets between tests."""
    CALLS.clear()
    FAIL_BUDGET.clear()


def _record(key: str, value) -> None:
    CALLS.setdefault(key, []).append(value)


class TransientError(RuntimeError):
    """Raised by `flaky` while it still has failure budget left."""


def succeed(label: str) -> str:
    """Always succeeds; records the label it ran with."""
    _record("succeed", label)
    return f"ok:{label}"


def flaky(label: str) -> str:
    """Fails while FAIL_BUDGET['flaky'] is positive, then succeeds."""
    _record("flaky", label)
    if FAIL_BUDGET.get("flaky", 0) > 0:
        FAIL_BUDGET["flaky"] -= 1
        raise TransientError(f"transient failure for {label}")
    return f"ok:{label}"


def always_fails(label: str) -> str:
    """Never succeeds."""
    _record("always_fails", label)
    raise TransientError(f"permanent failure for {label}")


async def succeed_async(label: str) -> str:
    """Coroutine task; the worker must actually await it to record the call."""
    await asyncio.sleep(0)
    _record("succeed_async", label)
    return f"ok:{label}"


async def flaky_async(label: str) -> str:
    """Coroutine twin of `flaky`, sharing its failure budget mechanics."""
    await asyncio.sleep(0)
    _record("flaky_async", label)
    if FAIL_BUDGET.get("flaky_async", 0) > 0:
        FAIL_BUDGET["flaky_async"] -= 1
        raise TransientError(f"transient failure for {label}")
    return f"ok:{label}"


def on_success(label: str) -> None:
    _record("on_success", label)


async def on_success_async(label: str) -> None:
    """Coroutine hook; dispatch must reroute it through run_task to await it."""
    await asyncio.sleep(0)
    _record("on_success_async", label)


def on_failure(label: str) -> None:
    _record("on_failure", label)


def on_success_with_context(label: str, context: dict | None = None) -> None:
    """Success hook that opted into `hook_context`; records what it received."""
    _record("on_success_with_context", (label, context))


def stage_two(label: str) -> str:
    """Second stage of the run in the e2e case; bound as its own unit."""
    return f"stage-two:{label}"


def bind_second_stage(label: str, context: dict | None = None) -> None:
    """
    Success hook of stage one that fans stage two out as a batch.

    The batch is the stage's completion unit, which is the shape the run
    primitive exists for: the pipeline decides what to enqueue next from
    application code, not from a static step list.
    """
    from qraft.batch import QraftBatch

    batch = QraftBatch(run=context["run_id"], stage="two")
    batch.append("tests.e2e_tasks.stage_two", label)
    batch.append("tests.e2e_tasks.stage_two", f"{label}-b")
    batch.run()
    _record("bind_second_stage", label)


def run_is_ready(context: dict | None = None) -> None:
    """The run's durable `on_settled` hook."""
    _record("run_is_ready", context)


def slow_scorer(label: str, seconds: float = 0.05) -> str:
    """Reports progress once, then works long enough to look stalled."""
    from qraft.context import report_progress

    report_progress(current=1, total=10, message="scoring")
    time.sleep(seconds)
    _record("slow_scorer", label)
    return f"scored:{label}"
