"""
Hook functions for the demo scenarios.

Hooks also run outside the scenario's process, so they record an `Event` too.
The payload carries the PID: an async hook lands on a worker, a synchronous
one lands in the monitor.
"""

import logging
import os

from showcase.models import Event
from showcase.tasks import record

_logger = logging.getLogger("showcase")


def on_success(run: str, label: str) -> None:
    record(run, Event.HOOK, f"success:{label}", pid=os.getpid())
    _logger.info("success hook %s/%s", run, label)


def on_failure(run: str, label: str) -> None:
    record(run, Event.HOOK, f"failure:{label}", pid=os.getpid())
    _logger.info("failure hook %s/%s", run, label)


async def on_success_async(run: str, label: str) -> None:
    """
    Coroutine hook; the Event after the await proves it was awaited.

    Async ORM only: sync ORM raises SynchronousOnlyOperation inside a
    running event loop, for hooks just as for tasks.
    """
    import asyncio

    await asyncio.sleep(0.05)
    await Event.objects.acreate(
        run=run,
        kind=Event.HOOK,
        name=f"success:{label}",
        payload={"pid": os.getpid(), "awaited": True},
    )
    _logger.info("async success hook %s/%s", run, label)


def on_workflow_success(run: str, label: str) -> None:
    record(run, Event.HOOK, f"wf-success:{label}", pid=os.getpid())
    _logger.info("workflow success hook %s/%s", run, label)


def on_workflow_failure(run: str, label: str) -> None:
    record(run, Event.HOOK, f"wf-failure:{label}", pid=os.getpid())
    _logger.info("workflow failure hook %s/%s", run, label)


def on_cancelled() -> None:
    """
    Cancellation hook.

    It takes no arguments because qraft dispatches it with none: see
    `QraftChain.reject`, which passes empty args and kwargs. The hook is
    therefore not told which workflow was cancelled, so a scenario counts
    dispatches rather than matching its own run id.
    """
    Event.objects.create(
        run="", kind=Event.HOOK, name="cancelled", payload={"pid": os.getpid()}
    )
    _logger.info("cancelled hook fired")


def on_progress(
    *,
    workflow_id: str,
    workflow_type: str,
    completed_count: int,
    total_count: int,
    success_count: int,
    failure_count: int,
) -> None:
    """
    Progress hook for parallel workflows.

    qraft calls this with a fixed keyword signature that carries no scenario
    run id, so the workflow id is what a scenario filters on.
    """
    Event.objects.create(
        run=str(workflow_id),
        kind=Event.HOOK,
        name="progress",
        payload={
            "workflow_id": str(workflow_id),
            "workflow_type": workflow_type,
            "completed_count": completed_count,
            "total_count": total_count,
            "success_count": success_count,
            "failure_count": failure_count,
            "pid": os.getpid(),
        },
    )


def on_graph_settled(run, context):
    Event.objects.create(
        run=run, kind=Event.HOOK, name="graph-settled", payload=context
    )
