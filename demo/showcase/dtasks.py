"""
django.tasks (DEP 14) task definitions, run through `qraft.backend.QraftTaskBackend`.

The `@task` decorator replaces the module attribute with a frozen `Task`
dataclass, so these names are not plain callables. `qraft.runner._resolve_target`
unwraps them worker-side.
"""

import os

from django.tasks import task

from showcase.models import Event
from showcase.tasks import TransientError, record


@task
def summarize(run: str, label: str, document: str) -> dict:
    """Plain django.tasks task enqueued through the qraft backend."""
    record(run, Event.TASK, label, pid=os.getpid(), document=document)
    return {
        "label": label,
        "summary": f"{document[:12]}...",
        "words": len(document.split()),
    }


@task
def deferred(run: str, label: str) -> dict:
    """Enqueued with `run_after`, so it must not run before its due time."""
    record(run, Event.TASK, label, pid=os.getpid())
    return {"label": label}


@task(takes_context=True)
def with_context(context, run: str, label: str) -> dict:
    """
    Receives a real `TaskContext`.

    The recorded result id is what proves the context points back at this
    task's own `TaskResult` rather than at a fabricated one.
    """
    record(
        run,
        Event.TASK,
        label,
        pid=os.getpid(),
        result_id=str(context.task_result.id),
        status=str(context.task_result.status),
        attempt=context.attempt,
    )
    return {"label": label, "result_id": str(context.task_result.id)}


@task
def always_fails(run: str, label: str) -> None:
    """Fails so the backend's FAILED status mapping can be observed."""
    record(run, Event.TASK, label, pid=os.getpid())
    raise TransientError(f"{label} always fails")
