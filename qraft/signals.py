"""
Signals Qraft emits about its own transitions.

Every send is `send_robust()` and every payload is an immutable mapping of ids,
outcomes and ISO timestamps rather than model instances: a receiver that needs
more looks the row up, and sees the committed state because sends are
registered with `transaction.on_commit` from inside the transaction that
performs the transition. Signals are best-effort observers for logs, caches and
in-process reactions; anything the application must not miss goes through a
hook, whose dispatch row survives a crash.

`task_started` fires in the worker process; everything else fires on the thread
that runs the hook handler (the monitor) or the reaper thread beside it, so
receivers must be cheap.
"""

import logging
from types import MappingProxyType

from django.db import transaction
from django.dispatch import Signal

_logger = logging.getLogger("qraft")

task_started = Signal()
attempt_finished = Signal()
task_settled = Signal()
workflow_settled = Signal()
attempt_stall_suspected = Signal()
run_settled = Signal()
run_overdue = Signal()


def _iso(value):
    if value is None:
        return None
    return value.isoformat() if hasattr(value, "isoformat") else str(value)


def _text(value):
    return str(value) if value is not None else None


def attempt_payload(attempt, qraft_task=None, outcome: str | None = None) -> dict:
    """Id-only view of an attempt and its task, shared by every attempt signal."""
    task = qraft_task if qraft_task is not None else attempt.qraft_task
    return {
        "task_id": str(task.id),
        "attempt_id": str(attempt.id),
        "attempt_number": attempt.attempt_number,
        "func": task.func,
        "status": task.status,
        "outcome": outcome,
        "exception_class": attempt.exception_class,
        "run_id": _text(getattr(task, "run_id", None)),
        "stage": getattr(task, "stage", None),
        "subject_type": task.subject_type,
        "subject_id": task.subject_id,
        "cluster": attempt.cluster,
        # How many deliveries of this attempt a worker began, and whether the
        # one being reported is a repeat the guard refused.
        "execution_count": attempt.execution_count,
        "redelivered": attempt.execution_count > 1
        or attempt.exception_class == "RedeliveredAttempt",
        "enqueued_at": _iso(attempt.enqueued_at),
        "date_started": _iso(attempt.date_started),
        "date_completed": _iso(attempt.date_completed),
    }


def workflow_payload(workflow, workflow_type: str, outcome: str) -> dict:
    return {
        "workflow_id": str(workflow.id),
        "workflow_type": workflow_type,
        "outcome": outcome,
        "run_id": _text(getattr(workflow, "run_id", None)),
        "stage": getattr(workflow, "stage", None),
        "subject_type": workflow.subject_type,
        "subject_id": workflow.subject_id,
        "settled_at": _iso(workflow.settled_at),
    }


def run_payload(run) -> dict:
    """Id-only view of a run, shared by `run_settled` and `run_overdue`."""
    return {
        "run_id": str(run.id),
        "subject_type": run.subject_type,
        "subject_id": run.subject_id,
        "kind": run.kind,
        "revision": run.revision,
        "outcome": run.status,
        "stages": {
            stage.name: stage.status for stage in run.stages.all().order_by("position")
        },
        "previous_run_id": _text(run.previous_run_id),
        "date_started": _iso(run.date_started),
        "settled_at": _iso(run.settled_at),
        "overdue_flagged_at": _iso(run.overdue_flagged_at),
    }


def send(signal: Signal, sender, payload: dict) -> None:
    """
    Announce a transition once the transaction performing it commits.

    Outside a transaction `on_commit` runs the callback immediately. A receiver
    that raises is logged with its name and never reaches the caller, which is
    the hook handler in the monitor.
    """
    frozen = MappingProxyType(dict(payload))

    def _send():
        for receiver, response in signal.send_robust(sender=sender, payload=frozen):
            if isinstance(response, Exception):
                _logger.warning(
                    "Signal receiver %s raised %r",
                    getattr(receiver, "__qualname__", repr(receiver)),
                    response,
                )

    transaction.on_commit(_send)
