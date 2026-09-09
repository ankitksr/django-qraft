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
node_settled = Signal()
graph_settled = Signal()
graph_overdue = Signal()


def _iso(value):
    if value is None:
        return None
    return value.isoformat() if hasattr(value, "isoformat") else str(value)


def _text(value):
    return str(value) if value is not None else None


def attempt_payload(attempt, qraft_task=None, outcome: str | None = None) -> dict:
    """Id-only view of an attempt and its task, shared by every attempt signal."""
    task = qraft_task if qraft_task is not None else attempt.qraft_task
    node = getattr(task, "graph_node", None)
    return {
        "task_id": str(task.id),
        "attempt_id": str(attempt.id),
        "attempt_number": attempt.attempt_number,
        "func": task.func,
        "status": task.status,
        "outcome": outcome,
        "exception_class": attempt.exception_class,
        "graph_id": _text(getattr(task, "graph_id", None)),
        "node_key": getattr(task, "node", None),
        "generation": node.generation if node is not None else None,
        "subject_type": task.subject_type,
        "subject_id": task.subject_id,
        "cluster": attempt.cluster,
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
        "graph_id": _text(getattr(workflow, "graph_id", None)),
        "node": getattr(workflow, "node", None),
        "subject_type": workflow.subject_type,
        "subject_id": workflow.subject_id,
        "settled_at": _iso(workflow.settled_at),
    }


def graph_payload(graph) -> dict:
    """Id-only view of a graph, shared by `graph_settled` and `graph_overdue`."""
    return {
        "graph_id": str(graph.id),
        "subject_type": graph.subject_type,
        "subject_id": graph.subject_id,
        "kind": graph.kind,
        "revision": graph.revision,
        "outcome": graph.status,
        "generation": graph.generation,
        "nodes": {
            node.key: node.status
            for node in graph.nodes.all().order_by("depth", "position")
        },
        "previous_graph_id": _text(graph.previous_graph_id),
        "date_started": _iso(graph.date_started),
        "settled_at": _iso(graph.settled_at),
        "overdue_flagged_at": _iso(graph.overdue_flagged_at),
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
