"""
Task-execution context: current-attempt lookup, usage accounting, and
progress reporting.

`_current_q2_task_id` is set by a `pre_execute` receiver so that task code
running deep in a call stack (e.g. inside an LLM client wrapper) can find
its own QraftTaskAttempt without threading it through every call.
"""

import logging
from contextvars import ContextVar

_logger = logging.getLogger("qraft")

_current_q2_task_id: ContextVar[str | None] = ContextVar(
    "_current_q2_task_id", default=None
)


def _is_numeric(value) -> bool:
    """Whether a usage value accumulates (bools are flags, not counters)."""
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _on_pre_execute(sender, func, task, **kwargs):
    """django_q `pre_execute` receiver: record the executing task's q2 id."""
    _current_q2_task_id.set(task.get("id"))


def current_attempt():
    """
    Return the QraftTaskAttempt for the task currently executing, or None.

    None both outside a task and when the running task has no Qraft record
    (e.g. a plain Django-Q2 task not created via qraft.async_task).
    """
    from qraft.models.tasks import QraftTaskAttempt

    q2_task_id = _current_q2_task_id.get()
    if q2_task_id is None:
        return None

    try:
        return QraftTaskAttempt.objects.select_related("qraft_task").get(
            q2_task_id=q2_task_id
        )
    except QraftTaskAttempt.DoesNotExist:
        return None


def record_usage(**fields) -> None:
    """
    Merge usage fields into the current attempt's `usage` JSON.

    Numeric fields (int/float, excluding bool) accumulate across repeated
    calls within the same attempt (e.g. several LLM calls in one task);
    non-numeric fields (model name, etc.) are overwritten by the latest call.
    No-op outside a task.
    """
    attempt = current_attempt()
    if attempt is None:
        _logger.debug("record_usage() called outside a task, ignoring")
        return

    usage = dict(attempt.usage or {})
    for key, value in fields.items():
        existing = usage.get(key)
        if _is_numeric(value) and _is_numeric(existing):
            usage[key] = existing + value
        else:
            usage[key] = value

    attempt.usage = usage
    attempt.save(update_fields=["usage"])


def report_progress(
    current: int | None = None,
    total: int | None = None,
    message: str | None = None,
    **extra,
) -> None:
    """
    Write a merged progress payload to the current QraftTask. No-op outside
    a task.
    """
    attempt = current_attempt()
    if attempt is None:
        _logger.debug("report_progress() called outside a task, ignoring")
        return

    qraft_task = attempt.qraft_task
    progress = dict(qraft_task.progress or {})
    for key, value in {
        "current": current,
        "total": total,
        "message": message,
        **extra,
    }.items():
        if value is not None:
            progress[key] = value

    qraft_task.progress = progress
    qraft_task.save(update_fields=["progress"])


def _sum_usage(usages) -> dict:
    """Sum numeric keys across a sequence of usage dicts, latest non-numeric wins."""
    aggregated: dict = {}
    for usage in usages:
        if not usage:
            continue
        for key, value in usage.items():
            if _is_numeric(value):
                aggregated[key] = aggregated.get(key, 0) + value
            else:
                aggregated[key] = value
    return aggregated


def aggregate_usage(qraft_task) -> dict:
    """Sum usage across all of a QraftTask's attempts."""
    return _sum_usage(qraft_task.attempts.values_list("usage", flat=True))


def aggregate_workflow_usage(workflow) -> dict:
    """
    Sum usage across a workflow's tasks.

    `workflow` is a QraftIterModel/QraftBatchModel (usage summed across
    `workflow.tasks`) or a QraftChainModel (usage summed across the chain's
    steps' linked qraft_tasks).
    """
    if hasattr(workflow, "tasks"):
        qraft_tasks = workflow.tasks.all()
    else:
        qraft_tasks = [
            step.qraft_task
            for step in workflow.steps.all()
            if step.qraft_task_id is not None
        ]

    usages = [aggregate_usage(qraft_task) for qraft_task in qraft_tasks]
    return _sum_usage(usages)
