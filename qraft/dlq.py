"""
Dead-letter queue: inspect and requeue tasks that failed permanently.

No dedicated table - "dead" is just QraftTask.status in (FAILED, EXHAUSTED).
Requeueing reuses the retry machinery's Schedule+marker pattern so the
requeued run lands as the next attempt on the same QraftTask, preserving
history instead of starting a new lineage.
"""

import logging
from datetime import datetime, timezone

from django.db import transaction
from django.db.models import QuerySet

from .models import QraftTask, TaskStatus
from .retry import QRAFT_MARKER_FMT, QRAFT_MARKER_PREFIX, QRAFT_RETRY_NAME_FMT

logger = logging.getLogger("qraft")

DEAD_STATUSES = (TaskStatus.FAILED, TaskStatus.EXHAUSTED)


def dead_letters() -> QuerySet[QraftTask]:
    """Return dead tasks (FAILED or EXHAUSTED), newest first."""
    return (
        QraftTask.objects.filter(status__in=DEAD_STATUSES)
        .prefetch_related("attempts")
        .order_by("-date_created")
    )


def requeue(qraft_task: QraftTask) -> str:
    """
    Re-enqueue a dead task's stored func/args/kwargs.

    Schedules the run for immediate execution (next_run=now) using the same
    Schedule+marker mechanism as RetryPolicy.schedule_retry, so it continues
    the attempt series on the same QraftTask and status returns to PENDING.

    Raises:
        ValueError: if the task isn't currently FAILED or EXHAUSTED.

    Returns:
        str: the created Django-Q2 Schedule ID.
    """
    from django_q.models import Schedule

    if qraft_task.status not in DEAD_STATUSES:
        raise ValueError(
            f"QraftTask {qraft_task.id} is {qraft_task.status!r}, not dead "
            f"(expected one of {[s.value for s in DEAD_STATUSES]})"
        )

    latest = qraft_task.latest_attempt
    next_attempt = (latest.attempt_number if latest else 0) + 1

    marker = QRAFT_MARKER_FMT.format(
        prefix=QRAFT_MARKER_PREFIX, task_id=qraft_task.id, attempt=next_attempt
    )
    schedule_kwargs = {"q_options": {"task_name": marker}}

    with transaction.atomic():
        # Scheduled via qraft.runner.run_task rather than qraft_task.func
        # directly: a @task-decorated function's dotted path resolves to the
        # non-callable django.tasks wrapper, not the function itself.
        schedule = Schedule.objects.create(
            name=QRAFT_RETRY_NAME_FMT.format(
                task_id=qraft_task.id, attempt=next_attempt
            ),
            func="qraft.runner.run_task",
            args=repr(
                (qraft_task.func, list(qraft_task.task_args), qraft_task.task_kwargs)
            ),
            kwargs=repr(schedule_kwargs),
            hook="qraft.hooks.qraft_hook_handler",
            schedule_type=Schedule.ONCE,
            next_run=datetime.now(timezone.utc),
        )

        qraft_task.status = TaskStatus.PENDING
        qraft_task.save(update_fields=["status", "date_updated"])

    logger.info(
        "Requeued QraftTask %s as attempt %d (schedule_id=%s)",
        qraft_task.id,
        next_attempt,
        schedule.id,
    )

    return str(schedule.id)
