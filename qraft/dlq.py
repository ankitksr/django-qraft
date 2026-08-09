"""
Dead-letter queue: inspect and requeue tasks that failed permanently.

No dedicated table - "dead" is just QraftTask.status in (FAILED, EXHAUSTED).
Requeueing reuses the retry machinery's scheduled-attempt path so the requeued
run lands as the next attempt on the same QraftTask, preserving history
instead of starting a new lineage.
"""

import logging

from django.db.models import QuerySet
from django.utils import timezone

from .models import QraftTask, TaskStatus

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

    Creates an immediately-due SCHEDULED attempt, so the run continues the
    attempt series on the same QraftTask and status returns to PENDING.

    The target cluster comes from the task's last attempt, not from the
    process calling this: an operator's shell has a `Conf.CLUSTER_NAME` that
    says nothing about where the task ran. A task whose attempts predate that
    record leaves it null, which now means "the first dispatcher to claim it"
    rather than the old "only a cluster named after the default prefix".

    Raises:
        ValueError: if the task isn't currently FAILED or EXHAUSTED.

    Returns:
        str: id of the scheduled QraftTaskAttempt.
    """
    from .scheduler import _inherited_cluster, schedule_attempt

    if qraft_task.status not in DEAD_STATUSES:
        raise ValueError(
            f"QraftTask {qraft_task.id} is {qraft_task.status!r}, not dead "
            f"(expected one of {[s.value for s in DEAD_STATUSES]})"
        )

    latest = qraft_task.latest_attempt
    next_attempt = (latest.attempt_number if latest else 0) + 1

    attempt = schedule_attempt(
        qraft_task,
        next_attempt,
        timezone.now(),
        cluster=_inherited_cluster(qraft_task),
    )

    logger.info(
        "Requeued QraftTask %s as attempt %d (attempt_id=%s)",
        qraft_task.id,
        next_attempt,
        attempt.id,
    )

    return str(attempt.id)
