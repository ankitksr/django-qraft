"""
Orphan task reaper for Django-Qraft.

If a worker process dies mid-task (OOM kill, hard crash, `kill -9`), the
Django-Q2 monitor never sees a result and the QraftTaskAttempt is stuck at
success=None forever with its QraftTask stuck at RUNNING. This module finds
those orphaned attempts and resolves them through the normal retry path.
"""

import logging
from datetime import timedelta

from django.db import transaction
from django.utils import timezone
from django_q.models import Task as Q2Task

from .conf import get_conf
from .models import QraftTask, QraftTaskAttempt, TaskStatus
from .retry import handle_task_retry

logger = logging.getLogger("qraft")


def reap_orphans(stale_after: float | None = None) -> int:
    """
    Find and resolve orphaned QraftTaskAttempts.

    An attempt is orphaned when its QraftTask is still RUNNING, the attempt
    itself is unresolved (success is None), it's older than `stale_after`,
    and no Django-Q2 Task row exists for it - meaning the worker died before
    the monitor could record a result.

    Args:
        stale_after: Seconds an attempt may go unresolved before it's
            considered orphaned (default: conf.reap_stale_after).

    Returns:
        Number of attempts reaped.
    """
    stale_after = (
        stale_after if stale_after is not None else get_conf().reap_stale_after
    )
    cutoff = timezone.now() - timedelta(seconds=stale_after)

    orphan_ids = (
        QraftTaskAttempt.objects.filter(
            success__isnull=True,
            qraft_task__status=TaskStatus.RUNNING,
            date_created__lt=cutoff,
        )
        .exclude(q2_task_id__in=Q2Task.objects.values("id"))
        .values_list("id", flat=True)
    )

    return sum(_reap_one(attempt_id) for attempt_id in list(orphan_ids))


def _reap_one(attempt_id) -> bool:
    """Reap a single attempt under a row lock, re-checking conditions still hold."""
    with transaction.atomic():
        try:
            attempt = QraftTaskAttempt.objects.select_related("qraft_task").get(
                id=attempt_id
            )
        except QraftTaskAttempt.DoesNotExist:
            return False

        qraft_task = QraftTask.objects.select_for_update().get(id=attempt.qraft_task_id)

        # Re-check under lock: the monitor may have resolved this between
        # the sweep query and acquiring the lock.
        if attempt.success is not None or qraft_task.status != TaskStatus.RUNNING:
            return False
        if Q2Task.objects.filter(id=attempt.q2_task_id).exists():
            return False

        attempt.success = False
        attempt.exception_class = "OrphanedTask"
        attempt.date_completed = timezone.now()
        attempt.save(update_fields=["success", "exception_class", "date_completed"])

        logger.warning(
            "Reaping orphaned attempt %d of QraftTask %s (q2_task_id=%s)",
            attempt.attempt_number,
            qraft_task.id,
            attempt.q2_task_id,
        )

        if not handle_task_retry(qraft_task, attempt):
            # handle_task_retry only sets EXHAUSTED when a policy exists;
            # with no policy at all it leaves status untouched.
            qraft_task.refresh_from_db(fields=["status"])
            if qraft_task.status == TaskStatus.RUNNING:
                qraft_task.status = TaskStatus.FAILED
                qraft_task.save(update_fields=["status", "date_updated"])

    return True
