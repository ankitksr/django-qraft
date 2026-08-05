"""
Orphan task reaper for Django-Qraft.

If a worker process dies mid-task (OOM kill, hard crash, `kill -9`), the
Django-Q2 monitor never sees a result and the QraftTaskAttempt is stuck at
success=None forever with its QraftTask stuck at RUNNING. This module finds
those orphaned attempts and resolves them through the normal retry path.

Liveness comes from the Qraft execution lease (`qraft.lease`), not from the
Django-Q2 `Task` row: that row is written only at completion, so "no Task row"
alone would reap every legitimate long-running task.
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

# Floor on the heartbeat grace period, so a short heartbeat_interval can't make
# the reaper trigger-happy under load or clock skew.
MIN_HEARTBEAT_GRACE = 90.0


def _heartbeat_grace(heartbeat_interval: float) -> float:
    """Seconds a heartbeat may go unrefreshed before the worker counts as dead."""
    return max(3 * heartbeat_interval, MIN_HEARTBEAT_GRACE)


def _queued_q2_task_ids() -> set[str] | None:
    """
    Ids of tasks still sitting in the ORM broker queue, undelivered.

    Those legitimately have no heartbeat - no worker has picked them up yet.
    OrmQ stores a signed, pickled pack; `OrmQ.task_id()` unsigns and parses it.
    Returns None if the queue can't be read, meaning "unknown, don't reap".
    """
    try:
        from django_q.models import OrmQ

        return {
            task_id
            for task_id in (ormq.task_id() for ormq in OrmQ.objects.all())
            if task_id
        }
    except Exception:
        logger.exception("Could not read the OrmQ queue; skipping unstarted attempts")
        return None


def reap_orphans(stale_after: float | None = None) -> int:
    """
    Find and resolve orphaned QraftTaskAttempts.

    An attempt is orphaned when its QraftTask is still RUNNING, the attempt
    itself is unresolved (success is None), no Django-Q2 Task row exists for
    it, and either:

    - its lease heartbeat is stale (the worker started the task and died), or
    - it never heartbeat at all, is older than `stale_after`, and its pack is
      no longer queued (it was delivered to a worker that died before it could
      start, or was lost by a broker without delivery receipts).

    Args:
        stale_after: Seconds a never-started attempt may sit unresolved before
            it is considered orphaned (default: conf.reap_stale_after).

    Returns:
        Number of attempts reaped.
    """
    conf = get_conf()
    stale_after = stale_after if stale_after is not None else conf.reap_stale_after
    grace = _heartbeat_grace(conf.heartbeat_interval)

    now = timezone.now()
    heartbeat_cutoff = now - timedelta(seconds=grace)
    created_cutoff = now - timedelta(seconds=stale_after)

    unresolved = QraftTaskAttempt.objects.filter(
        success__isnull=True,
        qraft_task__status=TaskStatus.RUNNING,
    ).exclude(q2_task_id__in=Q2Task.objects.values("id"))

    orphan_ids = list(
        unresolved.filter(heartbeat_at__lt=heartbeat_cutoff).values_list(
            "id", flat=True
        )
    )

    never_started = list(
        unresolved.filter(
            heartbeat_at__isnull=True, date_created__lt=created_cutoff
        ).values_list("id", "q2_task_id")
    )
    if never_started:
        queued = _queued_q2_task_ids()
        if queued is not None:
            orphan_ids += [
                attempt_id
                for attempt_id, q2_task_id in never_started
                if q2_task_id not in queued
            ]

    return sum(_reap_one(attempt_id, heartbeat_cutoff) for attempt_id in orphan_ids)


def _reap_one(attempt_id, heartbeat_cutoff) -> bool:
    """Reap a single attempt under a row lock, re-checking conditions still hold."""
    with transaction.atomic():
        try:
            attempt = QraftTaskAttempt.objects.select_related("qraft_task").get(
                id=attempt_id
            )
        except QraftTaskAttempt.DoesNotExist:
            return False

        qraft_task = QraftTask.objects.select_for_update().get(id=attempt.qraft_task_id)

        # Re-check under lock: the monitor may have resolved this, or the
        # worker may have heartbeat, between the sweep and the lock.
        if attempt.success is not None or qraft_task.status != TaskStatus.RUNNING:
            return False
        if (
            attempt.heartbeat_at is not None
            and attempt.heartbeat_at >= heartbeat_cutoff
        ):
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
