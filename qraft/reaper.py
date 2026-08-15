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
from django.db.models import Q
from django.utils import timezone
from django_q.models import Task as Q2Task

from .conf import get_conf
from .hooks import qraft_hook_handler
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
    Returns None whenever queue membership can't be established, meaning
    "unknown, don't reap": a non-ORM broker (Redis) holds its queue where
    this check can't see, so a long-queued unstarted task would look
    orphaned and get duplicated.
    """
    try:
        from django_q.brokers import get_broker
        from django_q.brokers.orm import ORM
        from django_q.models import OrmQ

        if not isinstance(get_broker(), ORM):
            return None

        return {
            task_id
            for task_id in (ormq.task_id() for ormq in OrmQ.objects.all())
            if task_id
        }
    except Exception:
        logger.exception("Could not read the broker queue; skipping unstarted attempts")
        return None


def rearm_stuck_claims(grace: float | None = None) -> int:
    """
    Return dispatcher claims that never reached the broker to SCHEDULED.

    A QUEUED attempt with no `q2_task_id` was claimed by a dispatcher that
    died before its enqueue was recorded. Nothing was executed and nothing is
    in a queue, so re-arming it is not a duplicate - it is the only way the
    attempt ever runs.

    Against the ORM broker the claim and the enqueue commit together, so this
    state is unreachable and this sweep is dead weight. It exists for brokers
    that write outside the database, where the two cannot be made atomic.

    Note the asymmetry with a plain SCHEDULED attempt, however overdue: that
    one is a live task waiting for a dispatcher (every cluster may simply be
    down), not an orphan, and reaping it would burn a retry on a run that
    never happened. Nothing here touches SCHEDULED rows.

    Returns:
        Number of attempts re-armed.
    """
    conf = get_conf()
    grace = grace if grace is not None else _heartbeat_grace(conf.heartbeat_interval)
    cutoff = timezone.now() - timedelta(seconds=grace)

    rearmed = QraftTaskAttempt.objects.filter(
        state=QraftTaskAttempt.AttemptState.QUEUED,
        q2_task_id__isnull=True,
        success__isnull=True,
        claimed_at__lt=cutoff,
    ).update(state=QraftTaskAttempt.AttemptState.SCHEDULED, claimed_at=None)

    if rearmed:
        logger.warning("Re-armed %d attempt(s) claimed but never enqueued", rearmed)
    return rearmed


def reconcile_finished(grace: float | None = None) -> int:
    """
    Replay saved completions whose hook handler never resolved the attempt.

    The monitor saved the Django-Q2 Task row, but the post_save hook handler
    died before resolving the attempt (django_q swallows the exception): the
    attempt sits at success=None with its task RUNNING, and the orphan sweep
    excludes it forever because a Q2 row exists. Once the completion is older
    than `grace`, the saved row is replayed through the normal hook handler,
    which resolves the attempt, routes retries and workflow completions, and
    dispatches hooks exactly as the monitor would have. Resolution is
    compare-and-set, so a replay racing a genuine resolution updates no rows
    and drops out.

    Staleness runs from the Q2 row's `stopped`: that is the moment the hook
    handler had its chance, so a completion still inside `grace` is left for
    the monitor's own delivery to resolve.

    Two residuals remain:
    (a) With SAVE_LIMIT > 0 django_q may trim the saved success row before
        the >= 90s grace elapses on a busy cluster; after that the orphan
        sweep re-executes a succeeded task.
    (b) It cannot replay attempts that resolved but crashed before workflow
        routing / hook dispatch (the post-commit window in hooks.py) -
        resolution is fenced by the CAS, so those stay "resolved" with no
        further recovery here.

    Args:
        grace: Seconds a saved completion may sit unresolved before it is
            replayed (default: the heartbeat grace).

    Returns:
        Number of completions replayed.
    """
    conf = get_conf()
    grace = grace if grace is not None else _heartbeat_grace(conf.heartbeat_interval)
    cutoff = timezone.now() - timedelta(seconds=grace)

    unresolved_q2_ids = QraftTaskAttempt.objects.filter(
        success__isnull=True,
        qraft_task__status=TaskStatus.RUNNING,
        state=QraftTaskAttempt.AttemptState.QUEUED,
        q2_task_id__isnull=False,
    ).values("q2_task_id")

    replayed = 0
    for q2_task in Q2Task.objects.filter(id__in=unresolved_q2_ids, stopped__lt=cutoff):
        logger.warning(
            "Replaying saved completion of q2 task %s; its hook handler "
            "never resolved the attempt",
            q2_task.id,
        )
        qraft_hook_handler(q2_task)
        replayed += 1
    return replayed


def reap_orphans(stale_after: float | None = None) -> int:
    """
    Find and resolve orphaned QraftTaskAttempts.

    An attempt is orphaned when its QraftTask is still RUNNING, the attempt
    itself is unresolved (success is None) and already handed to a broker, no
    Django-Q2 Task row exists for it, and either:

    - its lease heartbeat is stale (the worker started the task and died), or
    - it never heartbeat at all, has been in a queue longer than `stale_after`,
      and its pack is no longer queued (it was delivered to a worker that died
      before it could start, or was lost by a broker without delivery
      receipts).

    SCHEDULED attempts are excluded throughout: they are waiting by design,
    not stuck. See `rearm_stuck_claims()` for the one dispatcher-side failure
    that does need recovering.

    Args:
        stale_after: Seconds an attempt that never started may sit unresolved
            before it is considered orphaned (default: conf.reap_stale_after).

    Returns:
        Number of attempts reaped.
    """
    conf = get_conf()
    stale_after = stale_after if stale_after is not None else conf.reap_stale_after
    grace = _heartbeat_grace(conf.heartbeat_interval)

    # Attempts with a saved Q2 row are not orphans - their completion exists
    # and only needs replaying. Handled first so this sweep's Q2-row exclusion
    # below never turns "hook handler died after save" into a permanent stall.
    reconcile_finished(grace)

    now = timezone.now()
    heartbeat_cutoff = now - timedelta(seconds=grace)
    stale_cutoff = now - timedelta(seconds=stale_after)

    unresolved = (
        QraftTaskAttempt.objects.filter(
            success__isnull=True,
            qraft_task__status=TaskStatus.RUNNING,
            state=QraftTaskAttempt.AttemptState.QUEUED,
        )
        .exclude(q2_task_id__isnull=True)
        .exclude(q2_task_id__in=Q2Task.objects.values("id"))
    )

    orphan_ids = list(
        unresolved.filter(heartbeat_at__lt=heartbeat_cutoff).values_list(
            "id", flat=True
        )
    )

    # Staleness runs from the enqueue, not from row creation: a scheduled
    # attempt's row can be hours older than its dispatch, and measuring from
    # creation would make every long backoff look stale the moment it is
    # queued.
    never_started = list(
        unresolved.filter(heartbeat_at__isnull=True)
        .filter(
            Q(claimed_at__lt=stale_cutoff)
            | Q(claimed_at__isnull=True, date_created__lt=stale_cutoff)
        )
        .values_list("id", "q2_task_id")
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
    """
    Reap a single attempt under a row lock, re-checking conditions still hold.

    Workflow routing happens after the lock is released: a reaped member of a
    chain/iter/batch has to reach its dispatcher or the workflow waits on a
    completion that will never arrive, and dispatching a workflow hook is not
    work to do while holding the task row.
    """
    routable = None
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
            routable = (qraft_task, attempt)

    if routable is not None:
        from .dispatchers import route_workflow_completion

        route_workflow_completion(*routable)

    return True
