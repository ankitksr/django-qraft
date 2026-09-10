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

from . import metrics, signals
from .conf import get_conf
from .hooks import _owning_workflow_cancelled, announce_resolution, qraft_hook_handler
from .models import QraftTask, QraftTaskAttempt, TaskStatus
from .retry import handle_task_retry

logger = logging.getLogger("qraft")


def _heartbeat_grace(heartbeat_interval: float) -> float:
    """Seconds a heartbeat may go unrefreshed before the worker counts as dead."""
    return max(3 * heartbeat_interval, get_conf().min_heartbeat_grace)


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
        from django_q.brokers.orm import ORM
        from django_q.models import OrmQ

        from qraft.brokers import delivering_broker

        if not isinstance(delivering_broker(), ORM):
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
        metrics.counter("qraft.reaper.action", rearmed, action="rearmed")
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

    One residual remains: with SAVE_LIMIT > 0 django_q may trim the saved
    success row before the >= 90s grace elapses on a busy cluster; after that
    the orphan sweep re-executes a succeeded task. (An attempt that resolved
    but crashed before workflow routing / hook dispatch is not this sweep's
    problem - `replay_unrouted()` recovers it through the `routed` flag.)

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
    if replayed:
        metrics.counter("qraft.reaper.action", replayed, action="reconciled")
    return replayed


def replay_unrouted(grace: float | None = None) -> int:
    """
    Re-run routing and hook dispatch for resolved attempts that never got it.

    Resolution commits before workflow routing and hook dispatch run
    (hooks.py); a monitor crash in that window used to wedge the workflow at
    RUNNING forever, or drop the task's hooks. Such an attempt is resolved
    with `routed=False`. Once the resolution is older than `grace`, routing
    and dispatch are replayed - both are idempotent (the parallel `counted`
    flag, the chain step-index CAS, and the HookDispatch unique rows), so a
    replay racing the live handler's own slow post-commit work double-does
    nothing.

    Args:
        grace: Seconds a resolved attempt may sit unrouted before replay
            (default: the heartbeat grace).

    Returns:
        Number of attempts replayed.
    """
    from . import graphs
    from .dispatchers import route_workflow_completion
    from .hooks import HookDispatcher

    conf = get_conf()
    grace = grace if grace is not None else _heartbeat_grace(conf.heartbeat_interval)
    cutoff = timezone.now() - timedelta(seconds=grace)

    unrouted = QraftTaskAttempt.objects.filter(
        success__isnull=False,
        routed=False,
        date_completed__lt=cutoff,
    ).select_related(
        "qraft_task",
        "qraft_task__chain_step",
        "qraft_task__chain_step__chain",
    )

    replayed = 0
    for attempt in unrouted:
        logger.warning(
            "Replaying routing/hooks for attempt %d of QraftTask %s; its "
            "resolution committed but post-commit dispatch never ran",
            attempt.attempt_number,
            attempt.qraft_task_id,
        )
        if attempt.qraft_task.graph_node_id:
            graphs.handle_node_completion(attempt.qraft_task, attempt)
            HookDispatcher(attempt.qraft_task, attempt).dispatch(attempt.success)
        elif not route_workflow_completion(attempt.qraft_task, attempt):
            HookDispatcher(attempt.qraft_task, attempt).dispatch(attempt.success)
        QraftTaskAttempt.objects.filter(id=attempt.id).update(routed=True)
        replayed += 1
    if replayed:
        metrics.counter("qraft.reaper.action", replayed, action="replayed")
    return replayed


def flag_stalls(grace: float | None = None) -> int:
    """
    Flag attempts that are alive but not moving.

    An attempt qualifies when its task declares `stall_after`, its heartbeat is
    fresh (the worker is up), it has no outcome yet, it has not already been
    flagged, and `coalesce(progress_advanced_at, date_started)` is older than
    `stall_after` seconds. `progress_advanced_at`, not `progress_reported_at`:
    a task that narrates its own hang every ten seconds must not defeat the
    check.

    Nothing is resolved and nothing is retried. The attempt keeps running, and
    while the hook handler's compare-and-swap would drop its reported result,
    nothing drops its database writes or its provider charges - a retry that
    starts while attempt 1 is still writing produces two attempts writing the
    same rows. Acting on the flag is the application's decision; see
    `qraft.context.current_attempt_id()` for the ownership pattern that makes
    its writes safe against an overlapping attempt.

    The orphan sweep runs first, so an attempt is never both orphaned and
    stalled. A `stall_after` below the heartbeat grace is not a mistake: it
    measures a different condition, and no warning is emitted for it.

    Args:
        grace: Seconds a heartbeat may go unrefreshed and still count as alive
            (default: the heartbeat grace).

    Returns:
        Number of attempts flagged.
    """
    conf = get_conf()
    grace = grace if grace is not None else _heartbeat_grace(conf.heartbeat_interval)
    now = timezone.now()
    heartbeat_cutoff = now - timedelta(seconds=grace)

    # The threshold is per task, so the age test happens in Python. The
    # candidate set is only the running attempts of tasks that opted in.
    candidates = QraftTaskAttempt.objects.filter(
        success__isnull=True,
        stall_suspected_at__isnull=True,
        date_started__isnull=False,
        heartbeat_at__gte=heartbeat_cutoff,
        qraft_task__stall_after__isnull=False,
        qraft_task__status=TaskStatus.RUNNING,
    ).select_related("qraft_task")

    flagged = 0
    for attempt in candidates:
        moved = attempt.progress_advanced_at or attempt.date_started
        if (now - moved).total_seconds() < attempt.qraft_task.stall_after:
            continue
        # Re-check the candidate conditions on the write, not just the flag:
        # the monitor can resolve this attempt between the query above and here,
        # and a stall flagged on an attempt that already succeeded is a lie the
        # dashboard and the signal would both repeat.
        if not QraftTaskAttempt.objects.filter(
            id=attempt.id,
            stall_suspected_at__isnull=True,
            success__isnull=True,
            qraft_task__status=TaskStatus.RUNNING,
        ).update(stall_suspected_at=now):
            continue

        attempt.stall_suspected_at = now
        logger.warning(
            "Attempt %d of QraftTask %s has not advanced for %.0fs (stall_after=%ds); "
            "flagging, not resolving",
            attempt.attempt_number,
            attempt.qraft_task_id,
            (now - moved).total_seconds(),
            attempt.qraft_task.stall_after,
        )
        signals.send(
            signals.attempt_stall_suspected,
            QraftTaskAttempt,
            signals.attempt_payload(attempt, attempt.qraft_task, outcome=None),
        )
        metrics.counter(
            "qraft.attempt.stall_suspected",
            func=attempt.qraft_task.func,
            cluster=attempt.cluster,
        )
        flagged += 1

    if flagged:
        metrics.counter("qraft.reaper.action", flagged, action="stalled")
    return flagged


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

    # Same principle one step later in the pipeline: resolved attempts whose
    # post-commit routing/hook dispatch died get that work replayed.
    replay_unrouted(grace)

    # Observation only: a run open past its threshold is flagged, never failed.
    # An unenqueued stage is an application defect, and skip/cancel/abandon are
    # the operator's tools for it.
    from . import graphs

    graphs.flag_overdue()
    graphs.replay_settled_hooks(grace)

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

    reaped = sum(_reap_one(attempt_id, heartbeat_cutoff) for attempt_id in orphan_ids)

    # After the orphan sweep, so an attempt is never both orphaned and stalled.
    flag_stalls(grace)
    return reaped


def _resolve_committed(attempt, qraft_task) -> bool:
    """
    Resolve an attempt whose output provably committed as a success.

    Called under the task's row lock from `_reap_one`. The receipt is
    authoritative: it exists only because the application's writes committed in
    the same transaction, so the lost Django-Q2 result says nothing about
    whether the work happened.
    """
    from qraft import graphs

    now = timezone.now()
    attempt.success = True
    attempt.exception_class = None
    attempt.date_completed = now
    attempt.save(update_fields=["success", "exception_class", "date_completed"])

    qraft_task.status = TaskStatus.SUCCEEDED
    qraft_task.save(update_fields=["status", "date_updated"])

    logger.warning(
        "Attempt %d of QraftTask %s lost its result but had already published "
        "at %s; resolving it as succeeded",
        attempt.attempt_number,
        qraft_task.id,
        attempt.output_committed_at.isoformat(),
    )

    transaction.on_commit(lambda: graphs.handle_node_completion(qraft_task, attempt))
    QraftTaskAttempt.objects.filter(id=attempt.id).update(routed=True)
    return True


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

        if attempt.output_committed_at is not None:
            # The node published inside qraft.graphs.publish(), so its writes
            # and its receipt committed together. The work is done and only the
            # result was lost; reaping it as a failure would retry work that is
            # already durable.
            return _resolve_committed(attempt, qraft_task)

        attempt.success = False
        # Two different failures reach this point and a retry policy should be
        # allowed to treat them differently. `returned_at` set means the target
        # ran to the end and only its result was lost, so the work is already
        # done and a retry re-does it; unset means the worker died somewhere
        # inside the function, with no way to know how far it got.
        attempt.exception_class = (
            "ResultLost" if attempt.returned_at is not None else "OrphanedTask"
        )
        attempt.date_completed = timezone.now()
        # routed follows the same protocol as the hook handler: True when a
        # retry absorbs the failure (no post-commit work), set after routing
        # otherwise, so replay_unrouted() can recover a crash in between.
        attempt.save(update_fields=["success", "exception_class", "date_completed"])

        logger.warning(
            "Reaping attempt %d of QraftTask %s as %s (q2_task_id=%s)",
            attempt.attempt_number,
            qraft_task.id,
            attempt.exception_class,
            attempt.q2_task_id,
        )

        retry_scheduled = not _owning_workflow_cancelled(
            qraft_task
        ) and handle_task_retry(qraft_task, attempt)
        if retry_scheduled:
            QraftTaskAttempt.objects.filter(id=attempt.id).update(routed=True)
        else:
            # handle_task_retry only sets EXHAUSTED when a policy exists;
            # with no policy at all it leaves status untouched.
            qraft_task.refresh_from_db(fields=["status"])
            if qraft_task.status == TaskStatus.RUNNING:
                qraft_task.status = TaskStatus.FAILED
                qraft_task.save(update_fields=["status", "date_updated"])
            routable = (qraft_task, attempt)

        announce_resolution(
            attempt, qraft_task, "orphaned", settled=not retry_scheduled
        )
        metrics.counter_on_commit("qraft.reaper.action", action="orphaned")

    if routable is not None:
        from . import graphs
        from .dispatchers import route_workflow_completion
        from .hooks import HookDispatcher

        qraft_task, routable_attempt = routable
        if qraft_task.graph_node_id:
            graphs.handle_node_completion(qraft_task, routable_attempt)
            HookDispatcher(qraft_task, routable_attempt).dispatch(False)
        elif not route_workflow_completion(qraft_task, routable_attempt):
            HookDispatcher(qraft_task, routable_attempt).dispatch(False)
        QraftTaskAttempt.objects.filter(id=attempt.id).update(routed=True)

    return True
