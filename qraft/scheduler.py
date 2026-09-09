"""
Qraft-owned scheduling.

A delayed attempt is a row, not a Django-Q2 `Schedule`. `schedule_attempt()`
writes the `QraftTaskAttempt` up front - final attempt number, target cluster,
`not_before` due time - in the SCHEDULED state, and `dispatch_due()` later
claims it and hands it to the broker, stamping the q2 task id as it goes.

Two properties follow. Delay is exact rather than quantized to Django-Q2's
hardcoded 30-second scheduler cycle (`django_q/cluster.py`, `counter >= 30`),
so a 2-second backoff arrives in about 2 seconds. And because the row exists
before execution, the attempt no longer has to be reconstructed from a marker
in the task name after the fact.

Claiming is a compare-and-swap on `state`, so two dispatcher threads - or the
dispatchers of two different clusters, which all poll the same table - can
never enqueue one attempt twice. `SELECT ... FOR UPDATE SKIP LOCKED` is added
where the database offers it, as a contention optimisation only; correctness
rests on the UPDATE matching a row, which is also true on SQLite.
"""

import logging
import time

from django.db import close_old_connections, connection, models, transaction
from django.utils import timezone

from . import metrics
from .conf import executing_cluster, get_conf

logger = logging.getLogger("qraft")

# Floor on the idle sleep, so a due-but-undispatchable row cannot busy-loop.
MIN_SLEEP = 0.05

# Sleep after an unexpected failure, so a broken broker or config does not
# turn the loop into a hot retry.
ERROR_BACKOFF = 5.0

# Worker-side entry point for an attempt that carries no dispatch override.
# Resolves the task's own dotted path, unwrapping a django.tasks @task
# wrapper - see qraft.runner.
DEFAULT_DISPATCH_FUNC = "qraft.runner.run_task"


def _inherited_cluster(qraft_task) -> str | None:
    """
    Cluster the task's most recent attempt was routed to.

    This is what makes a retry or a DLQ requeue land where the work belongs
    without the caller having to know: the enqueuing process records the
    cluster on attempt 1, and every later attempt inherits it. An operator
    requeueing from a shell has no such knowledge, and neither does
    `Conf.CLUSTER_NAME` in that shell.
    """
    latest = qraft_task.latest_attempt
    return latest.cluster if latest else None


def schedule_attempt(
    qraft_task,
    attempt_number: int,
    not_before,
    cluster: str | None = None,
    dispatch_func: str | None = None,
    dispatch_args: list | None = None,
):
    """
    Create the SCHEDULED attempt that a dispatcher will enqueue at `not_before`.

    The parent task returns to PENDING in the same transaction, so a task with
    a scheduled attempt is never left looking terminal.

    Args:
        qraft_task: QraftTask this attempt belongs to.
        attempt_number: Final attempt number, decided now rather than at
            execution time.
        not_before: When the attempt becomes due (past means "immediately").
        cluster: Target cluster. None lets any dispatcher claim it and route
            it to its own cluster.
        dispatch_func: Dotted path to enqueue instead of the task's own
            function. Only the django.tasks TaskContext wrapper needs this.
        dispatch_args: Positional arguments for `dispatch_func`.

    Returns:
        QraftTaskAttempt: the SCHEDULED row.
    """
    from .models import QraftTask, QraftTaskAttempt, TaskStatus
    from .models.tasks import AttemptState

    # The trace begun at the original enqueue continues across retries.
    trace_context = (
        qraft_task.attempts.order_by("-attempt_number")
        .values_list("trace_context", flat=True)
        .first()
    )

    with transaction.atomic():
        # Locked before the insert because `context._snapshot_progress` takes
        # the same lock: the attempt this row supersedes must not still be
        # reporting itself as the task's latest once this one exists.
        QraftTask.objects.select_for_update().filter(pk=qraft_task.pk).first()
        attempt = QraftTaskAttempt.objects.create(
            qraft_task=qraft_task,
            attempt_number=attempt_number,
            state=AttemptState.SCHEDULED,
            not_before=not_before,
            cluster=cluster,
            dispatch_func=dispatch_func,
            dispatch_args=dispatch_args,
            trace_context=trace_context,
        )
        qraft_task.status = TaskStatus.PENDING
        qraft_task.save(update_fields=["status", "date_updated"])

    logger.info(
        "Scheduled QraftTask %s attempt %d for %s (cluster=%s)",
        qraft_task.id,
        attempt_number,
        not_before,
        cluster or "any",
    )
    return attempt


def _enqueue(attempt, qraft_task) -> str:
    """Hand one claimed attempt to the broker and return its q2 task id."""
    from django_q.tasks import async_task as q2_async_task

    from .brokers import broker_for_cluster
    from .retry import QRAFT_MARKER_FMT, QRAFT_MARKER_PREFIX

    func = attempt.dispatch_func or DEFAULT_DISPATCH_FUNC
    args = attempt.dispatch_args
    if args is None:
        args = [qraft_task.func, list(qraft_task.task_args), qraft_task.task_kwargs]

    # No cluster stamped: whichever cluster's dispatcher wins the claim race
    # on this row runs it under its own cluster - cross-cluster placement is
    # unspecified here, not guaranteed.
    target = attempt.cluster or executing_cluster()

    q2_kwargs = {
        "hook": "qraft.hooks.qraft_hook_handler",
        # The q2 task id is stamped below, so linkage no longer depends on
        # this name. It stays because it is what a pre-1.3 delivery still in
        # flight is resolved by (qraft.hooks.attempt_from_marker), and it
        # keeps the two paths reading identically in the admin.
        "task_name": QRAFT_MARKER_FMT.format(
            prefix=QRAFT_MARKER_PREFIX,
            task_id=qraft_task.id,
            attempt=attempt.attempt_number,
        ),
        # Qraft owns retries; the broker must not redeliver a failed message
        # underneath a retry Qraft has already scheduled.
        "ack_failure": True,
        "cluster": target,
    }

    # Priority survives the delay: the lane is rebuilt from the task's own
    # lane and the cluster this attempt is routed to. django_q drops
    # `cluster=` whenever an explicit broker is given, which is why the lane
    # key and the broker itself both have to carry the target cluster.
    q2_kwargs["broker"] = broker_for_cluster(target, qraft_task.priority)

    return q2_async_task(func, *args, **q2_kwargs)


def _claim_and_enqueue(now) -> bool | None:
    """
    Claim the most overdue attempt and enqueue it.

    Returns True when one was enqueued, False when another dispatcher won the
    row (the caller should move on to the next), and None when nothing is due.

    Everything happens in one transaction. Against the ORM broker that makes
    the claim and the enqueue a single atomic act, so a process killed at any
    point leaves the attempt SCHEDULED with nothing queued - the state that
    re-dispatches cleanly. A broker that writes outside the database (Redis)
    cannot offer that, and keeps the usual dual-write window.
    """
    from .models import QraftTask, QraftTaskAttempt, TaskStatus
    from .models.tasks import AttemptState

    with transaction.atomic():
        due = QraftTaskAttempt.objects.filter(
            state=AttemptState.SCHEDULED, not_before__lte=now
        ).order_by("not_before")
        if connection.features.has_select_for_update_skip_locked:
            due = due.select_for_update(skip_locked=True)
        attempt = due.first()
        if attempt is None:
            return None

        # `now` is the due cutoff the whole pass shares; the claim stamps its
        # own time, or a slow batch would backdate every attempt after the
        # first and report their queue wait as longer than it was.
        claimed_at = timezone.now()

        # The compare-and-swap, not the read above, is what decides ownership:
        # SQLite has no SKIP LOCKED, so two dispatchers can read the same row.
        claimed = QraftTaskAttempt.objects.filter(
            pk=attempt.pk, state=AttemptState.SCHEDULED
        ).update(state=AttemptState.QUEUED, claimed_at=claimed_at)
        if not claimed:
            return False

        qraft_task = attempt.qraft_task
        attempt.state = AttemptState.QUEUED
        attempt.claimed_at = claimed_at
        attempt.enqueued_at = claimed_at
        # `_enqueue` routes an unstamped attempt to this dispatcher's own
        # cluster; recording where it actually went is what gives the start
        # and pickup metrics a cluster label, puts it in the active gauge,
        # and lets the next attempt inherit the placement.
        attempt.cluster = attempt.cluster or executing_cluster()
        attempt.q2_task_id = _enqueue(attempt, qraft_task)
        attempt.save(
            update_fields=[
                "state",
                "claimed_at",
                "enqueued_at",
                "cluster",
                "q2_task_id",
            ]
        )

        # In a broker queue now, which is what brings it inside the reaper's
        # reach if the delivery is lost. Scoped to PENDING so a duplicate
        # delivery can never resurrect a task that has already settled.
        QraftTask.objects.filter(id=qraft_task.id, status=TaskStatus.PENDING).update(
            status=TaskStatus.RUNNING, date_updated=timezone.now()
        )

    lag = metrics.seconds_between(attempt.not_before, attempt.claimed_at)
    if lag is not None:
        metrics.histogram("qraft.scheduler.lag", lag, cluster=attempt.cluster)
    logger.debug(
        "Dispatched QraftTask %s attempt %d as q2 task %s",
        attempt.qraft_task_id,
        attempt.attempt_number,
        attempt.q2_task_id,
    )
    return True


def dispatch_due(limit: int | None = None) -> int:
    """
    Enqueue every attempt whose `not_before` has passed, up to `limit`.

    Returns the number enqueued.
    """
    limit = limit if limit is not None else get_conf().dispatch_batch
    now = timezone.now()
    dispatched = 0

    # Bounded rather than `while True`: a row is either enqueued or lost to a
    # competing dispatcher, and neither outcome leaves it claimable again, so
    # the extra headroom only absorbs contention.
    for _ in range(2 * limit):
        if dispatched >= limit:
            break
        try:
            outcome = _claim_and_enqueue(now)
        except Exception:
            # Almost always the broker being unreachable, which the next row
            # would hit too.
            logger.exception("Scheduler could not enqueue a due attempt")
            break
        if outcome is None:
            break
        dispatched += bool(outcome)

    return dispatched


def _seconds_until_due(ceiling: float) -> float:
    """
    How long the loop may sleep without overshooting the next due attempt.

    Without this the poll interval would be a floor on every delay, which is
    the defect this module exists to remove: a 2-second backoff must arrive in
    about 2 seconds, not at the next tick after it.
    """
    from .models import QraftTaskAttempt
    from .models.tasks import AttemptState

    earliest = QraftTaskAttempt.objects.filter(state=AttemptState.SCHEDULED).aggregate(
        models.Min("not_before")
    )["not_before__min"]

    if earliest is None:
        return ceiling
    return max(MIN_SLEEP, min(ceiling, (earliest - timezone.now()).total_seconds()))


def dispatch_loop(stop_event=None) -> None:
    """
    Poll for due attempts until `stop_event` is set. Runs beside the monitor.
    """
    while stop_event is None or not stop_event.is_set():
        try:
            close_old_connections()
            dispatch_due()
            if get_conf().metrics_gauges:
                from .metrics.gauges import emit_gauges

                emit_gauges()
            delay = _seconds_until_due(get_conf().dispatch_interval)
        except Exception:
            logger.exception("Scheduler dispatch pass failed")
            delay = ERROR_BACKOFF
        finally:
            close_old_connections()
        time.sleep(delay)
