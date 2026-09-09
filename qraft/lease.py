"""
Qraft-owned execution lease.

Django-Q2 writes its `Task` row only when a task *finishes*, so the absence of
that row says nothing about whether the worker is still alive - a task running
for an hour looks exactly like a task whose worker was OOM-killed. The lease
supplies the missing liveness signal: on `pre_execute` the worker stamps
`date_started` and `heartbeat_at` on the attempt, then a daemon thread refreshes
`heartbeat_at` every `heartbeat_interval` seconds until the task ends. A stale
heartbeat is then positive evidence of a dead worker, which is what
`qraft.reaper` reaps on.

Thread safety: worker processes are spawned before any task runs and never fork
afterwards, so a background thread here is safe - the same reasoning that puts
the reaper thread beside the monitor.

Terminating the thread: `post_execute` fires in the *monitor* process, not the
worker, so it only stops the thread in single-process setups. The reliable
terminators are (a) the threaded worker calling `stop_heartbeat()` when the task
returns, (b) the loop's own check - once the monitor records the result the hook
handler resolves the attempt and the heartbeat UPDATE matches no rows - and
(c) an absolute deadline backstop.
"""

import logging
import os
import threading

from django.db import close_old_connections, connections
from django.utils import timezone

from . import context, metrics, signals, tracing
from .conf import get_conf

_logger = logging.getLogger("qraft")

# Ceiling on a heartbeat thread's lifetime when no task timeout is configured.
DEFAULT_MAX_LEASE_SECONDS = 24 * 3600

# Extra seconds allowed past the task timeout before the backstop fires.
LEASE_DEADLINE_MARGIN = 60

# q2_task_id -> Event that stops that task's heartbeat thread.
_stop_events: dict[str, threading.Event] = {}
_stop_events_lock = threading.Lock()

# q2_task_id -> attempt span opened at first start, ended when the lease closes.
_pending_spans: dict[str, object] = {}


def _lease_deadline_seconds() -> float:
    """Hard upper bound on one lease, derived from the configured task timeout."""
    from django_q.conf import Conf

    timeout = getattr(Conf, "TIMEOUT", None)
    if timeout:
        return timeout + LEASE_DEADLINE_MARGIN
    return DEFAULT_MAX_LEASE_SECONDS


# Verdicts from claim_delivery().
FIRST_DELIVERY = "first"  # this delivery started the attempt
REPEAT_DELIVERY = "repeat"  # a further delivery the execution budget allows
REFUSED_DELIVERY = "refused"  # the attempt has already run its allowance
NO_ATTEMPT = "unowned"  # a plain django_q task, with no Qraft row behind it


def claim_delivery(q2_task_id: str) -> str:
    """
    Decide whether this delivery of an attempt may execute, and record it.

    Django-Q2 redelivers a message it never got an acknowledgement for, which
    after a monitor crash means re-running a task whose attempt row is still
    unresolved. Every such re-run refreshed the lease heartbeat, so the
    reaper's liveness test answered "alive" and the attempt never surfaced as
    orphaned; the run stayed OPEN until somebody cancelled it by hand.

    The guard is a compare-and-swap on `execution_count`: at most
    `max_executions_per_attempt` deliveries (default 1) are ever admitted, and
    a resolved attempt admits none. The verdict is memoised on the executing
    context so the lease and `qraft.runner.run_task` share one claim - the
    lease opens the heartbeat on it, the runner refuses to call the function
    on it. `NO_ATTEMPT` is deliberately not memoised: the legacy marker path
    creates the attempt row between two calls (see `open_marker_lease`).

    Returns one of FIRST_DELIVERY, REPEAT_DELIVERY, REFUSED_DELIVERY,
    NO_ATTEMPT.
    """
    claimed = context.delivery_claim()
    if claimed and claimed[0] == q2_task_id:
        return claimed[1]

    verdict, now = _claim(q2_task_id)
    if verdict != NO_ATTEMPT:
        context.set_delivery_claim((q2_task_id, verdict, now))
    if verdict == FIRST_DELIVERY:
        # Here rather than in `stamp_start`, because this is the one call that
        # made the claim: `task_started` and the pickup histogram fire once per
        # attempt, and both the lease receiver and the runner call this.
        try:
            _announce_start(q2_task_id, now)
        except Exception:
            # Observability, never the execution decision.
            _logger.exception("Could not announce the start of %s", q2_task_id)
    return verdict


def _claim(q2_task_id: str):
    """The two compare-and-swaps behind `claim_delivery`. Returns (verdict, now)."""
    from django.db.models import F

    from .models import QraftTaskAttempt

    limit = get_conf().max_executions_per_attempt
    now = timezone.now()
    # ThreadPoolExecutor threads are named via `thread_name_prefix`
    # (qraft.worker uses "qraft_worker"); the standard worker executes on
    # "MainThread", which isn't a pool thread worth reporting.
    thread_name = threading.current_thread().name
    worker_thread = thread_name if thread_name != "MainThread" else None

    # `success__isnull` on both: an attempt the reaper already resolved as
    # orphaned must not run, whatever the execution budget says.
    started = QraftTaskAttempt.objects.filter(
        q2_task_id=q2_task_id,
        date_started__isnull=True,
        success__isnull=True,
        execution_count__lt=limit,
    ).update(
        date_started=now,
        heartbeat_at=now,
        worker_pid=os.getpid(),
        worker_thread=worker_thread,
        execution_count=F("execution_count") + 1,
    )
    if started:
        return FIRST_DELIVERY, now

    repeated = QraftTaskAttempt.objects.filter(
        q2_task_id=q2_task_id,
        success__isnull=True,
        execution_count__lt=limit,
    ).update(
        heartbeat_at=now,
        worker_pid=os.getpid(),
        worker_thread=worker_thread,
        execution_count=F("execution_count") + 1,
    )
    if repeated:
        return REPEAT_DELIVERY, now

    row = (
        QraftTaskAttempt.objects.filter(q2_task_id=q2_task_id)
        .values("qraft_task__func", "cluster")
        .first()
    )
    if row is None:
        return NO_ATTEMPT, now

    _logger.warning(
        "Refusing a repeat delivery of q2 task %s: the attempt has already "
        "used its %d execution(s)",
        q2_task_id,
        limit,
    )
    metrics.counter(
        "qraft.attempt.redelivered",
        func=row["qraft_task__func"],
        cluster=row["cluster"],
    )
    return REFUSED_DELIVERY, now


def stamp_start(q2_task_id: str) -> bool:
    """
    Mark an attempt as started and open its lease.

    Also stamps worker identity (pid, and thread name for threaded workers)
    from inside the executing process - this runs at `pre_execute`, in the
    worker itself, for both the standard django_q worker and qraft's
    threaded worker (see qraft.worker._execute_task_in_thread), so
    `os.getpid()` is always the real worker pid rather than the monitor's.

    The start stamp is conditional on `date_started` being null, so a
    redelivered message stamps nothing and announces nothing: `task_started`
    and the pickup histogram fire once per attempt, only when that
    conditional update matched.

    Returns False both when no Qraft attempt owns this Django-Q2 task (a
    plain django_q task) and when the delivery was refused - neither has a
    lease to heartbeat. A refused delivery is not silently dropped: the
    claim is memoised, and `qraft.runner.run_task` raises
    `RedeliveredAttempt` off it rather than calling the function. Raising
    here instead would be lost, since django_q's worker wraps no try/except
    around a `pre_execute` receiver.
    """
    return claim_delivery(q2_task_id) in (FIRST_DELIVERY, REPEAT_DELIVERY)


def _announce_start(q2_task_id: str, now) -> None:
    """Bind the context, send `task_started`, count the start, open the span."""
    from .models import QraftTaskAttempt

    attempt = QraftTaskAttempt.objects.select_related("qraft_task").get(
        q2_task_id=q2_task_id
    )
    task = attempt.qraft_task
    context.bind_attempt(attempt, task)
    signals.send(
        signals.task_started,
        QraftTaskAttempt,
        signals.attempt_payload(attempt, task, outcome=None),
    )
    metrics.counter("qraft.attempt.started", func=task.func, cluster=attempt.cluster)
    pickup = metrics.seconds_between(attempt.enqueued_at, now)
    if pickup is not None:
        metrics.histogram(
            "qraft.attempt.pickup", pickup, func=task.func, cluster=attempt.cluster
        )
    span = tracing.start_attempt_span(attempt.trace_context, task.func)
    if span is not None:
        with _stop_events_lock:
            _pending_spans[q2_task_id] = span


def touch(q2_task_id: str) -> int:
    """Refresh the lease. Returns 0 once the attempt is resolved or gone."""
    from .models import QraftTaskAttempt

    return QraftTaskAttempt.objects.filter(
        q2_task_id=q2_task_id, success__isnull=True
    ).update(heartbeat_at=timezone.now())


def heartbeat_loop(
    q2_task_id: str,
    stop_event: threading.Event,
    interval: float,
    deadline: float,
    span=None,
) -> None:
    """
    Refresh `heartbeat_at` every `interval` seconds until the task ends.

    The attempt span (if any) ends here, on the worker side, because the loop
    is the one thing that reliably learns the attempt is over in the process
    that ran it: the threaded worker sets the stop event, and the standard
    worker's loop sees the first heartbeat UPDATE match no rows.
    """
    elapsed = 0.0
    try:
        while not stop_event.wait(interval):
            elapsed += interval
            if elapsed >= deadline:
                _logger.warning(
                    "Heartbeat for q2 task %s hit the %.0fs lease deadline, stopping",
                    q2_task_id,
                    deadline,
                )
                break
            close_old_connections()
            if not touch(q2_task_id):
                break
    except Exception:
        _logger.exception("Heartbeat for q2 task %s failed", q2_task_id)
    finally:
        tracing.end_span(span)
        connections.close_all()


def start_heartbeat(q2_task_id: str) -> threading.Thread | None:
    """Start the one heartbeat thread for this task, if not already running."""
    with _stop_events_lock:
        if q2_task_id in _stop_events:
            return None
        stop_event = threading.Event()
        _stop_events[q2_task_id] = stop_event
        span = _pending_spans.pop(q2_task_id, None)

    def _run():
        try:
            heartbeat_loop(
                q2_task_id,
                stop_event,
                get_conf().heartbeat_interval,
                _lease_deadline_seconds(),
                span,
            )
        finally:
            with _stop_events_lock:
                _stop_events.pop(q2_task_id, None)

    thread = threading.Thread(
        target=_run, daemon=True, name=f"qraft-heartbeat-{q2_task_id}"
    )
    thread.start()
    return thread


def stop_heartbeat(q2_task_id: str | None) -> None:
    """Close the lease for a finished task. Safe to call for unknown ids."""
    if not q2_task_id:
        return
    with _stop_events_lock:
        stop_event = _stop_events.get(q2_task_id)
    if stop_event is not None:
        stop_event.set()


def open_marker_lease(task_name: str | None, q2_task_id: str) -> bool:
    """
    Create the attempt row for a marker-carrying task and mark it RUNNING.

    Retries, DLQ requeues and deferred tasks are queued through a Schedule,
    which leaves the QraftTask PENDING with no row for the attempt about to
    run. Until both exist, a worker that dies mid-attempt is invisible to the
    reaper, which only looks at unresolved attempts of RUNNING tasks.

    Returns False when the task carries no Qraft marker.
    """
    from .hooks import attempt_from_marker
    from .models import QraftTask, TaskStatus

    attempt = attempt_from_marker(task_name, q2_task_id)
    if attempt is None:
        return False

    # Scoped to PENDING: that is the state schedule_retry(), dlq.requeue()
    # and the deferred-task path leave behind, and anything else is a
    # duplicate delivery that must not resurrect a settled task.
    QraftTask.objects.filter(
        id=attempt.qraft_task_id, status=TaskStatus.PENDING
    ).update(status=TaskStatus.RUNNING, date_updated=timezone.now())
    return True


def _on_pre_execute_lease(sender, func, task, **kwargs):
    """django_q `pre_execute` receiver: open the lease and start heartbeating."""
    q2_task_id = task.get("id")
    if not q2_task_id:
        return
    try:
        verdict = claim_delivery(q2_task_id)
        if verdict == NO_ATTEMPT:
            # A pre-1.3 Schedule delivery has no attempt row yet; the marker
            # in its task_name creates one, and the claim is then real.
            if not open_marker_lease(task.get("name"), q2_task_id):
                return
            verdict = claim_delivery(q2_task_id)
        if verdict not in (FIRST_DELIVERY, REPEAT_DELIVERY):
            # No heartbeat for a delivery that will not run. Refreshing the
            # lease on a redelivery is exactly what kept a stuck attempt
            # looking alive to the reaper, and a Qraft-dispatched attempt's
            # task_name is a marker, so a refused one reaches the line above.
            return
    except Exception:
        _logger.exception("Could not open execution lease for %s", q2_task_id)
        return
    start_heartbeat(q2_task_id)


def _on_post_execute(sender, task, **kwargs):
    """django_q `post_execute` receiver: close the lease (monitor-side)."""
    stop_heartbeat(task.get("id") if isinstance(task, dict) else None)
