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
import threading

from django.db import close_old_connections, connections
from django.utils import timezone

from .conf import get_conf

_logger = logging.getLogger("qraft")

# Ceiling on a heartbeat thread's lifetime when no task timeout is configured.
DEFAULT_MAX_LEASE_SECONDS = 24 * 3600

# Extra seconds allowed past the task timeout before the backstop fires.
LEASE_DEADLINE_MARGIN = 60

# q2_task_id -> Event that stops that task's heartbeat thread.
_stop_events: dict[str, threading.Event] = {}
_stop_events_lock = threading.Lock()


def _lease_deadline_seconds() -> float:
    """Hard upper bound on one lease, derived from the configured task timeout."""
    from django_q.conf import Conf

    timeout = getattr(Conf, "TIMEOUT", None)
    if timeout:
        return timeout + LEASE_DEADLINE_MARGIN
    return DEFAULT_MAX_LEASE_SECONDS


def stamp_start(q2_task_id: str) -> bool:
    """
    Mark an attempt as started and open its lease.

    Returns False when no Qraft attempt owns this Django-Q2 task (a plain
    django_q task), in which case there is nothing to heartbeat.
    """
    from .models import QraftTaskAttempt

    now = timezone.now()
    updated = QraftTaskAttempt.objects.filter(q2_task_id=q2_task_id).update(
        date_started=now, heartbeat_at=now
    )
    return bool(updated)


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
) -> None:
    """Refresh `heartbeat_at` every `interval` seconds until the task ends."""
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
        connections.close_all()


def start_heartbeat(q2_task_id: str) -> threading.Thread | None:
    """Start the one heartbeat thread for this task, if not already running."""
    with _stop_events_lock:
        if q2_task_id in _stop_events:
            return None
        stop_event = threading.Event()
        _stop_events[q2_task_id] = stop_event

    def _run():
        try:
            heartbeat_loop(
                q2_task_id,
                stop_event,
                get_conf().heartbeat_interval,
                _lease_deadline_seconds(),
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


def _on_pre_execute_lease(sender, func, task, **kwargs):
    """django_q `pre_execute` receiver: open the lease and start heartbeating."""
    q2_task_id = task.get("id")
    if not q2_task_id:
        return
    try:
        if not stamp_start(q2_task_id):
            return
    except Exception:
        _logger.exception("Could not open execution lease for %s", q2_task_id)
        return
    start_heartbeat(q2_task_id)


def _on_post_execute(sender, task, **kwargs):
    """django_q `post_execute` receiver: close the lease (monitor-side)."""
    stop_heartbeat(task.get("id") if isinstance(task, dict) else None)
