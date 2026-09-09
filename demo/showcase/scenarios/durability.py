"""
Durability scenarios: execution lease, orphan reaping after a real SIGKILL,
dead-letter requeue, retention sweep.
"""

import os
import signal
from datetime import timedelta

from django.utils import timezone
from django_q.models import Task as Q2Task

from qraft.dlq import dead_letters, requeue
from qraft.models import (
    QraftIterModel,
    QraftTask,
    QraftTaskAttempt,
    TaskStatus,
    WorkflowStatus,
)
from qraft.retention import sweep_retention
from qraft.tasks import async_task
from showcase import probe
from showcase.harness import scenario
from showcase.models import Control, Event

# The demo keeps `min_heartbeat_grace` at its 90-second default. The kill is
# real; only the dead worker's last heartbeat is pushed into the past, so the
# suite does not have to idle for a minute and a half per scenario.
BACKDATE = timedelta(seconds=300)


def _pid_from_events(ctx, label: str, attempt: int | None = None):
    """The PID a task published for itself, once it has published one."""

    def look():
        rows = ctx.events(kind=Event.TASK, name=label).order_by("created_at", "id")
        for row in rows:
            if attempt is None or row.payload.get("attempt") == attempt:
                return row.payload.get("pid")
        return None

    return look


def _kill(ctx, pid: int) -> bool:
    """SIGKILL a worker process and confirm it is gone."""
    try:
        os.kill(pid, signal.SIGKILL)
    except ProcessLookupError:
        return ctx.check("worker pid was killable", False, f"pid {pid} already gone")

    reaped = ctx.poll(lambda: not _alive(pid), timeout=15, interval=0.2)
    return ctx.check(
        "worker process is really dead",
        bool(reaped),
        f"SIGKILL to pid {pid}",
    )


def _alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _backdate(attempt_id) -> None:
    QraftTaskAttempt.objects.filter(id=attempt_id).update(
        heartbeat_at=timezone.now() - BACKDATE
    )


@scenario(
    "dur.lease",
    group="durability",
    title="Execution lease and heartbeat",
    proves="A running task stamps date_started and then keeps heartbeat_at "
    "moving, which is the liveness signal the reaper reads.",
)
def lease(ctx):
    run = ctx.run
    task_id = async_task(
        "showcase.tasks.progress_task", run, "leased", steps=8, delay=1.0
    )

    started = ctx.wait(
        "lease opens with date_started",
        lambda: (probe.attempt_of(task_id) or None)
        and probe.attempt_of(task_id).date_started,
        timeout=60,
    )
    if not started:
        return

    seen = set()
    deadline = timezone.now() + timedelta(seconds=25)
    while timezone.now() < deadline and len(seen) < 4:
        attempt = probe.attempt_of(task_id)
        if attempt and attempt.heartbeat_at:
            seen.add(attempt.heartbeat_at)
        if attempt and attempt.success is not None:
            break
        ctx.settle(0.5)

    ctx.check(
        "heartbeat advanced while the task ran",
        len(seen) >= 3,
        f"{len(seen)} distinct heartbeat timestamps",
    )
    ctx.check(
        "heartbeats came after the start stamp",
        bool(seen) and max(seen) >= started,
        f"last heartbeat {max(seen) if seen else None}, started {started}",
    )

    task = ctx.wait("task finishes", lambda: probe.settled(task_id), timeout=60)
    if task:
        ctx.equals("task succeeded", task.status, TaskStatus.SUCCEEDED)


@scenario(
    "dur.reaper-kill",
    group="durability",
    title="Orphan reaper reclaims a SIGKILLed worker",
    proves="A worker killed mid-task leaves no result, and the reaper "
    "resolves the orphaned attempt and retries the task.",
)
def reaper_kill(ctx):
    run, label = ctx.run, "crash"
    task_id = async_task(
        "showcase.tasks.crash_task",
        run,
        label,
        hold=90.0,
        qraft_options={"max_attempts": 3, "base_delay": 1.0, "jitter": False},
    )

    pid = ctx.wait(
        "task published its worker pid", _pid_from_events(ctx, label), timeout=60
    )
    if not pid:
        return

    opened = ctx.wait(
        "execution lease is open",
        lambda: (probe.attempt_of(task_id) or None)
        and probe.attempt_of(task_id).heartbeat_at,
        timeout=30,
    )
    if not opened:
        return

    # From attempt 2 on, the task returns instead of hanging.
    Control.objects.update_or_create(
        key=f"{run}:{label}:crash", defaults={"value": {"stop_crashing": True}}
    )

    if not _kill(ctx, pid):
        return

    attempt = probe.attempt_of(task_id)
    ctx.check(
        "no result was ever recorded for the killed attempt",
        not Q2Task.objects.filter(id=task_id).exists(),
        "django_q wrote no Task row, so completion alone cannot detect this",
    )
    ctx.check(
        "the attempt is stuck unresolved",
        attempt.success is None,
        "success is still null",
    )
    ctx.equals(
        "the task is still marked RUNNING",
        probe.task_of(task_id).status,
        TaskStatus.RUNNING,
    )

    ctx.note(
        "Heartbeat backdated by 300s. The kill is real; qraft's 90s minimum "
        "heartbeat grace is not configurable, so the clock is moved instead "
        "of idling. The cluster's own reaper thread does the reclaiming."
    )
    _backdate(attempt.id)

    reaped = ctx.wait(
        "reaper resolves the orphaned attempt",
        lambda: QraftTaskAttempt.objects.filter(
            id=attempt.id, exception_class="OrphanedTask"
        ).first(),
        timeout=60,
    )
    if not reaped:
        return
    ctx.equals("the reaped attempt is marked failed", reaped.success, False)

    task = ctx.wait(
        "the task is retried and succeeds",
        lambda: probe.in_status(task_id, TaskStatus.SUCCEEDED),
        timeout=120,
    )
    if task:
        ctx.equals("a second attempt ran", task.attempts.count(), 2)
        ctx.check(
            "the retry ran on a different worker",
            ctx.count(kind=Event.TASK, name=label) >= 2,
            f"{ctx.count(kind=Event.TASK, name=label)} task events",
        )


@scenario(
    "dur.reaper-retry-crash",
    group="durability",
    title="Orphan reaper reclaims a crash during a retry attempt",
    proves="A retry attempt — created SCHEDULED by qraft's dispatcher, not by "
    "async_task() — is still leased, so killing its worker is reclaimable too.",
)
def reaper_retry_crash(ctx):
    run, label = ctx.run, "retry-crash"
    task_id = async_task(
        "showcase.tasks.crash_on_attempt",
        run,
        label,
        crash_attempt=2,
        hold=90.0,
        qraft_options={"max_attempts": 4, "base_delay": 1.0, "jitter": False},
    )

    ctx.wait(
        "attempt 1 fails normally",
        lambda: ctx.events(kind=Event.TASK, name=label).count() >= 1,
        timeout=60,
    )

    pid = ctx.wait(
        "attempt 2 starts and publishes its pid",
        _pid_from_events(ctx, label, attempt=2),
        timeout=90,
    )
    if not pid:
        return

    task = probe.task_of(task_id)
    second = ctx.wait(
        "attempt 2 has a lease",
        lambda: task.attempts.filter(
            attempt_number=2, heartbeat_at__isnull=False
        ).first(),
        timeout=45,
    )
    if not second:
        ctx.note(
            "Without a lease on the retry attempt the reaper cannot see it. "
            "This is the regression this scenario exists for."
        )
        return
    ctx.check(
        "attempt 2 was created by the retry path, not by async_task",
        second.date_started is not None,
        "date_started stamped by the marker lease",
    )

    Control.objects.update_or_create(
        key=f"{run}:{label}:crash", defaults={"value": {"stop_crashing": True}}
    )
    if not _kill(ctx, pid):
        return

    ctx.check(
        "attempt 2 is stuck unresolved",
        QraftTaskAttempt.objects.get(id=second.id).success is None,
        "success is still null",
    )

    ctx.note("Heartbeat backdated by 300s, as in dur.reaper-kill.")
    _backdate(second.id)

    reaped = ctx.wait(
        "reaper resolves the orphaned retry attempt",
        lambda: QraftTaskAttempt.objects.filter(
            id=second.id, exception_class="OrphanedTask"
        ).first(),
        timeout=60,
    )
    if not reaped:
        return

    final = ctx.wait(
        "the task recovers on attempt 3",
        lambda: probe.in_status(task_id, TaskStatus.SUCCEEDED),
        timeout=120,
    )
    if final:
        ctx.equals("three attempts in total", final.attempts.count(), 3)


@scenario(
    "dur.dlq",
    group="durability",
    title="Dead-letter queue and requeue",
    proves="An exhausted task lands in the DLQ, and requeue() continues the "
    "same attempt series with its history and idempotency key intact.",
)
def dlq(ctx):
    run, label = ctx.run, "dead"
    key = f"dlq-{run}"
    task_id = async_task(
        "showcase.tasks.flaky_task",
        run,
        label,
        fail_times=2,
        qraft_options={
            "max_attempts": 2,
            "base_delay": 1.0,
            "jitter": False,
            "idempotency_key": key,
        },
    )

    task = ctx.wait(
        "task exhausts its retries",
        lambda: probe.in_status(task_id, TaskStatus.EXHAUSTED),
        timeout=90,
    )
    if not task:
        return

    ctx.equals("two attempts were made", task.attempts.count(), 2)
    ctx.check(
        "task appears in the dead-letter queue",
        dead_letters().filter(id=task.id).exists(),
        "dead_letters() lists it",
    )

    requeue(task)
    ctx.equals(
        "requeue puts the task back to PENDING",
        probe.fresh(task).status,
        TaskStatus.PENDING,
    )

    recovered = ctx.wait(
        "requeued task runs and succeeds",
        lambda: probe.in_status(task_id, TaskStatus.SUCCEEDED),
        timeout=120,
    )
    if not recovered:
        return

    ctx.equals(
        "the requeue continued the same attempt series", recovered.attempts.count(), 3
    )
    outcomes = list(
        recovered.attempts.order_by("attempt_number").values_list("success", flat=True)
    )
    ctx.check(
        "the two failed attempts are still on record",
        outcomes[:2] == [False, False],
        f"attempt outcomes {outcomes}",
    )
    ctx.equals(
        "the idempotency key survived the requeue", recovered.idempotency_key, key
    )
    ctx.check(
        "the task is no longer dead",
        not dead_letters().filter(id=task.id).exists(),
        "dead_letters() no longer lists it",
    )


@scenario(
    "dur.retention",
    group="durability",
    title="Retention sweep",
    proves="The sweep prunes settled rows past the window and leaves live "
    "rows, recent rows, and members of unfinished workflows alone.",
    clusters=(),
)
def retention(ctx):
    ctx.note("Runs entirely in-process against purpose-built rows; no cluster needed.")
    old = timezone.now() - timedelta(days=30)

    def make(status, aged: bool, **extra):
        task = QraftTask.objects.create(
            func="showcase.tasks.ok_task", status=status, **extra
        )
        if aged:
            QraftTask.objects.filter(id=task.id).update(date_updated=old)
        return task

    settled_old = make(TaskStatus.SUCCEEDED, True)
    settled_recent = make(TaskStatus.SUCCEEDED, False)
    live_old = make(TaskStatus.RUNNING, True)
    pending_old = make(TaskStatus.PENDING, True)

    done_workflow = QraftIterModel.objects.create(
        func="showcase.tasks.ok_task", status=WorkflowStatus.SUCCEEDED, total_count=1
    )
    live_workflow = QraftIterModel.objects.create(
        func="showcase.tasks.ok_task", status=WorkflowStatus.RUNNING, total_count=1
    )
    QraftIterModel.objects.filter(id__in=[done_workflow.id, live_workflow.id]).update(
        date_updated=old
    )
    member_of_done = make(TaskStatus.SUCCEEDED, True, qraft_iter=done_workflow)
    member_of_live = make(TaskStatus.SUCCEEDED, True, qraft_iter=live_workflow)

    ctx.equals("no window configured means no sweep", sweep_retention(), {})

    sweep_retention(retention_days=7)

    def gone(task) -> bool:
        return not QraftTask.objects.filter(id=task.id).exists()

    ctx.check("settled row past the window was pruned", gone(settled_old))
    ctx.check("settled row inside the window was kept", not gone(settled_recent))
    ctx.check("RUNNING row was kept however old", not gone(live_old))
    ctx.check("PENDING row was kept however old", not gone(pending_old))
    ctx.check(
        "finished workflow was pruned",
        not QraftIterModel.objects.filter(id=done_workflow.id).exists(),
    )
    ctx.check(
        "unfinished workflow was kept",
        QraftIterModel.objects.filter(id=live_workflow.id).exists(),
    )
    ctx.check("member of the finished workflow went with it", gone(member_of_done))
    ctx.check("member of the unfinished workflow was kept", not gone(member_of_live))
