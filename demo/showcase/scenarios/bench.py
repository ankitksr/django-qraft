"""
Benchmarks: qraft against plain django-q2, on the same cluster and database.

Same workers, same broker, same PostgreSQL — the only variable is which layer
enqueues and settles the task. Numbers land in the notes; checks only assert
what must always hold, so a slow laptop cannot fail the suite.
"""

import statistics
import time
from datetime import timedelta

from django.utils import timezone
from django_q.models import Schedule, Success
from django_q.tasks import async_task as q2_async_task

from qraft.models import QraftTaskAttempt, TaskStatus
from qraft.tasks import async_task
from showcase import probe
from showcase.harness import scenario
from showcase.models import Event

# Ceiling on qraft's delivery overhead for a scheduled attempt: dispatcher
# poll (<= 0.5s) plus broker pickup. Matches core.DELIVERY_SLACK.
DELIVERY_SLACK = 3.0


@scenario(
    "bench.delay",
    group="bench",
    title="Delay precision: qraft dispatcher vs q2 Schedule",
    proves="A 2-second delay through qraft's own scheduler is served in about "
    "2 seconds, while the same delay through a django-q2 Schedule waits for "
    "the ~30-second scheduler tick.",
)
def delay(ctx):
    run = ctx.run
    chosen = 2.0

    # qraft path: a retry is the delayed attempt in its natural habitat.
    qraft_id = async_task(
        "showcase.tasks.flaky_task",
        run,
        "qr-delay",
        fail_times=1,
        qraft_options={
            "max_attempts": 2,
            "base_delay": chosen,
            "backoff_strategy": "fixed",
            "jitter": False,
        },
    )

    # q2 path: the same 2-second ask, written the only way q2 offers.
    eta = timezone.now() + timedelta(seconds=chosen)
    Schedule.objects.create(
        name=f"{run}-q2-delay",
        func="showcase.tasks.ok_task",
        args=repr((run, "q2-delay")),
        schedule_type=Schedule.ONCE,
        next_run=eta,
    )

    task = ctx.wait(
        "qraft: task settles",
        lambda: probe.in_status(qraft_id, TaskStatus.SUCCEEDED),
        timeout=60,
    )
    if task:
        served = probe.gaps(task)
        if ctx.check("qraft: retry delay measured", bool(served)):
            ctx.between(
                "qraft: 2s chosen, served in", served[0], 1.5, chosen + DELIVERY_SLACK
            )
            ctx.note(f"qraft served the 2.0s delay in {served[0]:.1f}s")

    fired = ctx.wait(
        "q2: Schedule fires at all",
        lambda: ctx.events(kind=Event.TASK, name="q2-delay").first(),
        timeout=120,
        detail="the Q2 scheduler tick is ~30s",
    )
    if fired:
        q2_served = (fired.created_at - eta).total_seconds() + chosen
        ctx.check(
            "q2: served delay measured",
            q2_served >= 0,
            f"{q2_served:.1f}s",
        )
        ctx.note(
            f"q2 served the same 2.0s ask in {q2_served:.1f}s — "
            "whatever the ask, delivery waits for the next scheduler tick"
        )


@scenario(
    "bench.throughput",
    group="bench",
    title="Throughput: qraft overhead over raw q2",
    proves="Qraft's extra bookkeeping (task + attempt rows, lease, status "
    "updates) costs a bounded, small factor over raw django-q2 on the same "
    "workers.",
)
def throughput(ctx):
    run = ctx.run
    n = 40

    # Raw q2 first, so qraft's rows cannot slow the q2 pass's table scans.
    started = time.monotonic()
    for index in range(1, n + 1):
        q2_async_task(
            "showcase.tasks.ok_task", run, f"q2-{index:02d}", group=f"{run}-q2"
        )
    q2_enqueue = time.monotonic() - started
    done = ctx.wait(
        "q2: all executed",
        lambda: Success.objects.filter(group=f"{run}-q2").count() >= n,
        timeout=120,
    )
    q2_wall = time.monotonic() - started

    started = time.monotonic()
    ids = [
        async_task("showcase.tasks.ok_task", run, f"qr-{index:02d}")
        for index in range(1, n + 1)
    ]
    qraft_enqueue = time.monotonic() - started
    executed = ctx.wait(
        "qraft: all executed",
        lambda: ctx.events(kind=Event.TASK).filter(name__startswith="qr-").count() >= n,
        timeout=120,
    )
    qraft_wall = time.monotonic() - started
    ctx.wait(
        "qraft: all settled",
        lambda: QraftTaskAttempt.objects.filter(
            q2_task_id__in=ids, success=True
        ).count()
        >= n,
        timeout=60,
        detail="status bookkeeping after execution",
    )
    settled_wall = time.monotonic() - started

    if not (done and executed):
        return

    ctx.note(
        f"enqueue: q2 {q2_enqueue / n * 1000:.1f} ms/task, "
        f"qraft {qraft_enqueue / n * 1000:.1f} ms/task "
        f"(qraft writes the task + attempt rows up front)"
    )
    ctx.note(
        f"execute {n}: q2 {q2_wall:.1f}s ({n / q2_wall:.0f}/s), "
        f"qraft {qraft_wall:.1f}s ({n / qraft_wall:.0f}/s), "
        f"qraft settled in {settled_wall:.1f}s"
    )
    ctx.check(
        "qraft overhead bounded (< 5x raw q2)",
        qraft_wall < 5 * q2_wall,
        f"q2 {q2_wall:.1f}s vs qraft {qraft_wall:.1f}s",
    )


@scenario(
    "bench.pickup",
    group="bench",
    title="Pickup latency: enqueue to first execution",
    proves="An enqueued task starts executing quickly; the p95 gap between "
    "enqueue and a worker picking the task up stays in low seconds.",
)
def pickup(ctx):
    run = ctx.run
    n = 20

    ids = [
        async_task("showcase.tasks.ok_task", run, f"pk-{index:02d}")
        for index in range(1, n + 1)
    ]
    settled = ctx.wait(
        "all tasks settle",
        lambda: QraftTaskAttempt.objects.filter(
            q2_task_id__in=ids, success=True
        ).count()
        >= n,
        timeout=120,
    )
    if not settled:
        return

    attempts = QraftTaskAttempt.objects.filter(q2_task_id__in=ids).select_related(
        "qraft_task"
    )
    latencies = sorted(
        (attempt.date_started - attempt.qraft_task.date_created).total_seconds()
        for attempt in attempts
        if attempt.date_started
    )
    ctx.equals("every attempt recorded a start time", len(latencies), n)
    if not latencies:
        return

    p50 = statistics.median(latencies)
    p95 = latencies[max(0, int(len(latencies) * 0.95) - 1)]
    ctx.note(f"pickup latency over {n} tasks: p50 {p50:.2f}s, p95 {p95:.2f}s")
    ctx.check("p95 pickup under 5s", p95 < 5.0, f"p95 {p95:.2f}s")
