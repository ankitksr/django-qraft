"""AI-workload scenarios: idempotency, usage accounting, throttling, priority lanes."""

import logging
import time

from django_q.conf import Conf

from qraft.brokers import _lanes_drained_by, priority_lanes_available
from qraft.context import aggregate_usage, aggregate_workflow_usage
from qraft.iter import QraftIter
from qraft.models import (
    QraftIterModel,
    QraftTask,
    RateBucket,
    TaskStatus,
    WorkflowStatus,
)
from qraft.tasks import async_task
from showcase import probe
from showcase import tasks as demo_tasks
from showcase.harness import scenario
from showcase.models import Event

PLAIN_BROKER = "django_q.brokers.orm.ORM"
LANE_BROKER = "qraft.brokers.QraftOrmBroker"


class _Capture(logging.Handler):
    """Collects qraft's warnings so a scenario can assert one was emitted."""

    def __init__(self):
        super().__init__(level=logging.WARNING)
        self.messages: list[str] = []

    def emit(self, record):
        self.messages.append(record.getMessage())


@scenario(
    "ai.idempotency",
    group="ai",
    title="Idempotency keys",
    proves="A repeat enqueue under a live key returns the original id and "
    "runs nothing new; idempotency_retry_dead releases a dead key.",
)
def idempotency(ctx):
    run = ctx.run
    key = f"charge-{run}"

    first = async_task(
        "showcase.tasks.ok_task", run, "charge", qraft_options={"idempotency_key": key}
    )
    second = async_task(
        "showcase.tasks.ok_task", run, "charge", qraft_options={"idempotency_key": key}
    )
    ctx.equals("repeat enqueue returns the original id", second, first)
    ctx.equals(
        "only one task holds the key",
        QraftTask.objects.filter(idempotency_key=key).count(),
        1,
    )

    task = ctx.wait("the single task runs", lambda: probe.settled(first), timeout=60)
    if task:
        ctx.equals("task succeeded", task.status, TaskStatus.SUCCEEDED)
    ctx.settle(2)
    ctx.equals("the side effect happened exactly once", ctx.count(name="charge"), 1)

    dead_key = f"dead-{run}"
    dead = async_task(
        "showcase.tasks.fail_task",
        run,
        "dead-key",
        qraft_options={"max_attempts": 1, "idempotency_key": dead_key},
    )
    dead_task = ctx.wait(
        "a task under a second key dies",
        lambda: probe.in_status(dead, TaskStatus.EXHAUSTED),
        timeout=90,
    )
    if not dead_task:
        return

    blocked = async_task(
        "showcase.tasks.fail_task",
        run,
        "dead-key",
        qraft_options={"max_attempts": 1, "idempotency_key": dead_key},
    )
    ctx.equals("a dead key still blocks a plain re-enqueue", blocked, dead)

    reclaimed = async_task(
        "showcase.tasks.ok_task",
        run,
        "reclaimed",
        qraft_options={"idempotency_key": dead_key, "idempotency_retry_dead": True},
    )
    ctx.check(
        "idempotency_retry_dead re-enqueues under the same key",
        reclaimed != dead,
        f"new id {reclaimed}",
    )

    fresh_task = ctx.wait(
        "the reclaimed task runs", lambda: probe.settled(reclaimed), timeout=60
    )
    if fresh_task:
        ctx.equals(
            "the reclaimed task now holds the key", fresh_task.idempotency_key, dead_key
        )
        ctx.check(
            "the dead task released the key",
            probe.fresh(dead_task).idempotency_key is None,
            "old task's key is now null",
        )


@scenario(
    "ai.usage",
    group="ai",
    title="Token and cost accounting",
    proves="record_usage accumulates per attempt, and the aggregates roll it "
    "up across attempts and across a whole workflow.",
)
def usage(ctx):
    run, calls = ctx.run, 3
    task_id = async_task("showcase.tasks.llm_task", run, "usage", calls=calls)
    task = ctx.wait("usage task settles", lambda: probe.settled(task_id), timeout=60)
    if not task:
        return

    attempt = probe.attempt_of(task_id)
    recorded = attempt.usage or {}
    ctx.equals(
        "input tokens accumulated across calls",
        recorded.get("input_tokens"),
        100 * calls,
    )
    ctx.equals(
        "output tokens accumulated across calls",
        recorded.get("output_tokens"),
        25 * calls,
    )
    ctx.equals(
        "cost accumulated across calls",
        round(recorded.get("cost_usd", 0), 6),
        round(0.002 * calls, 6),
    )
    ctx.equals(
        "the model name is the latest value, not a sum",
        recorded.get("model"),
        "mock-llm-v1",
    )

    rolled = aggregate_usage(probe.fresh(task))
    ctx.equals(
        "aggregate_usage matches the single attempt",
        rolled.get("input_tokens"),
        100 * calls,
    )

    workflow = QraftIter("showcase.tasks.llm_task")
    for index in range(3):
        workflow.append(run, f"wf-usage-{index}", calls=calls)
    iter_id = workflow.run()

    row = ctx.wait(
        "usage workflow completes",
        lambda: (
            QraftIterModel.objects.filter(
                id=iter_id,
                status__in=[WorkflowStatus.SUCCEEDED, WorkflowStatus.FAILED],
            ).first()
        ),
        timeout=120,
    )
    if not row:
        return

    total = aggregate_workflow_usage(row)
    ctx.equals(
        "aggregate_workflow_usage sums every member",
        total.get("input_tokens"),
        100 * calls * 3,
    )
    ctx.equals(
        "workflow cost sums every member",
        round(total.get("cost_usd", 0), 6),
        round(0.002 * calls * 3, 6),
    )


@scenario(
    "ai.progress",
    group="ai",
    title="Task progress reporting",
    proves="report_progress() moves while the task runs and settles on the "
    "final step, so a watcher sees real progress.",
)
def progress(ctx):
    run, steps = ctx.run, 5
    task_id = async_task(
        "showcase.tasks.progress_task", run, "prog", steps=steps, delay=0.6
    )

    seen = set()
    deadline = time.monotonic() + 40
    while time.monotonic() < deadline:
        task = probe.task_of(task_id)
        if task and task.progress:
            seen.add(task.progress.get("current"))
        if task and task.status in probe.TERMINAL:
            break
        time.sleep(0.25)

    ctx.check(
        "progress advanced through several values",
        len(seen) >= 3,
        f"observed current={sorted(v for v in seen if v is not None)}",
    )

    task = ctx.wait("progress task settles", lambda: probe.settled(task_id), timeout=45)
    if not task:
        return
    final = probe.fresh(task).progress or {}
    ctx.equals("final progress reached the last step", final.get("current"), steps)
    ctx.equals("progress carried the total", final.get("total"), steps)
    ctx.check(
        "progress carried a message",
        bool(final.get("message")),
        f"message={final.get('message')!r}",
    )


@scenario(
    "ai.throttle",
    group="ai",
    title="Shared rate bucket across two clusters",
    proves="Two clusters drawing on one RateBucket cannot together exceed "
    "the bucket's capacity plus its refill over the observed window.",
    clusters=("throttle-a", "throttle-b"),
)
def throttle(ctx):
    run = ctx.run
    rate, capacity = demo_tasks.THROTTLE_RATE, demo_tasks.THROTTLE_CAPACITY
    RateBucket.objects.filter(key=demo_tasks.THROTTLE_KEY).delete()

    # max_attempts=1 so a denied task simply dies. Every success is then one
    # token the bucket actually handed out.
    total = 24
    ids = []
    for index in range(total):
        cluster = "throttle-a" if index % 2 == 0 else "throttle-b"
        ids.append(
            async_task(
                "showcase.tasks.throttled_task",
                run,
                f"th-{index}",
                qraft_options={"max_attempts": 1, "cluster": cluster},
            )
        )

    done = ctx.wait(
        "every throttled task settles",
        lambda: all(probe.settled(task_id) for task_id in ids) or None,
        timeout=180,
    )
    if not done:
        return

    granted = list(
        ctx.events(kind=Event.TASK)
        .order_by("created_at")
        .values_list("created_at", "payload")
    )
    successes = len(granted)
    denied = total - successes

    ctx.check(
        "the bucket denied some tasks",
        denied > 0,
        f"{denied} of {total} tasks were rate limited",
    )
    ctx.check(
        "the bucket granted some tasks",
        successes > 0,
        f"{successes} of {total} tasks got a token",
    )

    window = (granted[-1][0] - granted[0][0]).total_seconds() if successes > 1 else 0.0
    allowed = capacity + rate * window
    ctx.check(
        "combined grants stay within one bucket's budget",
        successes <= allowed + 1,
        f"{successes} grants over {window:.1f}s; budget is "
        f"{capacity:g} + {rate:g}/s x {window:.1f}s = {allowed:.1f}",
    )

    per_cluster: dict[str, int] = {}
    for _, payload in granted:
        name = payload.get("cluster") or "unknown"
        per_cluster[name] = per_cluster.get(name, 0) + 1
    ctx.note(
        f"{total} tasks were split evenly between throttle-a and throttle-b; "
        f"grants landed as {per_cluster}."
    )


@scenario(
    "ai.priority",
    group="ai",
    title="Priority lanes, and the misconfigured fallback",
    proves="High drains before default before low; a cluster whose broker "
    "cannot drain the lanes warns and still runs the task.",
    clusters=(),
    manual_clusters=("lanes",),
)
def priority(ctx):
    run, per_lane = ctx.run, 4

    ctx.check(
        "the enqueuing process can route priority lanes",
        priority_lanes_available(),
        f"broker_class={Conf.BROKER_CLASS}",
    )

    # Fill every lane before a consumer exists, worst order first, so drain
    # order is the broker's decision and not an artefact of arrival time.
    for lane, prefix in (("low", "lo"), ("default", "de"), ("high", "hi")):
        for index in range(per_lane):
            async_task(
                "showcase.tasks.ok_task",
                run,
                f"{prefix}-{index}",
                qraft_options={"priority": lane, "cluster": "lanes"},
            )

    ctx.clusters.start("lanes")

    ctx.wait(
        "all lanes drain",
        lambda: ctx.count(kind=Event.TASK) >= per_lane * 3 or None,
        timeout=180,
    )
    order = ctx.names(kind=Event.TASK)
    positions = {
        prefix: [index for index, name in enumerate(order) if name.startswith(prefix)]
        for prefix in ("hi", "de", "lo")
    }
    ctx.equals("high lane fully drained", len(positions["hi"]), per_lane)
    ctx.equals("default lane fully drained", len(positions["de"]), per_lane)
    ctx.equals("low lane fully drained", len(positions["lo"]), per_lane)

    if all(positions.values()):
        ctx.check(
            "high drained before default",
            max(positions["hi"]) < min(positions["de"]),
            f"high at {positions['hi']}, default at {positions['de']}",
        )
        ctx.check(
            "default drained before low",
            max(positions["de"]) < min(positions["lo"]),
            f"default at {positions['de']}, low at {positions['lo']}",
        )

    # Misconfiguration: a cluster whose broker_class does not drain the
    # suffixed lanes. qraft must warn and fall back, never strand the task.
    capture = _Capture()
    logger = logging.getLogger("qraft")
    logger.addHandler(capture)
    original = Conf.BROKER_CLASS
    try:
        Conf.BROKER_CLASS = PLAIN_BROKER
        _lanes_drained_by.cache_clear()
        ctx.check(
            "priority lanes report unavailable on a plain broker",
            not priority_lanes_available(),
            f"broker_class={PLAIN_BROKER}",
        )
        stranded = async_task(
            "showcase.tasks.ok_task",
            run,
            "fallback",
            qraft_options={"priority": "high", "cluster": "lanes"},
        )
    finally:
        Conf.BROKER_CLASS = original
        _lanes_drained_by.cache_clear()
        logger.removeHandler(capture)

    ctx.check(
        "qraft warned about the misconfiguration",
        any("does not drain Qraft priority" in message for message in capture.messages),
        f"{len(capture.messages)} warning(s) captured",
    )

    fell_back = ctx.wait(
        "the misrouted task still ran", lambda: probe.settled(stranded), timeout=90
    )
    if fell_back:
        ctx.equals(
            "it ran instead of stranding", fell_back.status, TaskStatus.SUCCEEDED
        )
        ctx.equals(
            "its requested priority is still on record", fell_back.priority, "high"
        )

    ctx.clusters.stop("lanes")
