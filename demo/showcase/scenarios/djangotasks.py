"""
django.tasks (DEP 14) scenarios.

These only became reachable when the demo moved to Django 6.0. They exercise
`qraft.backend.QraftTaskBackend` through Django's own Tasks API, not through
`qraft.tasks.async_task()`.
"""

from datetime import timedelta

from django.tasks import task_backends
from django.tasks.base import DEFAULT_TASK_PRIORITY, TaskResultStatus
from django.tasks.exceptions import InvalidTask, TaskResultDoesNotExist
from django.utils import timezone
from django_q.conf import Conf

from qraft.backend import QraftTaskBackend
from qraft.brokers import _lanes_drained_by
from showcase.dtasks import always_fails, deferred, summarize, with_context
from showcase.harness import scenario
from showcase.models import Event

PLAIN_BROKER = "django_q.brokers.orm.ORM"


def _backend():
    return task_backends["default"]


def _result(result_id):
    return _backend().get_result(result_id)


def _finished(result_id):
    """Poll helper: the TaskResult once the backend calls it finished."""

    def look():
        result = _result(result_id)
        return result if result.is_finished else None

    return look


@scenario(
    "dt.enqueue",
    group="django-tasks",
    title="Enqueue and result retrieval through QraftTaskBackend",
    proves="A @task enqueued by Django's Tasks API runs on a qraft cluster, "
    "and the backend reports its status and return value.",
)
def enqueue(ctx):
    run = ctx.run
    ctx.check(
        "the configured backend is QraftTaskBackend",
        isinstance(_backend(), QraftTaskBackend),
        type(_backend()).__name__,
    )
    ctx.check(
        "the backend advertises result retrieval",
        _backend().supports_get_result,
        "supports_get_result",
    )

    result = summarize.enqueue(run, "dt-ok", "Mockco filed a quarterly report")
    ctx.check("enqueue returned a TaskResult with an id", bool(result.id), result.id)
    ctx.check(
        "a freshly enqueued task is READY or RUNNING",
        result.status in (TaskResultStatus.READY, TaskResultStatus.RUNNING),
        str(result.status),
    )

    final = ctx.wait("task reaches a finished status", _finished(result.id), timeout=90)
    if not final:
        return

    ctx.equals("status maps to SUCCESSFUL", final.status, TaskResultStatus.SUCCESSFUL)
    ctx.equals("the task really ran", ctx.count(kind=Event.TASK, name="dt-ok"), 1)
    ctx.equals("the return value came back", final.return_value.get("words"), 5)
    ctx.equals(
        "args round-tripped",
        list(final.args),
        [run, "dt-ok", "Mockco filed a quarterly report"],
    )
    ctx.check(
        "at least one worker id is recorded",
        len(final.worker_ids) >= 1,
        str(final.worker_ids),
    )

    # refresh() on the caller's own object must pick the same state up.
    result.refresh()
    ctx.equals(
        "refresh() updates the caller's result",
        result.status,
        TaskResultStatus.SUCCESSFUL,
    )
    ctx.equals(
        "Task.get_result() finds it too",
        summarize.get_result(result.id).status,
        TaskResultStatus.SUCCESSFUL,
    )

    broken = always_fails.enqueue(run, "dt-fail")
    failed = ctx.wait(
        "failing task reaches a finished status", _finished(broken.id), timeout=90
    )
    if failed:
        ctx.equals("status maps to FAILED", failed.status, TaskResultStatus.FAILED)
        ctx.check(
            "the failure is reported with an error",
            bool(failed.errors),
            f"errors={[e.exception_class_path for e in failed.errors]}",
        )

    try:
        _result("00000000-0000-0000-0000-000000000000")
        ctx.check("an unknown id raises TaskResultDoesNotExist", False, "no exception")
    except TaskResultDoesNotExist:
        ctx.check("an unknown id raises TaskResultDoesNotExist", True)


@scenario(
    "dt.defer",
    group="django-tasks",
    title="run_after deferred execution",
    proves="A task enqueued with run_after stays unrun until its due time "
    "and then executes.",
)
def defer(ctx):
    run, delay = ctx.run, 8
    ctx.check(
        "the backend advertises deferral", _backend().supports_defer, "supports_defer"
    )

    due = timezone.now() + timedelta(seconds=delay)
    result = deferred.using(run_after=due).enqueue(run, "dt-defer")
    ctx.equals("a deferred task starts READY", result.status, TaskResultStatus.READY)

    ctx.settle(delay / 2)
    ctx.equals(
        "it had not run at the halfway point",
        ctx.count(kind=Event.TASK, name="dt-defer"),
        0,
    )

    ran = ctx.wait(
        "it runs once due",
        lambda: ctx.events(kind=Event.TASK, name="dt-defer").first(),
        timeout=90,
    )
    if not ran:
        return

    ctx.check(
        "it ran no earlier than its due time",
        ran.created_at >= due - timedelta(seconds=1),
        f"ran at {ran.created_at.isoformat()}, due {due.isoformat()}",
    )

    final = ctx.wait("the deferred result finishes", _finished(result.id), timeout=60)
    if final:
        ctx.equals("deferred task succeeded", final.status, TaskResultStatus.SUCCESSFUL)


@scenario(
    "dt.context",
    group="django-tasks",
    title="takes_context=True receives a real TaskContext",
    proves="A context-taking task is handed a TaskContext whose TaskResult "
    "is its own, not a placeholder.",
)
def context(ctx):
    run = ctx.run
    result = with_context.enqueue(run, "dt-ctx")

    final = ctx.wait("context task finishes", _finished(result.id), timeout=90)
    if not final:
        return
    ctx.equals("context task succeeded", final.status, TaskResultStatus.SUCCESSFUL)

    event = ctx.events(kind=Event.TASK, name="dt-ctx").first()
    if not ctx.check("the task recorded what it saw", event is not None):
        return

    ctx.equals(
        "the context pointed at this task's own result",
        event.payload.get("result_id"),
        str(result.id),
    )
    ctx.equals(
        "the context reported the running attempt", event.payload.get("attempt"), 1
    )
    ctx.check(
        "the context carried a status",
        bool(event.payload.get("status")),
        event.payload.get("status"),
    )


@scenario(
    "dt.priority",
    group="django-tasks",
    title="supports_priority reports honestly",
    proves="supports_priority follows the deployed broker rather than being "
    "a fixed claim, and Django rejects a priority enqueue when it is false.",
    clusters=(),
)
def priority(ctx):
    ctx.note("In-process capability check; no task is executed.")
    descriptor = QraftTaskBackend.__dict__.get("supports_priority")
    ctx.check(
        "supports_priority is a property, not a fixed attribute",
        isinstance(descriptor, property),
        type(descriptor).__name__,
    )

    original = Conf.BROKER_CLASS
    try:
        Conf.BROKER_CLASS = "qraft.brokers.QraftOrmBroker"
        _lanes_drained_by.cache_clear()
        ctx.check(
            "true when the broker drains the lanes",
            _backend().supports_priority,
            "QraftOrmBroker",
        )

        Conf.BROKER_CLASS = PLAIN_BROKER
        _lanes_drained_by.cache_clear()
        ctx.check(
            "false when the broker cannot",
            not _backend().supports_priority,
            PLAIN_BROKER,
        )

        try:
            summarize.using(priority=10).enqueue("unused", "unused", "unused")
            ctx.check(
                "Django rejects a priority enqueue it cannot honour",
                False,
                "enqueue was accepted",
            )
        except InvalidTask as error:
            ctx.check(
                "Django rejects a priority enqueue it cannot honour", True, str(error)
            )
    finally:
        Conf.BROKER_CLASS = original
        _lanes_drained_by.cache_clear()

    ctx.check(
        "default priority is still accepted",
        summarize.priority == DEFAULT_TASK_PRIORITY,
        f"priority={summarize.priority}",
    )
