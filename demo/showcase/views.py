"""
Dashboard for watching a qraft run.

Django templates and plain views, polled by inline JavaScript. Nothing is
fetched from the network, so the page works offline. Deep links point at the
Django admin, which `qraft.admin` already furnishes, rather than rebuilding
per-object detail pages here.
"""

import threading
import uuid

from django.db import connection
from django.db.models import Count
from django.http import Http404, JsonResponse
from django.shortcuts import render
from django.utils import timezone
from django.views.decorators.http import require_POST
from django_q.models import OrmQ, Schedule

from qraft.dlq import dead_letters, requeue
from qraft.models import (
    QraftBatchModel,
    QraftChainModel,
    QraftIterModel,
    QraftTask,
    QraftTaskAttempt,
    RateBucket,
    TaskStatus,
)
from showcase import runner
from showcase.clusters import PROFILES, ClusterManager
from showcase.models import Event, ScenarioRun

# Set by `manage.py demo serve`. A plain `runserver` gets one on first use.
CLUSTERS: ClusterManager | None = None
_LOCK = threading.Lock()
_ACTIVE: set[str] = set()

TASK_LIMIT = 60
EVENT_LIMIT = 40


def _clusters() -> ClusterManager:
    global CLUSTERS
    with _LOCK:
        if CLUSTERS is None:
            CLUSTERS = ClusterManager()
        return CLUSTERS


def _short(value) -> str:
    return str(value)[:8]


def _func(path: str) -> str:
    return path.rsplit(".", 1)[-1] if path else ""


def dashboard(request):
    runner.load()
    return render(
        request,
        "showcase/dashboard.html",
        {
            "scenarios": runner.ordered_scenarios(),
            "profiles": PROFILES,
        },
    )


def state(request):
    """Everything the dashboard polls for, in one round trip."""
    now = timezone.now()

    tasks = []
    recent = (
        QraftTask.objects.order_by("-date_created")
        .prefetch_related("attempts")
        .select_related("qraft_iter", "qraft_batch")[:TASK_LIMIT]
    )
    for task in recent:
        attempts = sorted(task.attempts.all(), key=lambda a: a.attempt_number)
        latest = attempts[-1] if attempts else None
        if latest and latest.date_started and latest.success is None:
            elapsed = (now - latest.date_started).total_seconds()
        elif latest and latest.date_completed:
            elapsed = (latest.date_completed - task.date_created).total_seconds()
        else:
            elapsed = (now - task.date_created).total_seconds()

        progress = task.progress or {}
        tasks.append(
            {
                "id": str(task.id),
                "short": _short(task.id),
                "func": _func(task.func),
                "status": task.status,
                "priority": task.priority,
                "attempts": len(attempts),
                "max_attempts": (task.retry_policy or {}).get("max_attempts"),
                "elapsed": round(elapsed, 1),
                "error": latest.exception_class if latest else None,
                "heartbeat": (
                    round((now - latest.heartbeat_at).total_seconds(), 1)
                    if latest and latest.heartbeat_at and latest.success is None
                    else None
                ),
                "progress": (
                    f"{progress.get('current')}/{progress.get('total')}"
                    if progress.get("total")
                    else ""
                ),
                "message": progress.get("message", ""),
                "workflow": (
                    "iter"
                    if task.qraft_iter_id
                    else "batch"
                    if task.qraft_batch_id
                    else ""
                ),
            }
        )

    workflows = []
    for model, kind in (
        (QraftChainModel, "chain"),
        (QraftIterModel, "iter"),
        (QraftBatchModel, "batch"),
    ):
        for row in model.objects.order_by("-date_created")[:12]:
            if kind == "chain":
                steps = list(
                    row.steps.select_related("qraft_task").order_by("step_index")
                )
                members = [
                    {
                        "label": f"{step.step_index}. {_func(step.func)}",
                        "status": (
                            step.qraft_task.status
                            if step.qraft_task_id
                            else "not queued"
                        ),
                        "gated": step.requires_approval,
                    }
                    for step in steps
                ]
                done = sum(
                    1
                    for step in steps
                    if step.qraft_task_id
                    and step.qraft_task.status == TaskStatus.SUCCEEDED
                )
                counters = {
                    "completed": done,
                    "total": len(steps),
                    "success": done,
                    "failure": 0,
                }
            else:
                members = [
                    {"label": _func(task.func), "status": task.status, "gated": False}
                    for task in row.tasks.all()[:12]
                ]
                counters = {
                    "completed": row.completed_count,
                    "total": row.total_count,
                    "success": row.success_count,
                    "failure": row.failure_count,
                }
            workflows.append(
                {
                    "id": str(row.id),
                    "short": _short(row.id),
                    "kind": kind,
                    "status": row.status,
                    "counters": counters,
                    "members": members,
                }
            )

    usage: dict[str, float] = {}
    for values in QraftTaskAttempt.objects.exclude(usage=None).values_list(
        "usage", flat=True
    )[:500]:
        for key, value in (values or {}).items():
            if isinstance(value, (int, float)) and not isinstance(value, bool):
                usage[key] = round(usage.get(key, 0) + value, 6)

    counts = {status: 0 for status, _ in TaskStatus.choices}
    for row in QraftTask.objects.values("status").annotate(n=Count("id")):
        counts[row["status"]] = row["n"]

    return JsonResponse(
        {
            "now": now.isoformat(),
            "counts": counts,
            "queued": OrmQ.objects.count(),
            "scheduled": Schedule.objects.count(),
            "clusters": sorted(_clusters().running()),
            "tasks": tasks,
            "workflows": workflows,
            "dlq": [
                {
                    "id": str(task.id),
                    "short": _short(task.id),
                    "func": _func(task.func),
                    "status": task.status,
                    "attempts": task.attempts.count(),
                    "key": task.idempotency_key or "",
                }
                for task in dead_letters()[:20]
            ],
            "buckets": [
                {"key": bucket.key, "tokens": round(bucket.tokens, 2)}
                for bucket in RateBucket.objects.order_by("key")
            ],
            "usage": usage,
            "events": [
                {
                    "kind": event.kind,
                    "name": event.name,
                    "at": event.created_at.strftime("%H:%M:%S"),
                }
                for event in Event.objects.order_by("-created_at", "-id")[:EVENT_LIMIT]
            ],
            "runs": [
                {
                    "id": str(row.id),
                    "key": row.key,
                    "group": row.group,
                    "title": row.title,
                    "status": row.status,
                    "passed": row.passed_checks,
                    "total": len(row.checks),
                    "duration": round((row.duration_ms or 0) / 1000, 1),
                    "checks": row.checks,
                    "notes": row.notes,
                    "error": row.error.strip().splitlines()[-1] if row.error else "",
                }
                for row in ScenarioRun.objects.all()[:25]
            ],
            "active": sorted(_ACTIVE),
        }
    )


@require_POST
def run_scenario(request, key: str):
    """Start one scenario in the background so the page can watch it."""
    runner.load()
    scenarios = [item for item in runner.ordered_scenarios() if item.key == key]
    if not scenarios:
        raise Http404(key)
    item = scenarios[0]

    with _LOCK:
        if key in _ACTIVE:
            return JsonResponse({"started": False, "reason": "already running"})
        _ACTIVE.add(key)

    manager = _clusters()

    def work():
        try:
            boot = [name for name in item.clusters if name not in item.manual_clusters]
            manager.ensure(boot)
            for name in item.manual_clusters:
                manager.stop(name)
            runner.run_one(item, manager, log=lambda message: None)
        except Exception as error:  # surfaced through the run row below
            ScenarioRun.objects.create(
                id=uuid.uuid4(),
                key=item.key,
                group=item.group,
                title=item.title,
                status=ScenarioRun.ERROR,
                error=str(error),
                finished_at=timezone.now(),
            )
        finally:
            with _LOCK:
                _ACTIVE.discard(key)
            connection.close()

    threading.Thread(target=work, daemon=True).start()
    return JsonResponse({"started": True})


@require_POST
def requeue_task(request, task_id: str):
    """Send a dead task back through the queue from the dashboard."""
    try:
        task = QraftTask.objects.get(id=task_id)
    except (QraftTask.DoesNotExist, ValueError) as error:
        raise Http404(task_id) from error
    requeue(task)
    return JsonResponse({"requeued": str(task.id)})
