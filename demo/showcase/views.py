"""
Dashboard for watching a qraft run.

Django templates and plain views, polled by inline JavaScript. Nothing is
fetched from the network, so the page works offline. Deep links point at the
Django admin, which `qraft.admin` already furnishes, rather than rebuilding
per-object detail pages here.
"""

import json
import random
import threading
import uuid
from datetime import timedelta

from django.db import connection
from django.db.models import Count, Prefetch, Q
from django.http import Http404, HttpResponseBadRequest, JsonResponse
from django.shortcuts import render
from django.utils import timezone
from django.views.decorators.http import require_POST
from django_q.models import OrmQ, Schedule

from qraft.dlq import dead_letters, requeue
from qraft.models import (
    QraftBatchModel,
    QraftChainModel,
    QraftChainStep,
    QraftIterModel,
    QraftTask,
    QraftTaskAttempt,
    RateBucket,
    TaskStatus,
)
from showcase import runner
from showcase.clusters import PROFILES, ClusterManager, profile_summaries
from showcase.models import Event, ScenarioRun
from showcase.reset import reset_state

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


def _worker_label(attempt) -> str:
    """`cluster · pid` for an attempt's worker, plus `· t<n>` when it ran on
    a threaded worker's pool thread (worker_thread is a name like
    "qraft_worker_2" - see qraft.lease.stamp_start)."""
    if attempt is None or not attempt.worker_pid:
        return ""
    label = f"{attempt.cluster or '?'} · {attempt.worker_pid}"
    if attempt.worker_thread:
        label += f" · t{attempt.worker_thread.rsplit('_', 1)[-1]}"
    return label


# Recent-window for the Workers panel: wide enough to keep a just-finished
# worker visible for a bit, narrow enough that a cluster stopped a while ago
# drops off on its own instead of needing an explicit liveness check.
WORKER_WINDOW_SECONDS = 600
THROUGHPUT_WINDOW_SECONDS = 60


def _workers_panel(now):
    """
    Worker rows derived entirely from QraftTaskAttempt rows in the recent
    window - no new infra, no process introspection.

    Grouped by the attempt's own `cluster` (the routing target set at
    creation/schedule time - see qraft.tasks.async_task and
    qraft.scheduler.schedule_attempt - which is also the cluster that
    actually ran it, since only that named cluster's worker drains the
    lane), then by `worker_pid`. A cluster with no attempts in the window is
    omitted entirely: an idle worker only appears after its first task,
    which is what the panel's footnote in the template says.
    """
    window_start = now - timedelta(seconds=WORKER_WINDOW_SECONDS)
    throughput_start = now - timedelta(seconds=THROUGHPUT_WINDOW_SECONDS)

    attempts = QraftTaskAttempt.objects.filter(worker_pid__isnull=False).filter(
        Q(date_started__gte=window_start) | Q(date_completed__gte=window_start)
    )

    capacity = {
        profile["name"]: profile["workers"] * profile["threads"]
        for profile in profile_summaries()
    }

    # One aggregate query for every cluster's throughput, rather than one
    # count() per cluster observed below.
    throughput_by_cluster = dict(
        QraftTaskAttempt.objects.filter(date_completed__gte=throughput_start)
        .values("cluster")
        .annotate(n=Count("id"))
        .values_list("cluster", "n")
    )

    clusters: dict[str, dict[int, dict]] = {}
    for attempt in attempts:
        cluster_name = attempt.cluster or "?"
        worker = clusters.setdefault(cluster_name, {}).setdefault(
            attempt.worker_pid,
            {
                "pid": attempt.worker_pid,
                "inflight": 0,
                "done": 0,
                "failed": 0,
                "durations": [],
                "last_seen": None,
            },
        )
        if attempt.success is None:
            worker["inflight"] += 1
        elif attempt.success:
            worker["done"] += 1
        else:
            worker["failed"] += 1
        if attempt.date_started and attempt.date_completed:
            worker["durations"].append(
                (attempt.date_completed - attempt.date_started).total_seconds()
            )
        seen = max(
            (
                t
                for t in (
                    attempt.date_started,
                    attempt.heartbeat_at,
                    attempt.date_completed,
                )
                if t
            ),
            default=None,
        )
        if seen and (worker["last_seen"] is None or seen > worker["last_seen"]):
            worker["last_seen"] = seen

    panel = []
    for cluster_name, workers_by_pid in clusters.items():
        throughput = throughput_by_cluster.get(cluster_name, 0)
        worker_rows = []
        inflight_total = 0
        for pid, w in sorted(workers_by_pid.items()):
            inflight_total += w["inflight"]
            worker_rows.append(
                {
                    "pid": pid,
                    "inflight": w["inflight"],
                    "done": w["done"],
                    "failed": w["failed"],
                    "avg_duration": (
                        round(sum(w["durations"]) / len(w["durations"]), 1)
                        if w["durations"]
                        else None
                    ),
                    "last_seen": (
                        round((now - w["last_seen"]).total_seconds(), 1)
                        if w["last_seen"]
                        else None
                    ),
                }
            )
        panel.append(
            {
                "name": cluster_name,
                "capacity": capacity.get(cluster_name),
                "inflight": inflight_total,
                "throughput": throughput,
                "workers": worker_rows,
            }
        )

    panel.sort(key=lambda cluster: cluster["name"])
    return panel


def dashboard(request):
    runner.load()
    return render(
        request,
        "showcase/dashboard.html",
        {
            "scenarios": runner.ordered_scenarios(),
            "cluster_profiles": profile_summaries(),
        },
    )


def state(request):
    """Everything the dashboard polls for, in one round trip."""
    now = timezone.now()

    tasks = []
    recent = list(
        QraftTask.objects.order_by("-date_created")
        .prefetch_related("attempts")
        .select_related("qraft_iter", "qraft_batch")[:TASK_LIMIT]
    )

    # Every scenario task function takes `run` as its first positional
    # argument (task.task_args[0]), and it's the same id the scenario's own
    # Ctx.run was created with - so a run-id lookup links a task back to the
    # scenario that queued it, with no changes to qraft itself. Tasks started
    # outside a scenario (soak, manual) simply miss this map.
    run_ids = {
        task.task_args[0]
        for task in recent
        if task.task_args and isinstance(task.task_args[0], str)
    }
    scenario_by_run = dict(
        ScenarioRun.objects.filter(run__in=run_ids).values_list("run", "key")
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
        run_id = (
            task.task_args[0]
            if task.task_args and isinstance(task.task_args[0], str)
            else None
        )
        if run_id in scenario_by_run:
            scenario = scenario_by_run[run_id]
        elif run_id and run_id.startswith("soak-"):
            scenario = "soak"
        else:
            scenario = ""
        tasks.append(
            {
                "id": str(task.id),
                "short": _short(task.id),
                "scenario": scenario,
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
                "worker": _worker_label(latest),
                "workflow": (
                    "iter"
                    if task.qraft_iter_id
                    else "batch"
                    if task.qraft_batch_id
                    else ""
                ),
            }
        )

    # Prefetch keeps each workflow kind to one extra query total instead of
    # one per row: chain steps come back sorted with their task already
    # joined, and iter/batch members are pre-sliced to the [:12] the panel
    # renders (a sliced Prefetch queryset requires to_attr - see the Django
    # docs on Prefetch objects).
    chain_prefetch = Prefetch(
        "steps",
        queryset=QraftChainStep.objects.select_related("qraft_task").order_by(
            "step_index"
        ),
    )
    member_prefetch = Prefetch(
        "tasks",
        queryset=QraftTask.objects.order_by("-date_created")[:12],
        to_attr="members",
    )

    workflows = []
    for model, kind, prefetch in (
        (QraftChainModel, "chain", chain_prefetch),
        (QraftIterModel, "iter", member_prefetch),
        (QraftBatchModel, "batch", member_prefetch),
    ):
        rows = model.objects.order_by("-date_created")[:12].prefetch_related(prefetch)
        for row in rows:
            if kind == "chain":
                steps = list(row.steps.all())
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
                    for task in row.members
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
            # Qraft-owned SCHEDULED attempts plus any legacy pre-2.0 Schedules.
            "scheduled": QraftTaskAttempt.objects.filter(
                state=QraftTaskAttempt.AttemptState.SCHEDULED
            ).count()
            + Schedule.objects.count(),
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
            "workers": _workers_panel(now),
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
def start_cluster(request, name: str):
    """Boot one worker cluster from the dashboard."""
    if name not in PROFILES:
        raise Http404(name)
    _clusters().ensure([name])
    return JsonResponse({"started": name})


@require_POST
def stop_cluster(request, name: str):
    """Stop one worker cluster from the dashboard."""
    if name not in PROFILES:
        raise Http404(name)
    _clusters().stop(name)
    return JsonResponse({"stopped": name})


# Bounds for the soak panel, so a typo cannot enqueue an afternoon of work.
SOAK_MAX_COUNT = 200
SOAK_MAX_SECONDS = 3600.0


@require_POST
def start_soak(request):
    """
    Fan out long fake-API tasks onto the soak cluster.

    Each task sleeps a random duration in [min_seconds, max_seconds], reports
    progress as it goes, and fails a `fail_pct` share of the time so retries
    show up too. The regular task table is the watcher.
    """
    from qraft.tasks import async_task

    try:
        params = json.loads(request.body or "{}")
        count = min(int(params.get("count", 8)), SOAK_MAX_COUNT)
        low = min(float(params.get("min_seconds", 180)), SOAK_MAX_SECONDS)
        high = min(float(params.get("max_seconds", 600)), SOAK_MAX_SECONDS)
        fail_pct = max(0.0, min(float(params.get("fail_pct", 20)), 100.0))
    except (ValueError, TypeError):
        return HttpResponseBadRequest("count, min_seconds, max_seconds, fail_pct")
    if count < 1 or low <= 0 or high < low:
        return HttpResponseBadRequest("need count >= 1 and 0 < min <= max")

    _clusters().ensure(["soak"])
    run = f"soak-{uuid.uuid4().hex[:6]}"
    for index in range(1, count + 1):
        async_task(
            "showcase.tasks.fake_api_task",
            run,
            f"api-{index:02d}",
            seconds=round(random.uniform(low, high), 1),
            fail_pct=fail_pct,
            qraft_options={
                "cluster": "soak",
                "max_attempts": 3,
                "base_delay": 10.0,
                "backoff_strategy": "exponential",
            },
        )
    return JsonResponse({"run": run, "count": count})


@require_POST
def requeue_task(request, task_id: str):
    """Send a dead task back through the queue from the dashboard."""
    try:
        task = QraftTask.objects.get(id=task_id)
    except (QraftTask.DoesNotExist, ValueError) as error:
        raise Http404(task_id) from error
    requeue(task)
    return JsonResponse({"requeued": str(task.id)})


@require_POST
def reset(request):
    """
    Wipe all demo data for a fresh start.

    Clusters are stopped first, not left running: a worker mid-task would
    otherwise keep writing attempt/heartbeat updates against rows this just
    deleted underneath it. Rejected while a scenario thread is still running:
    that thread owns tasks/events it is mid-write on, and a durability
    scenario may even spawn clusters of its own partway through - reset would
    race both. No cancellation machinery here, just an honest "try again".
    """
    with _LOCK:
        active = sorted(_ACTIVE)
    if active:
        return JsonResponse(
            {"reset": False, "error": f"scenarios still running: {', '.join(active)}"},
            status=409,
        )
    _clusters().stop_all()
    reset_state()
    return JsonResponse({"reset": True})
