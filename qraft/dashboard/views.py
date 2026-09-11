"""
Bundled monitoring dashboard.

Enable with:

    INSTALLED_APPS += ["qraft.dashboard"]

    urlpatterns += [path("qraft/", include("qraft.dashboard.urls"))]

Every view requires an active staff user; ``QRAFT_DASHBOARD = {"public": True}``
in Django settings waives the check (demos, local development).
"""

import logging
from functools import wraps

from django.conf import settings
from django.contrib.auth.views import redirect_to_login
from django.core.exceptions import ObjectDoesNotExist
from django.db.models import Count, Prefetch
from django.http import JsonResponse
from django.shortcuts import render
from django.urls import NoReverseMatch, reverse
from django.utils import timezone
from django.views.decorators.csrf import csrf_protect
from django.views.decorators.http import require_POST
from django_q.models import OrmQ, Schedule

from qraft import metrics as sink
from qraft.batch import QraftBatch
from qraft.chain import QraftChain
from qraft.dlq import dead_letters, requeue
from qraft.iter import QraftIter
from qraft.models import (
    InvalidStatusTransition,
    QraftBatchModel,
    QraftChainModel,
    QraftChainStep,
    QraftGraph,
    QraftIterModel,
    QraftTask,
    QraftTaskAttempt,
    RateBucket,
    TaskStatus,
    WorkflowStatus,
)
from qraft.models.graphs import GraphStatus, NodeStatus
from qraft.models.tasks import AttemptState

from . import metrics

logger = logging.getLogger("qraft.dashboard")

TASK_LIMIT = 60
WORKFLOW_LIMIT = 12
GRAPH_LIMIT = 12
MEMBER_LIMIT = 12
DLQ_LIMIT = 20
USAGE_ATTEMPT_CAP = 500

CANCELLABLE = (
    WorkflowStatus.PENDING,
    WorkflowStatus.RUNNING,
    WorkflowStatus.WAITING_APPROVAL,
)


def staff_required(view=None, *, json=False):
    """
    Gate a view on ``request.user.is_active and request.user.is_staff``.

    ``QRAFT_DASHBOARD = {"public": True}`` waives the check. Page views
    redirect to the admin login (or ``LOGIN_URL`` when the admin is not
    mounted); API views answer 403 JSON so the poller can surface it.
    """

    def decorate(fn):
        @wraps(fn)
        def wrapped(request, *args, **kwargs):
            if getattr(settings, "QRAFT_DASHBOARD", {}).get("public"):
                return fn(request, *args, **kwargs)
            user = getattr(request, "user", None)
            if user is not None and user.is_active and user.is_staff:
                return fn(request, *args, **kwargs)
            if json:
                return JsonResponse({"error": "staff required"}, status=403)
            try:
                login_url = reverse("admin:login")
            except NoReverseMatch:
                login_url = None  # redirect_to_login falls back to LOGIN_URL
            return redirect_to_login(request.get_full_path(), login_url)

        return wrapped

    return decorate(view) if view else decorate


def _short(value) -> str:
    return str(value)[:8]


def _func(path: str) -> str:
    return path.rsplit(".", 1)[-1] if path else ""


def _admin_url(task_id) -> str | None:
    # The admin may not be installed or mounted, and QraftTask may not be
    # registered on it; NoReverseMatch covers every one of those cases.
    try:
        return reverse("admin:qraft_qrafttask_change", args=[task_id])
    except NoReverseMatch:
        return None


def _task_url(task_id) -> str | None:
    return _admin_url(task_id)


def _node_errors(graphs) -> dict:
    """`(task id) -> latest attempt's exception class` for graph node tasks."""
    task_ids = [
        node.task_id for graph in graphs for node in graph.nodes.all() if node.task_id
    ]
    if not task_ids:
        return {}
    errors = {}
    for attempt in (
        QraftTaskAttempt.objects.filter(qraft_task_id__in=task_ids, success=False)
        .order_by("qraft_task_id", "attempt_number")
        .values("qraft_task_id", "exception_class")
    ):
        errors[str(attempt["qraft_task_id"])] = attempt["exception_class"]
    return errors


@staff_required
def dashboard(request):
    return render(request, "qraft_dashboard/dashboard.html")


def _filters(request) -> dict:
    """Subject/graph filters from the query string; empty values are ignored."""
    filters = {}
    for name in ("subject_type", "subject_id", "graph"):
        value = request.GET.get(name)
        if value:
            filters[name] = value
    return filters


def _apply_filters(
    queryset,
    filters: dict,
    graph_field: str = "graph_id",
    subject_prefix: str = "",
):
    if "subject_type" in filters:
        queryset = queryset.filter(
            **{f"{subject_prefix}subject_type": filters["subject_type"]}
        )
    if "subject_id" in filters:
        queryset = queryset.filter(
            **{f"{subject_prefix}subject_id": filters["subject_id"]}
        )
    if "graph" in filters:
        queryset = queryset.filter(**{graph_field: filters["graph"]})
    return queryset


def _current_progress(task, latest) -> dict:
    """
    Progress belonging to the attempt that is actually current.

    The latest attempt owns it. The task column is a snapshot, and falling back
    to it unconditionally reports a superseded attempt's numbers as the running
    one's: attempt 1 stops at 9/10 and fails, attempt 2 starts and has not
    reported yet. The snapshot is only shown when it names that latest attempt -
    or names none at all, which is what a row from before the column existed
    looks like.
    """
    if latest is not None and latest.progress:
        return latest.progress
    snapshot = task.progress or {}
    if latest is None or snapshot.get("attempt_id") in (None, str(latest.id)):
        return snapshot
    return {}


def _age(when, now) -> float | None:
    return round((now - when).total_seconds(), 1) if when else None


def _task_rows(now, filters: dict | None = None) -> list[dict]:
    recent = (
        _apply_filters(QraftTask.objects.all(), filters or {})
        .order_by("-date_created")
        .prefetch_related("attempts")
        .select_related("qraft_iter", "qraft_batch", "chain_step")[:TASK_LIMIT]
    )
    rows = []
    for task in recent:
        attempts = sorted(task.attempts.all(), key=lambda a: a.attempt_number)
        latest = attempts[-1] if attempts else None
        if latest and latest.date_started and latest.success is None:
            elapsed = (now - latest.date_started).total_seconds()
        elif latest and latest.date_completed:
            elapsed = (latest.date_completed - task.date_created).total_seconds()
        else:
            elapsed = (now - task.date_created).total_seconds()

        if task.qraft_iter_id:
            workflow = "iter"
        elif task.qraft_batch_id:
            workflow = "batch"
        elif getattr(task, "chain_step", None):
            workflow = "chain"
        else:
            workflow = ""

        progress = _current_progress(task, latest)
        rows.append(
            {
                "id": str(task.id),
                "short": _short(task.id),
                "func": _func(task.func),
                "status": task.status,
                "subject_type": task.subject_type,
                "subject_id": task.subject_id,
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
                "advanced_age": (
                    _age(latest.progress_advanced_at, now)
                    if latest and latest.success is None
                    else None
                ),
                "stalled": bool(latest and latest.stall_suspected_at),
                # A flagged attempt that advanced afterwards keeps the flag as
                # history; the row says "recovered" rather than dropping it.
                "stall_recovered": bool(
                    latest
                    and latest.stall_suspected_at
                    and latest.progress_advanced_at
                    and latest.progress_advanced_at > latest.stall_suspected_at
                ),
                "workflow": workflow,
                "admin_url": _admin_url(task.id),
            }
        )
    return rows


def _graph_rows(now, filters: dict | None = None) -> list[dict]:
    from qraft import graphs as graphs_module

    recent = _apply_filters(QraftGraph.objects.all(), filters or {}, graph_field="id")
    graphs = list(
        recent.order_by("-date_created").prefetch_related("nodes")[:GRAPH_LIMIT]
    )
    errors = _node_errors(graphs)
    rows = []
    for graph in graphs:
        end = graph.settled_at or now
        rows.append(
            {
                "id": str(graph.id),
                "short": _short(graph.id),
                "subject_type": graph.subject_type,
                "subject_id": graph.subject_id,
                "kind": graph.kind or "",
                "revision": graph.revision or "",
                "status": graph.status,
                "generation": graph.generation,
                "elapsed": round((end - graph.date_started).total_seconds(), 1),
                "overdue": graph.overdue_flagged_at is not None,
                "previous_graph": (
                    _short(graph.previous_graph_id) if graph.previous_graph_id else ""
                ),
                "previous_graph_id": (
                    str(graph.previous_graph_id) if graph.previous_graph_id else ""
                ),
                "nodes": [
                    {
                        "key": node.key,
                        "status": node.status,
                        "task_id": str(node.task_id) if node.task_id else "",
                        "task_url": _task_url(node.task_id),
                        "error": errors.get(str(node.task_id)) or "",
                        "can_approve": (
                            graph.status
                            in (GraphStatus.RUNNING, GraphStatus.WAITING_APPROVAL)
                            and node.status == NodeStatus.WAITING_APPROVAL
                        ),
                        "can_skip": (
                            graph.status == GraphStatus.RUNNING
                            and node.status == NodeStatus.PENDING
                        ),
                    }
                    for node in sorted(
                        graph.nodes.all(), key=lambda n: (n.depth, n.position)
                    )
                ],
                "can_cancel": graph.status
                in (GraphStatus.RUNNING, GraphStatus.WAITING_APPROVAL),
                "can_resume": graph.status == GraphStatus.FAILED,
                # The preview is what makes resume a decision rather than a
                # guess, so it travels with the button.
                "resume_preview": (
                    graphs_module._resume_preview(graph)
                    if graph.status == GraphStatus.FAILED
                    else None
                ),
            }
        )
    return rows


def _workflow_rows(filters: dict | None = None) -> list[dict]:
    rows = []
    chain_steps = Prefetch(
        "steps",
        queryset=QraftChainStep.objects.select_related("qraft_task").order_by(
            "step_index"
        ),
    )
    member_prefetch = Prefetch(
        "tasks",
        queryset=QraftTask.objects.order_by("-date_created")[:MEMBER_LIMIT],
        to_attr="dashboard_members",
    )
    for model, kind in (
        (QraftChainModel, "chain"),
        (QraftIterModel, "iter"),
        (QraftBatchModel, "batch"),
    ):
        recent = _apply_filters(model.objects.all(), filters or {}).prefetch_related(
            chain_steps if kind == "chain" else member_prefetch
        )
        for row in recent.order_by("-date_created")[:WORKFLOW_LIMIT]:
            gated = False
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
                gated = any(step.requires_approval for step in steps)
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
                    for task in row.dashboard_members
                ]
                counters = {
                    "completed": row.completed_count,
                    "total": row.total_count,
                    "success": row.success_count,
                    "failure": row.failure_count,
                }
            rows.append(
                {
                    "id": str(row.id),
                    "short": _short(row.id),
                    "kind": kind,
                    "status": row.status,
                    "subject_type": row.subject_type,
                    "subject_id": row.subject_id,
                    "gated": gated,
                    "counters": counters,
                    "members": members,
                    "can_approve": (
                        kind == "chain"
                        and row.status == WorkflowStatus.WAITING_APPROVAL
                    ),
                    "can_cancel": row.status in CANCELLABLE,
                }
            )
    return rows


def _usage_rollup(filters: dict) -> tuple[dict, dict | None]:
    """
    Token totals and the priced summary over the attempts in view.

    Honours the same subject and run filters as the task table, so the panel
    answers "what did this worksheet cost" as readily as "what did today cost".
    Capped at USAGE_ATTEMPT_CAP attempts per poll, like the rest of the page.
    """
    from qraft.context import _sum_usage

    attempts = _apply_filters(
        QraftTaskAttempt.objects.filter(usage__isnull=False),
        filters,
        graph_field="qraft_task__graph_id",
        subject_prefix="qraft_task__",
    )
    rows = list(attempts.values_list("usage", flat=True)[:USAGE_ATTEMPT_CAP])
    totals = _sum_usage(rows)
    cost = totals.pop("cost_summary", None)
    usage = {
        key: round(value, 6)
        for key, value in totals.items()
        if isinstance(value, (int, float)) and not isinstance(value, bool)
    }
    return usage, cost


@staff_required(json=True)
def state(request):
    """Everything the dashboard polls for, in one round trip."""
    now = timezone.now()
    filters = _filters(request)

    counts = {status: 0 for status, _ in TaskStatus.choices}
    for row in (
        _apply_filters(QraftTask.objects.all(), filters)
        .values("status")
        .annotate(n=Count("id"))
    ):
        counts[row["status"]] = row["n"]

    scheduled_qs = QraftTaskAttempt.objects.filter(state=AttemptState.SCHEDULED)
    next_due = (
        scheduled_qs.exclude(not_before=None)
        .order_by("not_before")
        .values_list("not_before", flat=True)
        .first()
    )

    usage, cost = _usage_rollup(filters)

    return JsonResponse(
        {
            "now": now.isoformat(),
            "counts": counts,
            "queued": OrmQ.objects.count(),
            "scheduled": scheduled_qs.count(),
            "next_due_in": (
                max(0.0, round((next_due - now).total_seconds(), 1))
                if next_due
                else None
            ),
            # Pre-1.3 retries lived in django-q2 Schedules; nonzero only on
            # databases that still carry them.
            "legacy_scheduled": Schedule.objects.count(),
            "filters": filters,
            "tasks": _task_rows(now, filters),
            "graphs": _graph_rows(now, filters),
            "workflows": _workflow_rows(filters),
            "dlq": [
                {
                    "id": str(task.id),
                    "short": _short(task.id),
                    "func": _func(task.func),
                    "status": task.status,
                    "attempts": len(task.attempts.all()),
                    "key": task.idempotency_key or "",
                }
                for task in _apply_filters(dead_letters(), filters)[:DLQ_LIMIT]
            ],
            "buckets": [
                {"key": bucket.key, "tokens": round(bucket.tokens, 2)}
                for bucket in RateBucket.objects.order_by("key")
            ],
            "usage": usage,
            "cost": cost,
            "metrics_health": sink.health(),
        }
    )


@staff_required(json=True)
def metrics_view(request):
    return JsonResponse(metrics.snapshot(request.GET.get("window")))


def _not_found(kind: str, object_id) -> JsonResponse:
    return JsonResponse({"error": f"unknown {kind} {object_id}"}, status=404)


@require_POST
@staff_required(json=True)
@csrf_protect
def requeue_task(request, task_id):
    try:
        task = QraftTask.objects.get(id=task_id)
    except QraftTask.DoesNotExist:
        return _not_found("task", task_id)
    try:
        attempt_id = requeue(task)
    except ValueError as error:
        return JsonResponse({"error": str(error)}, status=409)
    logger.info("Dashboard requeued task %s", task_id)
    return JsonResponse({"requeued": str(task_id), "attempt_id": attempt_id})


@require_POST
@staff_required(json=True)
@csrf_protect
def approve_chain(request, chain_id):
    try:
        chain = QraftChain(chain_id=chain_id)
    except QraftChainModel.DoesNotExist:
        return _not_found("chain", chain_id)
    try:
        chain.approve()
    except InvalidStatusTransition as error:
        return JsonResponse({"error": str(error)}, status=409)
    logger.info("Dashboard approved chain %s", chain_id)
    return JsonResponse({"approved": str(chain_id)})


@require_POST
@staff_required(json=True)
@csrf_protect
def reject_chain(request, chain_id):
    try:
        chain = QraftChain(chain_id=chain_id)
    except QraftChainModel.DoesNotExist:
        return _not_found("chain", chain_id)
    if chain._model.status == WorkflowStatus.CANCELLED:
        return JsonResponse({"rejected": str(chain_id), "already": True})
    try:
        chain.reject(reason="dashboard")
    except InvalidStatusTransition as error:
        return JsonResponse({"error": str(error)}, status=409)
    logger.info("Dashboard rejected chain %s", chain_id)
    return JsonResponse({"rejected": str(chain_id)})


@require_POST
@staff_required(json=True)
@csrf_protect
def cancel_graph(request, graph_id):
    """Cancel a running graph. Does not revoke work already in flight."""
    from qraft import graphs

    try:
        graphs.cancel(graph_id)
    except graphs.GraphError as error:
        status = 404 if "unknown graph" in str(error) else 409
        return JsonResponse({"error": str(error)}, status=status)
    logger.info("Dashboard cancelled graph %s", graph_id)
    return JsonResponse({"cancelled": str(graph_id)})


@require_POST
@staff_required(json=True)
@csrf_protect
def resume_graph(request, graph_id):
    """Re-run a failed graph's failed nodes and everything downstream."""
    from qraft import graphs

    try:
        rerun = graphs.resume(graph_id)
    except graphs.GraphError as error:
        status = 404 if "unknown graph" in str(error) else 409
        return JsonResponse({"error": str(error)}, status=status)
    logger.info("Dashboard resumed graph %s (%d nodes)", graph_id, rerun)
    return JsonResponse({"resumed": str(graph_id), "rerun": rerun})


@require_POST
@staff_required(json=True)
@csrf_protect
def skip_graph_node(request, graph_id, node_key):
    from qraft import graphs

    try:
        graphs.skip(graph_id, node_key, reason="dashboard")
    except graphs.GraphError as error:
        status = 404 if "unknown graph" in str(error) else 409
        return JsonResponse({"error": str(error)}, status=status)
    logger.info("Dashboard skipped node %s of graph %s", node_key, graph_id)
    return JsonResponse({"skipped": node_key})


# QraftIter's constructor takes func positionally but ignores it when handed
# an existing id, so the placeholder never lands anywhere.
WORKFLOWS = {
    "chain": (QraftChainModel, lambda id: QraftChain(chain_id=id)),
    "iter": (QraftIterModel, lambda id: QraftIter("", iter_id=id)),
    "batch": (QraftBatchModel, lambda id: QraftBatch(batch_id=id)),
}


@require_POST
@staff_required(json=True)
@csrf_protect
def cancel_workflow(request, kind, workflow_id):
    if kind not in WORKFLOWS:
        return _not_found("workflow kind", kind)
    model, wrap = WORKFLOWS[kind]
    try:
        row = model.objects.get(id=workflow_id)
    except ObjectDoesNotExist:
        return _not_found(kind, workflow_id)
    if row.status == WorkflowStatus.CANCELLED:
        return JsonResponse({"cancelled": str(workflow_id), "already": True})
    try:
        wrap(workflow_id).cancel()
    except InvalidStatusTransition as error:
        return JsonResponse({"error": str(error)}, status=409)
    logger.info("Dashboard cancelled %s %s", kind, workflow_id)
    return JsonResponse({"cancelled": str(workflow_id)})


@require_POST
@staff_required(json=True)
@csrf_protect
def approve_graph_node(request, graph_id, node_key):
    return _decide_graph_node(graph_id, node_key, approve=True)


@require_POST
@staff_required(json=True)
@csrf_protect
def reject_graph_node(request, graph_id, node_key):
    return _decide_graph_node(graph_id, node_key, approve=False)


def _decide_graph_node(graph_id, node_key, approve):
    from qraft import graphs

    try:
        if approve:
            graphs.approve(graph_id, node_key)
        else:
            graphs.reject(graph_id, node_key, reason="dashboard")
    except graphs.GraphError as error:
        return JsonResponse({"error": str(error)}, status=409)
    return JsonResponse({"node": node_key, "approved": approve})
