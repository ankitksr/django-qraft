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
from django.db.models import Count
from django.http import JsonResponse
from django.shortcuts import render
from django.urls import NoReverseMatch, reverse
from django.utils import timezone
from django.views.decorators.csrf import csrf_protect
from django.views.decorators.http import require_POST
from django_q.models import OrmQ, Schedule

from qraft.batch import QraftBatch
from qraft.chain import QraftChain
from qraft.dlq import dead_letters, requeue
from qraft.iter import QraftIter
from qraft.models import (
    InvalidStatusTransition,
    QraftBatchModel,
    QraftChainModel,
    QraftIterModel,
    QraftTask,
    QraftTaskAttempt,
    RateBucket,
    TaskStatus,
    WorkflowStatus,
)
from qraft.models.tasks import AttemptState

from . import metrics

logger = logging.getLogger("qraft.dashboard")

TASK_LIMIT = 60
WORKFLOW_LIMIT = 12
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


@staff_required
def dashboard(request):
    return render(request, "qraft_dashboard/dashboard.html")


def _task_rows(now) -> list[dict]:
    recent = (
        QraftTask.objects.order_by("-date_created")
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

        progress = task.progress or {}
        rows.append(
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
                "workflow": workflow,
                "admin_url": _admin_url(task.id),
            }
        )
    return rows


def _workflow_rows() -> list[dict]:
    rows = []
    for model, kind in (
        (QraftChainModel, "chain"),
        (QraftIterModel, "iter"),
        (QraftBatchModel, "batch"),
    ):
        for row in model.objects.order_by("-date_created")[:WORKFLOW_LIMIT]:
            gated = False
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
                    for task in row.tasks.all()[:MEMBER_LIMIT]
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


@staff_required(json=True)
def state(request):
    """Everything the dashboard polls for, in one round trip."""
    now = timezone.now()

    counts = {status: 0 for status, _ in TaskStatus.choices}
    for row in QraftTask.objects.values("status").annotate(n=Count("id")):
        counts[row["status"]] = row["n"]

    scheduled_qs = QraftTaskAttempt.objects.filter(state=AttemptState.SCHEDULED)
    next_due = (
        scheduled_qs.exclude(not_before=None)
        .order_by("not_before")
        .values_list("not_before", flat=True)
        .first()
    )

    usage: dict[str, float] = {}
    for values in QraftTaskAttempt.objects.exclude(usage=None).values_list(
        "usage", flat=True
    )[:USAGE_ATTEMPT_CAP]:
        for key, value in (values or {}).items():
            if isinstance(value, (int, float)) and not isinstance(value, bool):
                usage[key] = round(usage.get(key, 0) + value, 6)

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
            # Pre-2.0 retries lived in django-q2 Schedules; nonzero only on
            # databases that still carry them.
            "legacy_scheduled": Schedule.objects.count(),
            "tasks": _task_rows(now),
            "workflows": _workflow_rows(),
            "dlq": [
                {
                    "id": str(task.id),
                    "short": _short(task.id),
                    "func": _func(task.func),
                    "status": task.status,
                    "attempts": len(task.attempts.all()),
                    "key": task.idempotency_key or "",
                }
                for task in dead_letters()[:DLQ_LIMIT]
            ],
            "buckets": [
                {"key": bucket.key, "tokens": round(bucket.tokens, 2)}
                for bucket in RateBucket.objects.order_by("key")
            ],
            "usage": usage,
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
