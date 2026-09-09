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

from qraft import metrics as sink
from qraft.batch import QraftBatch
from qraft.chain import QraftChain
from qraft.dlq import dead_letters, requeue
from qraft.iter import QraftIter
from qraft.models import (
    InvalidStatusTransition,
    QraftBatchModel,
    QraftChainModel,
    QraftIterModel,
    QraftRun,
    QraftTask,
    QraftTaskAttempt,
    RateBucket,
    TaskStatus,
    WorkflowStatus,
)
from qraft.models.runs import RunStatus, UnitType
from qraft.models.tasks import AttemptState

from . import metrics

logger = logging.getLogger("qraft.dashboard")

TASK_LIMIT = 60
WORKFLOW_LIMIT = 12
RUN_LIMIT = 12
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


# UnitType -> the admin change view for the row a stage is bound to. The task
# page carries the attempt inline, which is where a stage failure's exception
# and traceback actually are.
_UNIT_ADMIN_VIEWS = {
    UnitType.TASK: "admin:qraft_qrafttask_change",
    UnitType.CHAIN: "admin:qraft_qraftchainmodel_change",
    UnitType.ITER: "admin:qraft_qraftitermodel_change",
    UnitType.BATCH: "admin:qraft_qraftbatchmodel_change",
}


def _unit_url(unit_type, unit_id) -> str | None:
    """Admin link to the row a stage is bound to, or None when unreachable."""
    view = _UNIT_ADMIN_VIEWS.get(unit_type)
    if not view or not unit_id:
        return None
    try:
        return reverse(view, args=[unit_id])
    except NoReverseMatch:
        return None


def _stage_errors(runs) -> dict:
    """
    `(task id) -> latest attempt's exception class`, for the task-bound stages
    of the runs being rendered.

    A stage that failed before its subject existed is visible only on the run,
    so the run row is where the exception has to surface.
    """
    task_ids = [
        stage.unit_id
        for run in runs
        for stage in run.stages.all()
        if stage.unit_type == UnitType.TASK and stage.unit_id
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
    """Subject/run filters from the query string; empty values are ignored."""
    filters = {}
    for name in ("subject_type", "subject_id", "run"):
        value = request.GET.get(name)
        if value:
            filters[name] = value
    return filters


def _apply_filters(
    queryset,
    filters: dict,
    run_field: str = "run_id",
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
    if "run" in filters:
        queryset = queryset.filter(**{run_field: filters["run"]})
    return queryset


def _current_progress(task, latest) -> dict:
    """
    Progress belonging to the attempt that is actually current.

    The latest attempt owns it. The task column is a snapshot, and falling back
    to it unconditionally reports a superseded attempt's numbers as the running
    one's: attempt 1 stops at 9/10 and fails, attempt 2 starts and has not
    reported yet. The snapshot is only shown when it names that latest attempt -
    or names none at all, which is what a pre-1.4 row looks like.
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


def _run_rows(now, filters: dict | None = None) -> list[dict]:
    recent = _apply_filters(QraftRun.objects.all(), filters or {}, run_field="id")
    runs = list(recent.order_by("-date_created").prefetch_related("stages")[:RUN_LIMIT])
    errors = _stage_errors(runs)
    rows = []
    for run in runs:
        end = run.settled_at or now
        rows.append(
            {
                "id": str(run.id),
                "short": _short(run.id),
                "subject_type": run.subject_type,
                "subject_id": run.subject_id,
                "kind": run.kind or "",
                "revision": run.revision or "",
                "status": run.status,
                "elapsed": round((end - run.date_started).total_seconds(), 1),
                "overdue": run.overdue_flagged_at is not None,
                "previous_run": (
                    _short(run.previous_run_id) if run.previous_run_id else ""
                ),
                "previous_run_id": (
                    str(run.previous_run_id) if run.previous_run_id else ""
                ),
                "stages": [
                    {
                        "name": stage.name,
                        "status": stage.status,
                        "unit_type": stage.unit_type or "",
                        "unit_id": str(stage.unit_id) if stage.unit_id else "",
                        "unit_url": _unit_url(stage.unit_type, stage.unit_id),
                        "error": errors.get(str(stage.unit_id)) or "",
                    }
                    for stage in sorted(run.stages.all(), key=lambda s: s.position)
                ],
                "can_settle": run.status == RunStatus.OPEN,
            }
        )
    return rows


def _workflow_rows(filters: dict | None = None) -> list[dict]:
    rows = []
    for model, kind in (
        (QraftChainModel, "chain"),
        (QraftIterModel, "iter"),
        (QraftBatchModel, "batch"),
    ):
        recent = _apply_filters(model.objects.all(), filters or {})
        for row in recent.order_by("-date_created")[:WORKFLOW_LIMIT]:
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
        QraftTaskAttempt.objects.exclude(usage=None),
        filters,
        run_field="qraft_task__run_id",
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
    for row in QraftTask.objects.values("status").annotate(n=Count("id")):
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
            # Pre-2.0 retries lived in django-q2 Schedules; nonzero only on
            # databases that still carry them.
            "legacy_scheduled": Schedule.objects.count(),
            "filters": filters,
            "tasks": _task_rows(now, filters),
            "runs": _run_rows(now, filters),
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
                for task in dead_letters()[:DLQ_LIMIT]
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
def settle_run(request, action, run_id):
    """Cancel or abandon an open run. Neither revokes work already in flight."""
    if action not in ("cancel", "abandon"):
        return _not_found("run action", action)
    from qraft import runs

    try:
        if action == "cancel":
            runs.cancel(run_id)
        else:
            runs.abandon(run_id, reason="dashboard")
    except runs.RunError as error:
        status = 404 if "unknown run" in str(error) else 409
        return JsonResponse({"error": str(error)}, status=status)
    logger.info("Dashboard %sed run %s", action, run_id)
    return JsonResponse({action: str(run_id)})


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
