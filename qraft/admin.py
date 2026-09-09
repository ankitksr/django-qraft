"""
Django admin configuration for Qraft models.
"""

from django.contrib import admin, messages
from django.db.models import Count
from django.urls import reverse
from django.utils.html import format_html

from .dlq import requeue
from .models import (
    HookDispatch,
    QraftBatchModel,
    QraftChainModel,
    QraftChainStep,
    QraftGraph,
    QraftGraphNode,
    QraftIterModel,
    QraftTask,
    QraftTaskAttempt,
    WorkflowHookDispatch,
)

_DEFAULT_STATUS_COLOR = "#6c757d"

_WORKFLOW_STATUS_COLORS = {
    "pending": _DEFAULT_STATUS_COLOR,
    "running": "#007bff",
    "succeeded": "#28a745",
    "failed": "#dc3545",
    "cancelled": "#fd7e14",
}

_TASK_STATUS_COLORS = {
    **_WORKFLOW_STATUS_COLORS,
    "exhausted": "#6f42c1",
}

_GRAPH_STATUS_COLORS = {
    "running": "#007bff",
    "succeeded": "#28a745",
    "failed": "#dc3545",
    "cancelled": "#fd7e14",
}


def _short_uuid(uuid_val):
    """Return shortened UUID for display."""
    return str(uuid_val)[:8] if uuid_val else "-"


def _colored_status(status, display, colors):
    """Return HTML-formatted colored status badge."""
    return format_html(
        '<span style="color: {}; font-weight: bold;">{}</span>',
        colors.get(status, _DEFAULT_STATUS_COLOR),
        display,
    )


def _compact_usage(usage):
    """Render a usage JSON dict compactly (e.g. 'input_tokens=120, cost=0.02')."""
    if not usage:
        return "-"
    return ", ".join(f"{key}={value}" for key, value in usage.items())


class _ReadOnly:
    """Forbids adding and deleting; works for both ModelAdmin and inlines."""

    def has_add_permission(self, request, obj=None):
        return False

    def has_delete_permission(self, request, obj=None):
        return False


class _UsageDisplay:
    """Renders the attempt `usage` JSON column."""

    def usage_display(self, obj):
        return _compact_usage(obj.usage)

    usage_display.short_description = "Usage"


class _ShortIdAdmin(_ReadOnly):
    """Read-only admin that lists a truncated UUID primary key."""

    def short_id(self, obj):
        return _short_uuid(obj.id)

    short_id.short_description = "ID"


class _QraftTaskLinkAdmin(_ShortIdAdmin):
    """Read-only admin for models pointing at a QraftTask."""

    def qraft_task_link(self, obj):
        url = reverse("admin:qraft_qrafttask_change", args=[obj.qraft_task_id])
        return format_html('<a href="{}">{}</a>', url, _short_uuid(obj.qraft_task_id))

    qraft_task_link.short_description = "Qraft Task"


class _WorkflowAdmin(_ShortIdAdmin):
    """Shared display helpers for the chain/iter/batch admins."""

    # (model field, single-letter flag) shown in the Hooks column
    _hook_flags = (("success_hook", "S"), ("failure_hook", "F"), ("on_cancelled", "C"))

    def status_display(self, obj):
        return _colored_status(
            obj.status, obj.get_status_display(), _WORKFLOW_STATUS_COLORS
        )

    status_display.short_description = "Status"

    def hooks_display(self, obj):
        flags = [flag for field, flag in self._hook_flags if getattr(obj, field)]
        return ", ".join(flags) or "-"

    hooks_display.short_description = "Hooks"


class _ParallelWorkflowAdmin(_WorkflowAdmin):
    """Shared display helpers for the counter-tracking iter/batch admins."""

    _hook_flags = _WorkflowAdmin._hook_flags + (("progress_hook", "P"),)

    def counters_display(self, obj):
        return (
            f"{obj.completed_count}/{obj.total_count}"
            f" (S:{obj.success_count} F:{obj.failure_count})"
        )

    counters_display.short_description = "Progress"


# ── QraftTask ──────────────────────────────────────────────


class QraftTaskAttemptInline(_UsageDisplay, _ReadOnly, admin.TabularInline):
    """Inline display of task attempts within QraftTask admin."""

    model = QraftTaskAttempt
    extra = 0
    readonly_fields = [
        "id",
        "attempt_number",
        "q2_task_id",
        "success",
        "exception_class",
        "usage_display",
        "progress",
        "progress_reported_at",
        "progress_advanced_at",
        "stall_suspected_at",
        "cluster",
        "worker_pid",
        "worker_thread",
        "date_created",
        "enqueued_at",
        "returned_at",
        "date_completed",
    ]
    ordering = ["attempt_number"]


class HookDispatchInline(_ReadOnly, admin.TabularInline):
    """Inline display of hook dispatches within QraftTask admin."""

    model = HookDispatch
    extra = 0
    readonly_fields = [
        "id",
        "hook_type",
        "hook_path",
        "q2_task_id",
        "date_created",
    ]
    ordering = ["-date_created"]


@admin.register(QraftTask)
class QraftTaskAdmin(_ShortIdAdmin, admin.ModelAdmin):
    """Admin for QraftTask model."""

    list_display = [
        "short_id",
        "status_display",
        "func",
        "subject_type",
        "subject_id",
        "node",
        "attempt_count",
        "date_created",
        "date_updated",
    ]
    list_filter = ["status", "subject_type", "node", "date_created"]
    search_fields = ["id", "func", "subject_type", "subject_id", "graph__id"]
    readonly_fields = [
        "id",
        "date_created",
        "date_updated",
        "status",
        "func",
        "subject_type",
        "subject_id",
        "graph",
        "node",
        "graph_node",
        "task_args",
        "task_kwargs",
        "success_hook",
        "success_args",
        "success_kwargs",
        "failure_hook",
        "failure_args",
        "failure_kwargs",
        "retry_policy",
        "stall_after",
    ]
    inlines = [QraftTaskAttemptInline, HookDispatchInline]
    ordering = ["-date_created"]
    actions = ["requeue_dead_tasks"]

    fieldsets = [
        (None, {"fields": ["id", "status", "func", "date_created", "date_updated"]}),
        ("Subject", {"fields": ["subject_type", "subject_id"]}),
        ("Graph", {"fields": ["graph", "node", "graph_node"]}),
        ("Task Arguments", {"fields": ["task_args", "task_kwargs"]}),
        (
            "Success Hook",
            {"fields": ["success_hook", "success_args", "success_kwargs"]},
        ),
        (
            "Failure Hook",
            {"fields": ["failure_hook", "failure_args", "failure_kwargs"]},
        ),
        ("Retry Policy", {"fields": ["retry_policy"]}),
        ("Stall Observation", {"fields": ["stall_after"]}),
    ]

    def get_queryset(self, request):
        """Annotate attempt_count to avoid N+1 queries in list view."""
        qs = super().get_queryset(request)
        return qs.annotate(_attempt_count=Count("attempts"))

    def status_display(self, obj):
        return _colored_status(
            obj.status, obj.get_status_display(), _TASK_STATUS_COLORS
        )

    status_display.short_description = "Status"

    def attempt_count(self, obj):
        return getattr(obj, "_attempt_count", obj.attempts.count())

    attempt_count.short_description = "Attempts"
    attempt_count.admin_order_field = "_attempt_count"

    @admin.action(description="Requeue selected dead tasks")
    def requeue_dead_tasks(self, request, queryset):
        requeued = skipped = 0
        for task in queryset:
            try:
                requeue(task)
            except ValueError:
                skipped += 1
            else:
                requeued += 1

        self.message_user(
            request,
            f"Requeued {requeued} task(s); skipped {skipped} not in a dead state.",
            level=messages.WARNING if skipped else messages.INFO,
        )


@admin.register(QraftTaskAttempt)
class QraftTaskAttemptAdmin(_UsageDisplay, _QraftTaskLinkAdmin, admin.ModelAdmin):
    """Admin for QraftTaskAttempt model."""

    list_display = [
        "short_id",
        "qraft_task_link",
        "attempt_number",
        "success_display",
        "exception_class",
        "usage_display",
        "cluster",
        "worker_pid",
        "worker_thread",
        "date_created",
        "date_completed",
    ]
    list_filter = ["success", "cluster", "date_created"]
    search_fields = ["id", "q2_task_id", "exception_class", "qraft_task__id"]
    readonly_fields = [
        "id",
        "qraft_task",
        "attempt_number",
        "q2_task_id",
        "success",
        "exception_class",
        "usage_display",
        "progress",
        "progress_reported_at",
        "progress_advanced_at",
        "stall_suspected_at",
        "trace_context",
        "cluster",
        "worker_pid",
        "worker_thread",
        "date_created",
        "enqueued_at",
        "returned_at",
        "date_completed",
    ]
    ordering = ["-date_created"]

    def success_display(self, obj):
        if obj.success is None:
            return format_html('<span style="color: #6c757d;">Pending</span>')
        elif obj.success:
            return format_html('<span style="color: #28a745;">Success</span>')
        else:
            return format_html('<span style="color: #dc3545;">Failed</span>')

    success_display.short_description = "Outcome"


@admin.register(HookDispatch)
class HookDispatchAdmin(_QraftTaskLinkAdmin, admin.ModelAdmin):
    """Admin for HookDispatch model."""

    list_display = [
        "short_id",
        "qraft_task_link",
        "hook_type",
        "hook_path",
        "q2_task_id",
        "date_created",
    ]
    list_filter = ["hook_type", "date_created"]
    search_fields = ["id", "q2_task_id", "hook_path", "qraft_task__id"]
    readonly_fields = [
        "id",
        "qraft_task",
        "hook_type",
        "hook_path",
        "q2_task_id",
        "date_created",
    ]
    ordering = ["-date_created"]


# ── Workflow Models ──────────────────────────────────────────


class QraftChainStepInline(_ReadOnly, admin.TabularInline):
    """Inline display of chain steps."""

    model = QraftChainStep
    extra = 0
    readonly_fields = [
        "id",
        "step_index",
        "func",
        "task_args",
        "task_kwargs",
        "qraft_options",
        "qraft_task",
    ]
    ordering = ["step_index"]


@admin.register(QraftChainModel)
class QraftChainModelAdmin(_WorkflowAdmin, admin.ModelAdmin):
    """Admin for QraftChainModel."""

    list_display = [
        "short_id",
        "status_display",
        "subject_type",
        "subject_id",
        "step_count",
        "current_step_index",
        "hooks_display",
        "date_created",
        "date_updated",
    ]
    list_filter = ["status", "subject_type"]
    search_fields = ["id", "subject_type", "subject_id"]
    readonly_fields = [
        "id",
        "status",
        "settled_at",
        "subject_type",
        "subject_id",
        "current_step_index",
        "success_hook",
        "success_args",
        "success_kwargs",
        "failure_hook",
        "failure_args",
        "failure_kwargs",
        "on_cancelled",
        "progress_hook",
        "date_created",
        "date_updated",
    ]
    inlines = [QraftChainStepInline]
    ordering = ["-date_created"]

    def get_queryset(self, request):
        qs = super().get_queryset(request)
        return qs.annotate(_step_count=Count("steps"))

    def step_count(self, obj):
        return getattr(obj, "_step_count", obj.steps.count())

    step_count.short_description = "Steps"
    step_count.admin_order_field = "_step_count"


@admin.register(QraftIterModel)
class QraftIterModelAdmin(_ParallelWorkflowAdmin, admin.ModelAdmin):
    """Admin for QraftIterModel."""

    list_display = [
        "short_id",
        "status_display",
        "func",
        "subject_type",
        "subject_id",
        "counters_display",
        "hooks_display",
        "date_created",
        "date_updated",
    ]
    list_filter = ["status", "subject_type"]
    search_fields = ["id", "func", "subject_type", "subject_id"]
    readonly_fields = [
        "id",
        "status",
        "settled_at",
        "subject_type",
        "subject_id",
        "func",
        "default_qraft_options",
        "total_count",
        "completed_count",
        "success_count",
        "failure_count",
        "success_hook",
        "success_args",
        "success_kwargs",
        "failure_hook",
        "failure_args",
        "failure_kwargs",
        "on_cancelled",
        "progress_hook",
        "date_created",
        "date_updated",
    ]
    ordering = ["-date_created"]


@admin.register(QraftBatchModel)
class QraftBatchModelAdmin(_ParallelWorkflowAdmin, admin.ModelAdmin):
    """Admin for QraftBatchModel."""

    list_display = [
        "short_id",
        "status_display",
        "subject_type",
        "subject_id",
        "counters_display",
        "hooks_display",
        "date_created",
        "date_updated",
    ]
    list_filter = ["status", "subject_type"]
    search_fields = ["id", "subject_type", "subject_id"]
    readonly_fields = [
        "id",
        "status",
        "settled_at",
        "subject_type",
        "subject_id",
        "total_count",
        "completed_count",
        "success_count",
        "failure_count",
        "success_hook",
        "success_args",
        "success_kwargs",
        "failure_hook",
        "failure_args",
        "failure_kwargs",
        "on_cancelled",
        "progress_hook",
        "date_created",
        "date_updated",
    ]
    ordering = ["-date_created"]


@admin.register(WorkflowHookDispatch)
class WorkflowHookDispatchAdmin(_ShortIdAdmin, admin.ModelAdmin):
    """Admin for WorkflowHookDispatch model."""

    list_display = [
        "short_id",
        "workflow_type",
        "workflow_id_short",
        "hook_type",
        "hook_path",
        "date_created",
    ]
    list_filter = ["workflow_type", "hook_type"]
    search_fields = ["id", "workflow_id", "hook_path", "q2_task_id"]
    readonly_fields = [
        "id",
        "workflow_type",
        "workflow_id",
        "hook_type",
        "hook_path",
        "q2_task_id",
        "date_created",
    ]
    ordering = ["-date_created"]

    def workflow_id_short(self, obj):
        return _short_uuid(obj.workflow_id)

    workflow_id_short.short_description = "Workflow ID"


# ── Graphs ───────────────────────────────────────────────────


class QraftGraphNodeInline(_ReadOnly, admin.TabularInline):
    """Inline display of a graph's nodes."""

    model = QraftGraphNode
    extra = 0
    readonly_fields = [
        "position",
        "depth",
        "key",
        "status",
        "recovery",
        "task",
        "skip_reason",
        "dispatched_at",
        "settled_at",
    ]
    ordering = ["depth", "position"]


class QraftGraphMemberInline(_ReadOnly, admin.TabularInline):
    """Tasks correlated with this graph."""

    model = QraftTask
    fk_name = "graph"
    extra = 0
    fields = ["id", "node", "func", "status", "date_created"]
    readonly_fields = fields
    ordering = ["date_created"]
    verbose_name_plural = "Member tasks"


@admin.register(QraftGraph)
class QraftGraphAdmin(_ShortIdAdmin, admin.ModelAdmin):
    """Read-only admin for QraftGraph, with cancel as an action."""

    list_display = [
        "short_id",
        "status_display",
        "subject_type",
        "subject_id",
        "kind",
        "revision",
        "generation",
        "overdue_display",
        "date_started",
        "settled_at",
    ]
    list_filter = ["status", "subject_type", "kind", "date_created"]
    search_fields = ["id", "subject_type", "subject_id", "kind", "revision"]
    readonly_fields = [
        "id",
        "status",
        "generation",
        "cluster",
        "subject_type",
        "subject_id",
        "kind",
        "revision",
        "metadata",
        "previous_graph",
        "date_started",
        "settled_at",
        "resumed_at",
        "overdue_flagged_at",
        "on_settled",
        "on_settled_kwargs",
        "summary",
        "request_key",
        "plan_hash",
        "date_created",
        "date_updated",
    ]
    inlines = [QraftGraphNodeInline, QraftGraphMemberInline]
    ordering = ["-date_created"]
    actions = ["cancel_graphs"]

    def status_display(self, obj):
        return _colored_status(
            obj.status, obj.get_status_display(), _GRAPH_STATUS_COLORS
        )

    status_display.short_description = "Status"

    def overdue_display(self, obj):
        return "⚠" if obj.overdue_flagged_at else "-"

    overdue_display.short_description = "Overdue"

    @admin.action(description="Cancel selected graphs")
    def cancel_graphs(self, request, queryset):
        from qraft import graphs

        settled = skipped = 0
        for graph in queryset:
            try:
                graphs.cancel(graph.id)
            except graphs.GraphError:
                skipped += 1
            else:
                settled += 1
        self.message_user(
            request,
            f"Cancelled {settled} graph(s); skipped {skipped} already terminal.",
            level=messages.WARNING if skipped else messages.INFO,
        )
