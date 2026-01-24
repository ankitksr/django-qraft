"""
Django admin configuration for Qraft models.
"""

from django.contrib import admin
from django.utils.html import format_html

from .models import HookDispatch, QraftTask, QraftTaskAttempt


def _short_uuid(uuid_val):
    """Return shortened UUID for display."""
    return str(uuid_val)[:8] if uuid_val else "-"


class QraftTaskAttemptInline(admin.TabularInline):
    """Inline display of task attempts within QraftTask admin."""

    model = QraftTaskAttempt
    extra = 0
    readonly_fields = [
        "id",
        "attempt_number",
        "q2_task_id",
        "success",
        "exception_class",
        "date_created",
        "date_completed",
    ]
    ordering = ["attempt_number"]

    def has_add_permission(self, request, obj=None):
        return False

    def has_delete_permission(self, request, obj=None):
        return False


class HookDispatchInline(admin.TabularInline):
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

    def has_add_permission(self, request, obj=None):
        return False

    def has_delete_permission(self, request, obj=None):
        return False


@admin.register(QraftTask)
class QraftTaskAdmin(admin.ModelAdmin):
    """Admin for QraftTask model."""

    list_display = [
        "short_id",
        "status_display",
        "func",
        "attempt_count",
        "date_created",
        "date_updated",
    ]
    list_filter = ["status", "date_created"]
    search_fields = ["id", "func"]
    readonly_fields = [
        "id",
        "date_created",
        "date_updated",
        "status",
        "func",
        "task_args",
        "task_kwargs",
        "success_hook",
        "success_args",
        "success_kwargs",
        "failure_hook",
        "failure_args",
        "failure_kwargs",
        "retry_policy",
    ]
    inlines = [QraftTaskAttemptInline, HookDispatchInline]
    ordering = ["-date_created"]

    fieldsets = [
        (None, {"fields": ["id", "status", "func", "date_created", "date_updated"]}),
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
    ]

    def short_id(self, obj):
        return _short_uuid(obj.id)

    short_id.short_description = "ID"

    def status_display(self, obj):
        colors = {
            "pending": "#6c757d",
            "running": "#007bff",
            "succeeded": "#28a745",
            "failed": "#dc3545",
            "exhausted": "#6f42c1",
        }
        color = colors.get(obj.status, "#6c757d")
        return format_html(
            '<span style="color: {}; font-weight: bold;">{}</span>',
            color,
            obj.get_status_display(),
        )

    status_display.short_description = "Status"

    def has_add_permission(self, request):
        return False

    def has_delete_permission(self, request, obj=None):
        return False


@admin.register(QraftTaskAttempt)
class QraftTaskAttemptAdmin(admin.ModelAdmin):
    """Admin for QraftTaskAttempt model."""

    list_display = [
        "short_id",
        "qraft_task_link",
        "attempt_number",
        "success_display",
        "exception_class",
        "date_created",
        "date_completed",
    ]
    list_filter = ["success", "date_created"]
    search_fields = ["id", "q2_task_id", "exception_class", "qraft_task__id"]
    readonly_fields = [
        "id",
        "qraft_task",
        "attempt_number",
        "q2_task_id",
        "success",
        "exception_class",
        "date_created",
        "date_completed",
    ]
    ordering = ["-date_created"]

    def short_id(self, obj):
        return _short_uuid(obj.id)

    short_id.short_description = "ID"

    def qraft_task_link(self, obj):
        from django.urls import reverse

        url = reverse("admin:qraft_qrafttask_change", args=[obj.qraft_task_id])
        return format_html('<a href="{}">{}</a>', url, _short_uuid(obj.qraft_task_id))

    qraft_task_link.short_description = "Qraft Task"

    def success_display(self, obj):
        if obj.success is None:
            return format_html('<span style="color: #6c757d;">Pending</span>')
        elif obj.success:
            return format_html('<span style="color: #28a745;">Success</span>')
        else:
            return format_html('<span style="color: #dc3545;">Failed</span>')

    success_display.short_description = "Outcome"

    def has_add_permission(self, request):
        return False

    def has_delete_permission(self, request, obj=None):
        return False


@admin.register(HookDispatch)
class HookDispatchAdmin(admin.ModelAdmin):
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

    def short_id(self, obj):
        return _short_uuid(obj.id)

    short_id.short_description = "ID"

    def qraft_task_link(self, obj):
        from django.urls import reverse

        url = reverse("admin:qraft_qrafttask_change", args=[obj.qraft_task_id])
        return format_html('<a href="{}">{}</a>', url, _short_uuid(obj.qraft_task_id))

    qraft_task_link.short_description = "Qraft Task"

    def has_add_permission(self, request):
        return False

    def has_delete_permission(self, request, obj=None):
        return False
