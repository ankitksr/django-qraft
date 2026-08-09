"""
Admin for the demo's own bookkeeping tables.

qraft's own models are already registered by `qraft.admin`; the dashboard
links there instead of duplicating it.
"""

from django.contrib import admin

from showcase.models import Control, Event, ScenarioRun


@admin.register(Event)
class EventAdmin(admin.ModelAdmin):
    list_display = ("created_at", "run", "kind", "name")
    list_filter = ("kind",)
    search_fields = ("run", "name")


@admin.register(ScenarioRun)
class ScenarioRunAdmin(admin.ModelAdmin):
    list_display = ("key", "status", "duration_ms", "started_at")
    list_filter = ("status", "group")


@admin.register(Control)
class ControlAdmin(admin.ModelAdmin):
    list_display = ("key", "counter")
