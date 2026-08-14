"""
Demo-side bookkeeping.

Scenarios assert on what really happened inside worker processes, which are
separate OS processes from the one running the assertions. The database is the
only channel they share, so tasks and hooks write an `Event` row for every
observable thing they do and the scenario reads those rows back.
"""

from uuid import uuid4

from django.db import models


class Event(models.Model):
    """One thing that happened inside a worker, hook, or task function."""

    TASK = "task"
    HOOK = "hook"
    STEP = "step"
    NOTE = "note"

    run = models.CharField(max_length=40, db_index=True)
    kind = models.CharField(max_length=16)
    name = models.CharField(max_length=120)
    payload = models.JSONField(default=dict, blank=True)
    created_at = models.DateTimeField(auto_now_add=True, db_index=True)

    def __str__(self):
        return f"{self.run}/{self.kind}:{self.name}"

    class Meta:
        # `id` breaks ties: two events in the same microsecond still order by
        # insertion, which is what the chain and priority scenarios read.
        ordering = ["created_at", "id"]
        indexes = [models.Index(fields=["run", "kind", "name"])]


class Control(models.Model):
    """
    A switch a scenario sets and a task function reads.

    Task functions run in another process, so "fail the first two times" or
    "stop failing now" has to be state both sides can see.
    """

    key = models.CharField(max_length=120, unique=True)
    # Bumped with an F() expression, so several workers can count attempts of
    # the same task without losing an increment.
    counter = models.IntegerField(default=0)
    value = models.JSONField(default=dict, blank=True)

    def __str__(self):
        return f"{self.key}={self.counter}/{self.value}"


class ScenarioRun(models.Model):
    """A single execution of one scenario, as the dashboard sees it."""

    RUNNING = "running"
    PASSED = "passed"
    FAILED = "failed"
    ERROR = "error"
    SKIPPED = "skipped"

    id = models.UUIDField(primary_key=True, default=uuid4, editable=False)
    key = models.CharField(max_length=60, db_index=True)
    # Same id as this run's Ctx.run, which every task/event it creates carries
    # as task_args[0]. That's what lets the dashboard trace a task back to the
    # scenario run that queued it.
    run = models.CharField(max_length=40, blank=True, default="", db_index=True)
    group = models.CharField(max_length=40)
    title = models.CharField(max_length=200, blank=True)
    status = models.CharField(max_length=12, default=RUNNING, db_index=True)
    checks = models.JSONField(default=list, blank=True)
    notes = models.JSONField(default=list, blank=True)
    error = models.TextField(blank=True)
    duration_ms = models.IntegerField(null=True, blank=True)
    started_at = models.DateTimeField(auto_now_add=True)
    finished_at = models.DateTimeField(null=True, blank=True)

    @property
    def passed_checks(self) -> int:
        return sum(1 for check in self.checks if check.get("ok"))

    def __str__(self):
        return f"{self.key} ({self.status})"

    class Meta:
        ordering = ["-started_at"]
