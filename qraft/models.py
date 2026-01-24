from uuid import uuid4

from django.core.serializers.json import DjangoJSONEncoder
from django.db import models


class QraftTask(models.Model):
    """
    Extension model for Django-Q2 Task with enhanced Qraft functionality.

    Stores Qraft-specific metadata (hooks, retry policy). Individual execution
    attempts are tracked via QraftTaskAttempt, each linked to a Django-Q2 task.
    """

    id = models.UUIDField(primary_key=True, default=uuid4, editable=False)

    date_created = models.DateTimeField(auto_now_add=True)
    date_updated = models.DateTimeField(auto_now=True)

    class TaskStatus(models.TextChoices):
        """Status states for QraftTask lifecycle."""

        PENDING = "pending", "Pending"
        RUNNING = "running", "Running"
        SUCCEEDED = "succeeded", "Succeeded"
        FAILED = "failed", "Failed"
        EXHAUSTED = "exhausted", "Exhausted"  # All retries failed

    # Task status
    status = models.CharField(
        max_length=20,
        choices=TaskStatus.choices,
        default=TaskStatus.PENDING,
        db_index=True,
        help_text="Current task status",
    )

    # Task function metadata (stored for reference/debugging and retry re-queueing)
    func = models.CharField(
        max_length=256,
        help_text="Dotted path to the task function",
    )
    task_args = models.JSONField(
        default=list,
        blank=True,
        encoder=DjangoJSONEncoder,
        help_text="Task positional arguments",
    )
    task_kwargs = models.JSONField(
        default=dict,
        blank=True,
        encoder=DjangoJSONEncoder,
        help_text="Task keyword arguments",
    )

    # Success hook metadata
    success_hook = models.CharField(
        max_length=255,
        null=True,
        blank=True,
        help_text="Success hook function name",
    )
    success_args = models.JSONField(
        default=list,
        blank=True,
        encoder=DjangoJSONEncoder,
        help_text="Success hook arguments",
    )
    success_kwargs = models.JSONField(
        default=dict,
        blank=True,
        encoder=DjangoJSONEncoder,
        help_text="Success hook keyword arguments",
    )

    # Failure hook metadata
    failure_hook = models.CharField(
        max_length=256,
        null=True,
        blank=True,
        help_text="Failure hook function name",
    )
    failure_args = models.JSONField(
        default=list,
        blank=True,
        encoder=DjangoJSONEncoder,
        help_text="Failure hook arguments",
    )
    failure_kwargs = models.JSONField(
        default=dict,
        blank=True,
        encoder=DjangoJSONEncoder,
        help_text="Failure hook keyword arguments",
    )

    # Retry metadata
    retry_policy = models.JSONField(
        default=dict,
        blank=True,
        encoder=DjangoJSONEncoder,
        help_text="Retry policy configuration",
    )

    @property
    def attempt_count(self) -> int:
        """Return the number of attempts for this task."""
        return self.attempts.count()

    @property
    def latest_attempt(self):
        """Return the most recent attempt, or None if no attempts exist."""
        return self.attempts.order_by("-attempt_number").first()

    def __str__(self):
        return f"QraftTask {self.id} ({self.status})"

    class Meta:
        app_label = "qraft"
        verbose_name = "Qraft Task"
        verbose_name_plural = "Qraft Tasks"
        ordering = ["-date_created"]


class QraftTaskAttempt(models.Model):
    """
    Tracks each execution attempt of a QraftTask.

    Each attempt corresponds to a Django-Q2 task execution. This model provides
    an audit trail of all attempts, their outcomes, and exception information
    for debugging and retry decisions.
    """

    id = models.UUIDField(primary_key=True, default=uuid4, editable=False)

    qraft_task = models.ForeignKey(
        QraftTask,
        related_name="attempts",
        on_delete=models.CASCADE,
        help_text="Parent QraftTask this attempt belongs to",
    )

    attempt_number = models.PositiveSmallIntegerField(
        help_text="Attempt number (1-based)",
    )

    q2_task_id = models.CharField(
        max_length=32,
        unique=True,
        db_index=True,
        help_text="Django-Q2 task ID for this attempt",
    )

    # Outcome
    success = models.BooleanField(
        null=True,
        help_text="True if succeeded, False if failed, null if pending/running",
    )
    exception_class = models.CharField(
        max_length=256,
        null=True,
        blank=True,
        help_text="Exception class name if failed",
    )

    # Timing
    date_created = models.DateTimeField(auto_now_add=True)
    date_completed = models.DateTimeField(
        null=True,
        blank=True,
        help_text="When the attempt completed",
    )

    def __str__(self):
        status = (
            "pending" if self.success is None else ("ok" if self.success else "failed")
        )
        return f"Attempt {self.attempt_number} of {self.qraft_task_id} ({status})"

    def get_q2_task(self):
        """Retrieve the associated Django-Q2 Task if it exists."""
        from django_q.models import Task as Q2Task

        try:
            return Q2Task.objects.get(id=self.q2_task_id)
        except Q2Task.DoesNotExist:
            return None

    class Meta:
        app_label = "qraft"
        verbose_name = "Qraft Task Attempt"
        verbose_name_plural = "Qraft Task Attempts"
        ordering = ["attempt_number"]
        constraints = [
            models.UniqueConstraint(
                fields=["qraft_task", "attempt_number"],
                name="unique_attempt_per_task",
            )
        ]


class HookDispatch(models.Model):
    """
    Tracks hooks dispatched as async tasks.

    Links a QraftTask to the hook task that was queued for execution.
    The unique constraint on (qraft_task, hook_type) ensures hooks
    are only dispatched once per task, providing idempotency.
    """

    id = models.UUIDField(primary_key=True, default=uuid4, editable=False)

    qraft_task = models.ForeignKey(
        QraftTask,
        related_name="hook_dispatches",
        on_delete=models.CASCADE,
        help_text="QraftTask that triggered this hook",
    )

    hook_type = models.CharField(
        max_length=10,
        help_text="Type of hook: 'success' or 'failure'",
    )

    hook_path = models.CharField(
        max_length=256,
        help_text="Dotted path to hook function",
    )

    q2_task_id = models.CharField(
        max_length=32,
        unique=True,
        help_text="Django-Q2 task ID of the queued hook",
    )

    date_created = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"HookDispatch {self.hook_type} for {self.qraft_task_id}"

    def get_q2_task(self):
        """Retrieve the associated Django-Q2 Task if it exists."""
        from django_q.models import Task as Q2Task

        if not self.q2_task_id:
            return None

        try:
            return Q2Task.objects.get(id=self.q2_task_id)
        except Q2Task.DoesNotExist:
            return None

    class Meta:
        app_label = "qraft"
        verbose_name = "Hook Dispatch"
        verbose_name_plural = "Hook Dispatches"
        ordering = ["-date_created"]
        constraints = [
            models.UniqueConstraint(
                fields=["qraft_task", "hook_type"],
                name="unique_hook_dispatch_per_task_type",
            )
        ]


# Module-level export for convenience
TaskStatus = QraftTask.TaskStatus
