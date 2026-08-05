"""Core task models for Django-Qraft."""

from uuid import uuid4

from django.core.serializers.json import DjangoJSONEncoder
from django.db import models

from .mixins import get_q2_task


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

    idempotency_key = models.CharField(
        max_length=255,
        null=True,
        blank=True,
        unique=True,
        help_text="Caller-supplied key; re-enqueueing the same key is a no-op",
    )

    class TaskPriority(models.TextChoices):
        HIGH = "high", "High"
        DEFAULT = "default", "Default"
        LOW = "low", "Low"

    priority = models.CharField(
        max_length=10,
        choices=TaskPriority.choices,
        default=TaskPriority.DEFAULT,
        db_index=True,
        help_text="Priority lane within the consuming cluster",
    )

    progress = models.JSONField(
        null=True,
        blank=True,
        encoder=DjangoJSONEncoder,
        help_text="Task-reported progress payload (current/total/message)",
    )

    # Workflow linkage (only one will be set, if any)
    qraft_iter = models.ForeignKey(
        "QraftIterModel",
        null=True,
        blank=True,
        on_delete=models.CASCADE,
        related_name="tasks",
        help_text="Parent QraftIter workflow (if part of iter)",
    )
    qraft_batch = models.ForeignKey(
        "QraftBatchModel",
        null=True,
        blank=True,
        on_delete=models.CASCADE,
        related_name="tasks",
        help_text="Parent QraftBatch workflow (if part of batch)",
    )
    # Chain linkage is via QraftChainStep.qraft_task OneToOne
    # (reverse: task.chain_step)

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

    # Dispatcher idempotency flag (prevents double-counting in parallel workflows)
    counted = models.BooleanField(
        default=False,
        help_text="Whether this attempt has been counted by a parallel dispatcher",
    )

    usage = models.JSONField(
        null=True,
        blank=True,
        encoder=DjangoJSONEncoder,
        help_text="Task-reported usage (model, input_tokens, output_tokens, cost)",
    )

    # Timing
    date_created = models.DateTimeField(auto_now_add=True)
    date_started = models.DateTimeField(
        null=True,
        blank=True,
        help_text="When a worker began executing this attempt",
    )
    heartbeat_at = models.DateTimeField(
        null=True,
        blank=True,
        db_index=True,
        help_text="Last execution-lease heartbeat from the running worker",
    )
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
        return get_q2_task(self.q2_task_id)

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


class RateBucket(models.Model):
    """
    DB-coordinated token bucket for cross-worker backpressure.

    One row per throttle key (provider, tenant, ...). Workers refill and drain
    the bucket under a row lock, so N workers share one rate limit instead of
    each enforcing its own.
    """

    key = models.CharField(max_length=255, unique=True)
    tokens = models.FloatField(default=0.0)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"RateBucket {self.key} ({self.tokens:.1f})"

    class Meta:
        app_label = "qraft"
        verbose_name = "Rate Bucket"
        verbose_name_plural = "Rate Buckets"


# Module-level export for convenience
TaskStatus = QraftTask.TaskStatus
TaskPriority = QraftTask.TaskPriority
