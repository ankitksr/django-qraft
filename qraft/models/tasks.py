"""Core task models for Django-Qraft."""

from uuid import uuid4

from django.core.serializers.json import DjangoJSONEncoder
from django.db import models

from .mixins import GraphMemberMixin, SubjectMixin, get_q2_task


class QraftTaskQuerySet(models.QuerySet):
    def for_subject(self, subject_type: str, subject_id) -> "QraftTaskQuerySet":
        """Every task for one domain entity, newest first."""
        return self.filter(
            subject_type=subject_type, subject_id=str(subject_id)
        ).order_by("-date_created")


class QraftTask(SubjectMixin, GraphMemberMixin, models.Model):
    """
    Extension model for Django-Q2 Task with enhanced Qraft functionality.

    Stores Qraft-specific metadata (hooks, retry policy). Individual execution
    attempts are tracked via QraftTaskAttempt, each linked to a Django-Q2 task.
    """

    objects = QraftTaskQuerySet.as_manager()

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

    # db_default: a rolling deploy's previous release still inserts rows
    # without this column (same reasoning as QraftTaskAttempt.state).
    hook_context = models.BooleanField(
        default=False,
        db_default=False,
        help_text="Whether success/failure hooks receive a `context` keyword argument",
    )

    # Retry metadata
    retry_policy = models.JSONField(
        default=dict,
        blank=True,
        encoder=DjangoJSONEncoder,
        help_text="Retry policy configuration",
    )

    # A column rather than a retry_policy key: RetryPolicy.from_options copies
    # a whitelist and would drop an unknown key silently, and stalling is not
    # retry semantics. It coexists with the heartbeat grace rather than
    # competing with it - a task can heartbeat every 30s while advancing
    # nothing for 60.
    stall_after = models.PositiveIntegerField(
        null=True,
        blank=True,
        help_text="Seconds without progress advancing before an attempt is "
        "flagged as a suspected stall; null disables the check",
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

    # Denormalised snapshot of the latest attempt's progress, for dashboard
    # task rows. The attempt owns progress (see QraftTaskAttempt.progress);
    # the writer stamps its `attempt_id` here and only the task's latest
    # attempt may overwrite it.
    progress = models.JSONField(
        null=True,
        blank=True,
        encoder=DjangoJSONEncoder,
        help_text="Snapshot of the latest attempt's progress (current/total/message)",
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

    graph_node = models.ForeignKey(
        "QraftGraphNode",
        null=True,
        blank=True,
        on_delete=models.CASCADE,
        related_name="tasks",
        help_text="The node this task is the current execution of",
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
        indexes = [
            models.Index(
                fields=["subject_type", "subject_id"], name="qraft_task_subject_idx"
            ),
        ]


class QraftTaskAttempt(models.Model):
    """
    Tracks each execution attempt of a QraftTask.

    Each attempt corresponds to a Django-Q2 task execution. This model provides
    an audit trail of all attempts, their outcomes, and exception information
    for debugging and retry decisions.

    An attempt is created either at enqueue time (`async_task()`, state QUEUED)
    or ahead of it (`qraft.scheduler.schedule_attempt()`, state SCHEDULED with
    a `not_before` due time). The row therefore always exists before the
    attempt runs, which is what lets every completion resolve through the one
    `q2_task_id` lookup.
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
        null=True,
        blank=True,
        help_text="Django-Q2 task ID for this attempt; null until a dispatcher "
        "enqueues a SCHEDULED attempt",
    )

    class AttemptState(models.TextChoices):
        """Where an attempt sits relative to the broker."""

        SCHEDULED = "scheduled", "Scheduled"  # due at not_before, not yet queued
        QUEUED = "queued", "Queued"  # handed to the broker

    state = models.CharField(
        max_length=16,
        choices=AttemptState.choices,
        default=AttemptState.QUEUED,
        # A real database default, not just a Python one: during a rolling
        # upgrade the previous release is still inserting attempt rows without
        # this column, and Django drops the temporary default it uses to add
        # it. Without db_default those inserts fail the NOT NULL constraint.
        # "queued" is right for them - before this field existed, an attempt
        # row was only ever created at enqueue time.
        db_default=AttemptState.QUEUED,
        help_text="Whether this attempt is still waiting for the dispatcher",
    )

    not_before = models.DateTimeField(
        null=True,
        blank=True,
        help_text="Earliest time a dispatcher may enqueue this attempt",
    )
    claimed_at = models.DateTimeField(
        null=True,
        blank=True,
        help_text="When a dispatcher claimed this attempt and enqueued it",
    )

    cluster = models.CharField(
        max_length=150,
        null=True,
        blank=True,
        help_text="Requested routing target; null means whichever dispatcher "
        "claims it. Also the executor, except when an explicit broker "
        "override bypasses routing",
    )

    # Enqueue override for callers whose worker-side entry point is not the
    # task's own func (currently only the django.tasks TaskContext wrapper).
    dispatch_func = models.CharField(
        max_length=256,
        null=True,
        blank=True,
        help_text="Dotted path to enqueue instead of qraft.runner.run_task",
    )
    dispatch_args = models.JSONField(
        null=True,
        blank=True,
        encoder=DjangoJSONEncoder,
        help_text="Positional arguments for dispatch_func",
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

    # How many deliveries of this attempt a worker has begun. Django-Q2
    # redelivers an unacknowledged message after a monitor crash, and every
    # re-run refreshed the lease heartbeat, so the reaper's liveness test
    # answered "alive" for a task stuck in a redelivery loop. The counter is
    # the compare-and-swap that stops the second run (see qraft.lease
    # .claim_delivery and `max_executions_per_attempt`).
    execution_count = models.PositiveIntegerField(
        default=0,
        db_default=0,
        help_text="Deliveries of this attempt a worker has begun executing",
    )

    # Dispatcher idempotency flag (prevents double-counting in parallel workflows)
    counted = models.BooleanField(
        default=False,
        help_text="Whether this attempt has been counted by a parallel dispatcher",
    )

    # Resolution commits before workflow routing and hook dispatch run; this
    # flag is what lets the reaper find a resolution whose post-commit work
    # died (monitor crash) and replay it, instead of leaving the workflow
    # wedged. True also for resolutions with no post-commit work (a scheduled
    # retry).
    routed = models.BooleanField(
        default=False,
        db_default=False,
        help_text="Whether post-resolution routing and hook dispatch completed",
    )

    usage = models.JSONField(
        null=True,
        blank=True,
        encoder=DjangoJSONEncoder,
        help_text="Task-reported usage (model, input_tokens, output_tokens, cost)",
    )

    # Progress belongs to the attempt that produced it: a retry starts from
    # zero rather than inheriting its predecessor's 90%, and a still-running
    # superseded attempt cannot overwrite the current one's numbers.
    progress = models.JSONField(
        null=True,
        blank=True,
        encoder=DjangoJSONEncoder,
        help_text="Attempt-reported progress payload (current/total/message)",
    )
    progress_reported_at = models.DateTimeField(
        null=True,
        blank=True,
        help_text="Last report_progress() call from this attempt",
    )
    # Moves only when current/total change, so a task narrating its own hang
    # cannot defeat stall detection by reporting often.
    progress_advanced_at = models.DateTimeField(
        null=True,
        blank=True,
        help_text="Last time current/total changed",
    )

    # Observation only. The attempt keeps running: nothing here resolves it,
    # schedules a retry, or stops the worker - a retry starting while attempt 1
    # is still writing would produce two attempts writing the same rows.
    stall_suspected_at = models.DateTimeField(
        null=True,
        blank=True,
        help_text="When the reaper first flagged this attempt as a suspected stall",
    )

    trace_context = models.CharField(
        max_length=128,
        null=True,
        blank=True,
        help_text="W3C traceparent captured at enqueue, when a span was current",
    )

    # Timing
    date_created = models.DateTimeField(auto_now_add=True)
    # Distinct from date_created (a scheduled attempt's row is created long
    # before it is queued) and from claimed_at (attempt 1 is never claimed):
    # this is the moment the broker received the attempt, which is what queue
    # wait is measured from.
    enqueued_at = models.DateTimeField(
        null=True,
        blank=True,
        help_text="When the attempt was handed to the broker",
    )
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
    returned_at = models.DateTimeField(
        null=True,
        blank=True,
        help_text=(
            "When the target function returned or raised, stamped by "
            "qraft.runner.run_task inside the worker. Set with success still "
            "NULL means the work ran and its result never reached the monitor"
        ),
    )
    date_completed = models.DateTimeField(
        null=True,
        blank=True,
        help_text="When the attempt completed",
    )

    output_committed_at = models.DateTimeField(
        null=True,
        blank=True,
        help_text="When a transactional node published its receipt (phase 3)",
    )

    # Worker identity, stamped from inside the executing process at
    # pre_execute time (see qraft.lease.stamp_start). `cluster` above already
    # carries the routing target, which is also the cluster that actually ran
    # the attempt in every normal path (a named lane is only drained by the
    # worker process bound to it) - these two fields add the pid and, for
    # threaded workers, which pool thread within that process.
    worker_pid = models.IntegerField(
        null=True,
        blank=True,
        help_text="OS pid of the worker process that executed this attempt",
    )
    worker_thread = models.CharField(
        max_length=64,
        null=True,
        blank=True,
        help_text="Executor thread name within the worker process; blank for "
        "standard (non-threaded) workers",
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
        indexes = [
            # The dispatcher's only query: due SCHEDULED attempts, oldest first.
            models.Index(fields=["state", "not_before"], name="qraft_attempt_due_idx"),
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
AttemptState = QraftTaskAttempt.AttemptState
