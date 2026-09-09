"""Workflow models for chain, iter, and batch orchestration."""

from uuid import uuid4

from django.core.serializers.json import DjangoJSONEncoder
from django.db import models

from .mixins import (
    RunMemberMixin,
    SubjectMixin,
    WorkflowHookMixin,
    WorkflowStatus,
    WorkflowStatusMixin,
)


class QraftChainModel(
    SubjectMixin, RunMemberMixin, WorkflowHookMixin, WorkflowStatusMixin, models.Model
):
    """Database model for chain workflow state (sequential execution)."""

    id = models.UUIDField(primary_key=True, default=uuid4, editable=False)

    status = models.CharField(
        max_length=20,
        choices=WorkflowStatus.choices,
        default=WorkflowStatus.PENDING,
        db_index=True,
        help_text="Current chain status",
    )

    current_step_index = models.PositiveSmallIntegerField(
        default=0,
        help_text="Index of the currently executing step",
    )

    date_created = models.DateTimeField(auto_now_add=True)
    date_updated = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"QraftChain {self.id} ({self.status})"

    class Meta:
        app_label = "qraft"
        verbose_name = "Qraft Chain"
        verbose_name_plural = "Qraft Chains"
        ordering = ["-date_created"]


class QraftChainStep(models.Model):
    """Individual step within a chain, created before execution."""

    id = models.UUIDField(primary_key=True, default=uuid4, editable=False)

    chain = models.ForeignKey(
        QraftChainModel,
        related_name="steps",
        on_delete=models.CASCADE,
        help_text="Parent chain this step belongs to",
    )

    step_index = models.PositiveSmallIntegerField(
        help_text="Zero-based index of this step in the chain",
    )

    # Step definition (stored before execution)
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
    qraft_options = models.JSONField(
        default=dict,
        blank=True,
        encoder=DjangoJSONEncoder,
        help_text="Qraft-specific options (retry policy, etc.)",
    )

    requires_approval = models.BooleanField(
        default=False,
        help_text="Chain parks in WAITING_APPROVAL before running this step",
    )

    # Link to QraftTask once step is queued (null until run)
    qraft_task = models.OneToOneField(
        "QraftTask",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="chain_step",
        help_text="QraftTask instance once step is queued",
    )

    def __str__(self):
        return f"Step {self.step_index} of Chain {self.chain_id}"

    class Meta:
        app_label = "qraft"
        verbose_name = "Qraft Chain Step"
        verbose_name_plural = "Qraft Chain Steps"
        ordering = ["step_index"]
        constraints = [
            models.UniqueConstraint(
                fields=["chain", "step_index"],
                name="unique_chain_step",
            )
        ]


class QraftIterModel(
    SubjectMixin, RunMemberMixin, WorkflowHookMixin, WorkflowStatusMixin, models.Model
):
    """Database model for iter workflow state (same function, many inputs)."""

    id = models.UUIDField(primary_key=True, default=uuid4, editable=False)

    status = models.CharField(
        max_length=20,
        choices=WorkflowStatus.choices,
        default=WorkflowStatus.PENDING,
        db_index=True,
        help_text="Current iter status",
    )

    # Function to call for each item
    func = models.CharField(
        max_length=256,
        help_text="Dotted path to the task function",
    )
    default_qraft_options = models.JSONField(
        default=dict,
        blank=True,
        encoder=DjangoJSONEncoder,
        help_text="Default Qraft options for all tasks",
    )

    # Counters for atomic completion tracking
    total_count = models.PositiveIntegerField(
        default=0,
        help_text="Total number of tasks in this iter",
    )
    completed_count = models.PositiveIntegerField(
        default=0,
        help_text="Number of tasks completed (success or exhausted)",
    )
    success_count = models.PositiveIntegerField(
        default=0,
        help_text="Number of tasks that succeeded",
    )
    failure_count = models.PositiveIntegerField(
        default=0,
        help_text="Number of tasks that failed (exhausted)",
    )

    date_created = models.DateTimeField(auto_now_add=True)
    date_updated = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"QraftIter {self.id} ({self.completed_count}/{self.total_count})"

    class Meta:
        app_label = "qraft"
        verbose_name = "Qraft Iter"
        verbose_name_plural = "Qraft Iters"
        ordering = ["-date_created"]


class QraftBatchModel(
    SubjectMixin, RunMemberMixin, WorkflowHookMixin, WorkflowStatusMixin, models.Model
):
    """Database model for batch workflow state (different functions, parallel)."""

    id = models.UUIDField(primary_key=True, default=uuid4, editable=False)

    status = models.CharField(
        max_length=20,
        choices=WorkflowStatus.choices,
        default=WorkflowStatus.PENDING,
        db_index=True,
        help_text="Current batch status",
    )

    # Counters for atomic completion tracking
    total_count = models.PositiveIntegerField(
        default=0,
        help_text="Total number of tasks in this batch",
    )
    completed_count = models.PositiveIntegerField(
        default=0,
        help_text="Number of tasks completed (success or exhausted)",
    )
    success_count = models.PositiveIntegerField(
        default=0,
        help_text="Number of tasks that succeeded",
    )
    failure_count = models.PositiveIntegerField(
        default=0,
        help_text="Number of tasks that failed (exhausted)",
    )

    date_created = models.DateTimeField(auto_now_add=True)
    date_updated = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"QraftBatch {self.id} ({self.completed_count}/{self.total_count})"

    class Meta:
        app_label = "qraft"
        verbose_name = "Qraft Batch"
        verbose_name_plural = "Qraft Batches"
        ordering = ["-date_created"]
