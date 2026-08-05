"""Shared mixins, enums, and base fields for workflow models."""

from django.core.serializers.json import DjangoJSONEncoder
from django.db import models


def get_q2_task(q2_task_id):
    """Return the Django-Q2 Task with this id, or None if it's gone."""
    from django_q.models import Task as Q2Task

    if not q2_task_id:
        return None

    try:
        return Q2Task.objects.get(id=q2_task_id)
    except Q2Task.DoesNotExist:
        return None


class WorkflowStatus(models.TextChoices):
    """Shared status enum for all workflow types."""

    PENDING = "pending", "Pending"
    RUNNING = "running", "Running"
    WAITING_APPROVAL = "waiting_approval", "Waiting for approval"
    SUCCEEDED = "succeeded", "Succeeded"
    FAILED = "failed", "Failed"
    CANCELLED = "cancelled", "Cancelled"


# Valid state transitions for workflow status
VALID_TRANSITIONS = {
    WorkflowStatus.PENDING: {WorkflowStatus.RUNNING, WorkflowStatus.CANCELLED},
    WorkflowStatus.RUNNING: {
        WorkflowStatus.WAITING_APPROVAL,
        WorkflowStatus.SUCCEEDED,
        WorkflowStatus.FAILED,
        WorkflowStatus.CANCELLED,
    },
    WorkflowStatus.WAITING_APPROVAL: {
        WorkflowStatus.RUNNING,  # approve
        WorkflowStatus.CANCELLED,  # reject
    },
    WorkflowStatus.FAILED: {WorkflowStatus.RUNNING},  # resume
    WorkflowStatus.SUCCEEDED: set(),  # terminal
    WorkflowStatus.CANCELLED: set(),  # terminal
}


class InvalidStatusTransition(ValueError):
    """Raised when an invalid workflow status transition is attempted."""


class WorkflowStatusMixin(models.Model):
    """Mixin providing status field with state machine validation."""

    status = models.CharField(
        max_length=20,
        choices=WorkflowStatus.choices,
        default=WorkflowStatus.PENDING,
        db_index=True,
    )

    def transition_to(self, new_status: str):
        """
        Validate and set a new status.

        Args:
            new_status: The target WorkflowStatus value

        Raises:
            InvalidStatusTransition: If the transition is not valid
        """
        allowed = VALID_TRANSITIONS.get(self.status, set())
        if new_status not in allowed:
            raise InvalidStatusTransition(
                f"Cannot transition from {self.status} to {new_status}"
            )
        self.status = new_status

    class Meta:
        abstract = True


class WorkflowHookMixin(models.Model):
    """Mixin providing dual-phase hook fields for workflows."""

    # Success hook metadata
    success_hook = models.CharField(
        max_length=256,
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

    # Cancellation hook
    on_cancelled = models.CharField(
        max_length=256,
        null=True,
        blank=True,
        help_text="Hook called when workflow is cancelled",
    )

    # Progress hook
    progress_hook = models.CharField(
        max_length=256,
        null=True,
        blank=True,
        help_text="Hook called on each task completion (parallel workflows only)",
    )

    class Meta:
        abstract = True
