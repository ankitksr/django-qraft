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


class SubjectMixin(models.Model):
    """
    The domain entity a task or workflow is for, as a string pair.

    Strings rather than a generic foreign key: the subject may live in another
    database or another service, and a string pair is what every consumer can
    produce.
    """

    subject_type = models.CharField(
        max_length=100,
        null=True,
        blank=True,
        help_text="Kind of domain entity this work is for (e.g. 'worksheet')",
    )
    subject_id = models.CharField(
        max_length=255,
        null=True,
        blank=True,
        help_text="Identifier of the domain entity, as a string",
    )

    class Meta:
        abstract = True


class GraphMemberMixin(models.Model):
    """
    The graph and node key a task or workflow is correlated with.

    `SET_NULL`, never `CASCADE`: deleting a graph must never delete work. A
    node's own task also carries `graph_node` (CASCADE) for membership.
    """

    graph = models.ForeignKey(
        "QraftGraph",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="%(class)s_members",
        help_text="Graph this work is correlated with",
    )
    node = models.CharField(
        max_length=100,
        null=True,
        blank=True,
        help_text="Node key within the graph, for correlation",
    )

    class Meta:
        abstract = True


class WorkflowStatusMixin(models.Model):
    """Mixin providing status field with state machine validation."""

    status = models.CharField(
        max_length=20,
        choices=WorkflowStatus.choices,
        default=WorkflowStatus.PENDING,
        db_index=True,
    )

    # Transition identity for settlement. Status alone cannot tell a first
    # settlement from a replayed one (a final-step redelivery, an
    # already-terminal _complete_chain, a cancel racing a completion) and a
    # resumed chain settles twice legitimately; the conditional update on this
    # column is what fires workflow_settled and the workflow hook exactly once
    # per settlement.
    settled_at = models.DateTimeField(
        null=True,
        blank=True,
        help_text="When the workflow last settled (settlement compare-and-swap column)",
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

    # db_default: a rolling deploy's previous release still inserts rows
    # without this column (same reasoning as QraftTaskAttempt.state).
    hook_context = models.BooleanField(
        default=False,
        db_default=False,
        help_text="Whether workflow hooks receive a `context` keyword argument",
    )

    class Meta:
        abstract = True
