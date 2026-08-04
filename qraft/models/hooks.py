"""Hook dispatch tracking models for idempotency."""

from uuid import uuid4

from django.db import models


class HookDispatch(models.Model):
    """
    Tracks hooks dispatched as async tasks for QraftTask completion.

    Links a QraftTask to the hook task that was queued for execution.
    The unique constraint on (qraft_task, hook_type) ensures hooks
    are only dispatched once per task, providing idempotency.
    """

    id = models.UUIDField(primary_key=True, default=uuid4, editable=False)

    qraft_task = models.ForeignKey(
        "QraftTask",
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


class WorkflowHookDispatch(models.Model):
    """
    Tracks workflow-level hooks dispatched as async tasks.

    Ensures workflow hooks (chain/iter/batch completion) are only dispatched
    once per workflow, providing idempotency at the workflow level.
    """

    id = models.UUIDField(primary_key=True, default=uuid4, editable=False)

    workflow_type = models.CharField(
        max_length=10,
        help_text="Type of workflow: 'chain', 'iter', or 'batch'",
    )

    workflow_id = models.UUIDField(
        db_index=True,
        help_text="UUID of the workflow (chain/iter/batch)",
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
        return (
            f"WorkflowHookDispatch {self.hook_type}"
            f" for {self.workflow_type} {self.workflow_id}"
        )

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
        verbose_name = "Workflow Hook Dispatch"
        verbose_name_plural = "Workflow Hook Dispatches"
        ordering = ["-date_created"]
        constraints = [
            models.UniqueConstraint(
                fields=["workflow_type", "workflow_id", "hook_type"],
                name="unique_workflow_hook_dispatch",
            )
        ]
