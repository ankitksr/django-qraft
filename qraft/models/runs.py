"""Run and stage models: one row spanning a pipeline whose stages enqueue each other."""

from uuid import uuid4

from django.core.serializers.json import DjangoJSONEncoder
from django.db import models
from django.utils import timezone

from .mixins import SubjectMixin


class RunStatus(models.TextChoices):
    """Where a run sits. Everything but OPEN is terminal and never mutated."""

    OPEN = "open", "Open"
    SUCCEEDED = "succeeded", "Succeeded"
    FAILED = "failed", "Failed"
    CANCELLED = "cancelled", "Cancelled"
    ABANDONED = "abandoned", "Abandoned"


TERMINAL_RUN_STATUSES = (
    RunStatus.SUCCEEDED,
    RunStatus.FAILED,
    RunStatus.CANCELLED,
    RunStatus.ABANDONED,
)


class StageStatus(models.TextChoices):
    """Where a declared stage sits relative to its one completion unit."""

    PENDING = "pending", "Pending"  # declared, nothing bound
    BOUND = "bound", "Bound"  # a task or workflow owns it
    SUCCEEDED = "succeeded", "Succeeded"
    FAILED = "failed", "Failed"
    CANCELLED = "cancelled", "Cancelled"
    SKIPPED = "skipped", "Skipped"


SETTLED_STAGE_STATUSES = (
    StageStatus.SUCCEEDED,
    StageStatus.FAILED,
    StageStatus.CANCELLED,
    StageStatus.SKIPPED,
)


class UnitType(models.TextChoices):
    """What kind of row a stage is bound to."""

    TASK = "task", "Task"
    CHAIN = "chain", "Chain"
    ITER = "iter", "Iter"
    BATCH = "batch", "Batch"


class QraftRun(SubjectMixin, models.Model):
    """
    A pipeline over one subject, declaring the stages it expects.

    Lighter than a nested workflow: the application decides what to enqueue
    under each stage and when, and the run only records which unit owns which
    stage and how each ended. It settles by derivation over the declared
    stages, never by an explicit close call, because the code that knows the
    pipeline is done is the last stage's success path - the one place a crash
    loses the call.
    """

    id = models.UUIDField(primary_key=True, default=uuid4, editable=False)

    # A metric label, so its vocabulary must stay bounded by the application's
    # own (shadow/parity/simulation), unlike revision and metadata below.
    kind = models.CharField(
        max_length=50,
        null=True,
        blank=True,
        help_text="Execution fingerprint (e.g. 'shadow', 'parity'); a metric label",
    )
    revision = models.CharField(
        max_length=100,
        null=True,
        blank=True,
        help_text="Pipeline or input revision; never a metric label",
    )
    metadata = models.JSONField(
        null=True,
        blank=True,
        encoder=DjangoJSONEncoder,
        help_text="Application data; never a metric label",
    )

    previous_run = models.ForeignKey(
        "self",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="reruns",
        help_text="The run this one reruns",
    )

    status = models.CharField(
        max_length=16,
        choices=RunStatus.choices,
        default=RunStatus.OPEN,
        db_index=True,
        help_text="Current run status",
    )

    # Defaults to now, but the application may backdate it to the moment the
    # work became due (a report arriving), so report-to-ready includes the time
    # the first task spent queued.
    date_started = models.DateTimeField(
        default=timezone.now,
        help_text="When the work this run covers began; may be backdated",
    )
    settled_at = models.DateTimeField(
        null=True,
        blank=True,
        help_text="When the run settled (settlement compare-and-swap column)",
    )
    overdue_flagged_at = models.DateTimeField(
        null=True,
        blank=True,
        help_text="When the overdue sweep first flagged this run",
    )

    on_settled = models.CharField(
        max_length=256,
        null=True,
        blank=True,
        help_text="Dotted path to the durable completion hook",
    )
    on_settled_kwargs = models.JSONField(
        default=dict,
        blank=True,
        encoder=DjangoJSONEncoder,
        help_text="Keyword arguments for the completion hook",
    )

    # Key -> remaining allowance, decremented atomically by
    # qraft.context.consume_budget(). It lives on the run, not the attempt,
    # because the thing being bounded is what one pipeline may spend on a
    # provider across every stage and every retry of every stage.
    budgets = models.JSONField(
        null=True,
        blank=True,
        encoder=DjangoJSONEncoder,
        help_text="Remaining request allowance per budget key (whole counts)",
    )

    # Written in the settlement transaction so retention may later prune the
    # member rows without losing the answer.
    summary = models.JSONField(
        null=True,
        blank=True,
        encoder=DjangoJSONEncoder,
        help_text="Snapshot of stages, durations and usage, written at settlement",
    )

    date_created = models.DateTimeField(auto_now_add=True)
    date_updated = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"QraftRun {self.id} ({self.status})"

    def bind_subject(self, subject_type: str, subject_id) -> None:
        """
        Name this run's subject, once, while it is still OPEN.

        Thin wrapper over `qraft.runs.bind_subject()`, which is where the rule
        and the locking live; this refreshes the instance afterwards.
        """
        from qraft import runs

        runs.bind_subject(self.id, (subject_type, subject_id))
        self.refresh_from_db(fields=["subject_type", "subject_id", "date_updated"])

    class Meta:
        app_label = "qraft"
        verbose_name = "Qraft Run"
        verbose_name_plural = "Qraft Runs"
        ordering = ["-date_created"]
        indexes = [
            models.Index(
                fields=["subject_type", "subject_id"], name="qraft_run_subject_idx"
            ),
        ]


class QraftRunStage(models.Model):
    """
    One declared stage of a run, bound to exactly one completion unit.

    One unit, not "any successful task": a stage with three passes, one
    finished and two running, would otherwise settle, and a later failure
    could not reopen it. A stage that needs several tasks uses an Iter or a
    Batch, whose membership closes at run() and whose completion the parallel
    dispatcher already tracks.
    """

    id = models.UUIDField(primary_key=True, default=uuid4, editable=False)

    run = models.ForeignKey(
        QraftRun,
        related_name="stages",
        on_delete=models.CASCADE,
        help_text="Run that declared this stage",
    )

    name = models.CharField(max_length=100, help_text="Stage name, unique per run")
    position = models.PositiveSmallIntegerField(
        default=0,
        help_text="Declaration order, for display only; Qraft enforces no ordering",
    )

    unit_type = models.CharField(
        max_length=8,
        choices=UnitType.choices,
        null=True,
        blank=True,
        help_text="What kind of row owns this stage",
    )
    unit_id = models.UUIDField(
        null=True,
        blank=True,
        help_text="The bound task or workflow",
    )

    status = models.CharField(
        max_length=16,
        choices=StageStatus.choices,
        default=StageStatus.PENDING,
        help_text="Current stage status",
    )
    skip_reason = models.TextField(null=True, blank=True)

    bound_at = models.DateTimeField(null=True, blank=True)
    settled_at = models.DateTimeField(null=True, blank=True)

    def __str__(self):
        return f"{self.name} ({self.status}) of run {self.run_id}"

    class Meta:
        app_label = "qraft"
        verbose_name = "Qraft Run Stage"
        verbose_name_plural = "Qraft Run Stages"
        ordering = ["position", "name"]
        constraints = [
            models.UniqueConstraint(
                fields=["run", "name"], name="unique_stage_name_per_run"
            )
        ]
