"""Graph and node models: one row spanning a plan qraft dispatches."""

from uuid import uuid4

from django.core.serializers.json import DjangoJSONEncoder
from django.db import models
from django.utils import timezone

from .mixins import SubjectMixin


class GraphStatus(models.TextChoices):
    """Where a graph sits. Everything but RUNNING is terminal and never mutated."""

    RUNNING = "running", "Running"
    SUCCEEDED = "succeeded", "Succeeded"
    FAILED = "failed", "Failed"
    CANCELLED = "cancelled", "Cancelled"


TERMINAL_GRAPH_STATUSES = (
    GraphStatus.SUCCEEDED,
    GraphStatus.FAILED,
    GraphStatus.CANCELLED,
)


class NodeStatus(models.TextChoices):
    """Where a node sits relative to its current generation's task."""

    PENDING = "pending", "Pending"
    RUNNING = "running", "Running"
    SUCCEEDED = "succeeded", "Succeeded"
    FAILED = "failed", "Failed"
    CANCELLED = "cancelled", "Cancelled"
    SKIPPED = "skipped", "Skipped"


SETTLED_NODE_STATUSES = (
    NodeStatus.SUCCEEDED,
    NodeStatus.FAILED,
    NodeStatus.CANCELLED,
    NodeStatus.SKIPPED,
)


class RecoveryMode(models.TextChoices):
    """How a node's completion may be trusted on resume."""

    TRANSACTIONAL = "transactional", "Transactional"
    IDEMPOTENT = "idempotent", "Idempotent"
    MANUAL = "manual", "Manual"


class QraftGraphQuerySet(models.QuerySet):
    def for_subject(self, subject) -> "QraftGraphQuerySet":
        """Every graph for one domain entity, newest first."""
        subject_type, subject_id = subject
        return self.filter(
            subject_type=subject_type, subject_id=str(subject_id)
        ).order_by("-date_created")


class QraftGraph(SubjectMixin, models.Model):
    """
    One execution of a plan over one subject.

    Qraft owns the topology sealed at `start()` and dispatches every node
    whose `after` dependencies have settled. The graph settles on quiescence:
    nothing running and nothing newly dispatched.
    """

    objects = QraftGraphQuerySet.as_manager()

    id = models.UUIDField(primary_key=True, default=uuid4, editable=False)

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

    previous_graph = models.ForeignKey(
        "self",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="reruns",
        help_text="The graph this one reruns",
    )

    status = models.CharField(
        max_length=16,
        choices=GraphStatus.choices,
        default=GraphStatus.RUNNING,
        db_index=True,
        help_text="Current graph status",
    )

    generation = models.PositiveIntegerField(
        default=1,
        db_default=1,
        help_text="Bumped on resume; stale-generation completions are dropped",
    )

    cluster = models.CharField(
        max_length=150,
        null=True,
        blank=True,
        help_text="Default cluster for nodes that name none",
    )

    date_started = models.DateTimeField(
        default=timezone.now,
        help_text="When the work this graph covers began; may be backdated",
    )
    settled_at = models.DateTimeField(
        null=True,
        blank=True,
        help_text="When the graph settled (settlement compare-and-swap column)",
    )
    resumed_at = models.DateTimeField(
        null=True,
        blank=True,
        help_text="When the graph was last resumed",
    )
    overdue_flagged_at = models.DateTimeField(
        null=True,
        blank=True,
        help_text="When the overdue sweep first flagged this graph",
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

    budgets = models.JSONField(
        null=True,
        blank=True,
        encoder=DjangoJSONEncoder,
        help_text="Remaining request allowance per budget key (whole counts)",
    )

    summary = models.JSONField(
        null=True,
        blank=True,
        encoder=DjangoJSONEncoder,
        help_text="Snapshot of nodes, durations and usage, written at settlement",
    )

    request_key = models.CharField(
        max_length=255,
        null=True,
        blank=True,
        unique=True,
        help_text="Submission idempotency key; same key and plan returns the graph",
    )
    plan_hash = models.CharField(
        max_length=64,
        null=True,
        blank=True,
        help_text="Fingerprint of the node topology at start()",
    )

    date_created = models.DateTimeField(auto_now_add=True)
    date_updated = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"QraftGraph {self.id} ({self.status})"

    def bind_subject(self, subject_type: str, subject_id) -> None:
        """Name this graph's subject once, while it is still RUNNING."""
        from qraft import graphs

        graphs.bind_subject(self.id, (subject_type, subject_id))
        self.refresh_from_db(fields=["subject_type", "subject_id", "date_updated"])

    class Meta:
        app_label = "qraft"
        verbose_name = "Qraft Graph"
        verbose_name_plural = "Qraft Graphs"
        ordering = ["-date_created"]
        indexes = [
            models.Index(
                fields=["subject_type", "subject_id"], name="qraft_graph_subject_idx"
            ),
        ]


class QraftGraphNode(models.Model):
    """One unit of work in a graph, frozen at start()."""

    id = models.UUIDField(primary_key=True, default=uuid4, editable=False)

    graph = models.ForeignKey(
        QraftGraph,
        related_name="nodes",
        on_delete=models.CASCADE,
        help_text="Graph that declared this node",
    )

    key = models.CharField(max_length=100, help_text="Node key, unique per graph")
    position = models.PositiveSmallIntegerField(
        default=0,
        help_text="Declaration order within the same depth",
    )
    depth = models.PositiveSmallIntegerField(
        default=0,
        help_text="Longest path from a root, derived at start()",
    )

    after = models.JSONField(
        default=list,
        blank=True,
        encoder=DjangoJSONEncoder,
        help_text="Node keys that must succeed or be skipped before this runs",
    )

    func = models.CharField(
        max_length=256, help_text="Dotted path to the task function"
    )
    task_args = models.JSONField(
        default=list,
        blank=True,
        encoder=DjangoJSONEncoder,
        help_text="Frozen positional arguments",
    )
    task_kwargs = models.JSONField(
        default=dict,
        blank=True,
        encoder=DjangoJSONEncoder,
        help_text="Frozen keyword arguments",
    )
    options = models.JSONField(
        default=dict,
        blank=True,
        encoder=DjangoJSONEncoder,
        help_text="Frozen qraft options (retry, cluster, hooks, ...)",
    )

    recovery = models.CharField(
        max_length=16,
        choices=RecoveryMode.choices,
        help_text="How this node's completion may be trusted on resume",
    )

    generation = models.PositiveIntegerField(
        default=1,
        db_default=1,
        help_text="Bumped on resume; the task FK is the generation marker",
    )
    receipt = models.JSONField(
        null=True,
        blank=True,
        help_text=(
            "What the node published, written in the same transaction as the "
            "application's own writes. Its presence is the proof the effect "
            "committed; cleared when the node is re-run"
        ),
    )
    receipt_attempt_id = models.UUIDField(
        null=True,
        blank=True,
        help_text="The attempt that published the receipt",
    )

    task = models.ForeignKey(
        "QraftTask",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="graph_node_binding",
        help_text="The current generation's task",
    )

    status = models.CharField(
        max_length=16,
        choices=NodeStatus.choices,
        default=NodeStatus.PENDING,
        help_text="Current node status",
    )
    skip_reason = models.TextField(null=True, blank=True)

    dispatched_at = models.DateTimeField(null=True, blank=True)
    settled_at = models.DateTimeField(null=True, blank=True)

    def __str__(self):
        return f"{self.key} ({self.status}) of graph {self.graph_id}"

    class Meta:
        app_label = "qraft"
        verbose_name = "Qraft Graph Node"
        verbose_name_plural = "Qraft Graph Nodes"
        ordering = ["depth", "position", "key"]
        constraints = [
            models.UniqueConstraint(
                fields=["graph", "key"], name="unique_node_key_per_graph"
            )
        ]
