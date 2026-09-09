"""
Execution graphs: qraft-owned plans with explicit edges and quiescent settlement.

A graph is built in memory, validated, and started in one transaction. Qraft
dispatches every node whose `after` dependencies have succeeded or been
skipped, schedules attempts through `scheduler.schedule_attempt()`, and settles
the graph when nothing is running and nothing can start.
"""

import hashlib
import json
import logging
from contextlib import contextmanager
from datetime import timedelta

from django.core.serializers.json import DjangoJSONEncoder
from django.db import transaction
from django.utils import timezone

from qraft import metrics, signals
from qraft.models import QraftTask, TaskStatus
from qraft.models.graphs import (
    SETTLED_NODE_STATUSES,
    GraphStatus,
    NodeStatus,
    QraftGraph,
    QraftGraphNode,
)

_logger = logging.getLogger("qraft.graphs")

_TASK_OUTCOMES = {
    TaskStatus.SUCCEEDED: NodeStatus.SUCCEEDED,
    TaskStatus.FAILED: NodeStatus.FAILED,
    TaskStatus.EXHAUSTED: NodeStatus.FAILED,
}

_NODE_OPTION_KEYS = frozenset(
    {
        "max_attempts",
        "base_delay",
        "backoff_strategy",
        "jitter",
        "jitter_max",
        "retry_exceptions",
        "skip_exceptions",
        "rate_limit_exceptions",
        "rate_limit_max_delay",
        "cluster",
        "priority",
        "stall_after",
        "success_hook",
        "success_args",
        "success_kwargs",
        "failure_hook",
        "failure_args",
        "failure_kwargs",
        "hook_context",
    }
)


class GraphError(ValueError):
    """Raised when a graph or node transition is refused."""


class Graph:
    """In-memory builder for a graph plan. No row exists until `start()`."""

    def __init__(
        self,
        subject=None,
        kind: str | None = None,
        revision: str | None = None,
        metadata: dict | None = None,
        started_at=None,
        on_settled: str | None = None,
        on_settled_kwargs: dict | None = None,
        previous_graph=None,
        budgets: dict | None = None,
        cluster: str | None = None,
    ):
        from qraft.tasks import parse_subject

        self._subject = parse_subject(subject)
        self._kind = kind
        self._revision = revision
        self._metadata = metadata
        self._started_at = started_at
        self._on_settled = on_settled
        self._on_settled_kwargs = on_settled_kwargs or {}
        self._previous_graph_id = getattr(previous_graph, "id", previous_graph)
        self._budgets = budgets
        self._cluster = cluster
        self._nodes: list[dict] = []

    def node(
        self,
        key: str,
        func: str,
        *args,
        after=(),
        recovery: str,
        qraft_options: dict | None = None,
        **kwargs,
    ):
        from qraft.tasks import _reject_workflow_member_opt_keys

        if not recovery:
            raise GraphError(f"node {key!r} must declare recovery")
        _reject_workflow_member_opt_keys(kwargs)
        options = dict(qraft_options or {})
        unknown = set(options) - _NODE_OPTION_KEYS
        if unknown:
            raise GraphError(
                f"node {key!r} has unrecognised qraft_options: {sorted(unknown)}"
            )
        self._nodes.append(
            {
                "key": key,
                "func": func,
                "task_args": list(args),
                "task_kwargs": kwargs,
                "after": list(after),
                "recovery": recovery,
                "options": options,
            }
        )

    def start(self, request_key: str | None = None) -> str:
        """Validate topology, write rows, and dispatch root nodes."""
        if not self._nodes:
            raise GraphError("a graph must declare at least one node")
        keys = [node["key"] for node in self._nodes]
        if len(set(keys)) != len(keys):
            raise GraphError(f"node keys must be unique within a graph, got {keys}")
        _validate_topology(self._nodes)
        depths = _compute_depths(self._nodes)
        plan_hash = _plan_hash(self._nodes)
        budgets = _validated_budgets(self._budgets)

        if request_key:
            existing = QraftGraph.objects.filter(request_key=request_key).first()
            if existing:
                if existing.plan_hash != plan_hash:
                    raise GraphError(
                        f"request_key {request_key!r} already used with a "
                        "different plan"
                    )
                return str(existing.id)

        subject_type, subject_id = self._subject
        with transaction.atomic():
            graph = QraftGraph.objects.create(
                subject_type=subject_type,
                subject_id=subject_id,
                kind=self._kind,
                revision=self._revision,
                metadata=self._metadata,
                date_started=self._started_at or timezone.now(),
                on_settled=self._on_settled,
                on_settled_kwargs=self._on_settled_kwargs,
                previous_graph_id=self._previous_graph_id,
                budgets=budgets,
                cluster=self._cluster,
                request_key=request_key,
                plan_hash=plan_hash,
            )
            node_rows = []
            for position, spec in enumerate(self._nodes):
                node_rows.append(
                    QraftGraphNode(
                        graph=graph,
                        key=spec["key"],
                        position=position,
                        depth=depths[spec["key"]],
                        after=spec["after"],
                        func=spec["func"],
                        task_args=spec["task_args"],
                        task_kwargs=spec["task_kwargs"],
                        options=spec["options"],
                        recovery=spec["recovery"],
                    )
                )
            QraftGraphNode.objects.bulk_create(node_rows)
            _dispatch_frontier(graph.id)

        _logger.debug(
            "Started QraftGraph %s for %s:%s with %d nodes",
            graph.id,
            subject_type,
            subject_id,
            len(node_rows),
        )
        return str(graph.id)


def _validate_topology(nodes: list[dict]) -> None:
    keys = {node["key"] for node in nodes}
    for node in nodes:
        if node["key"] in node["after"]:
            raise GraphError(f"node {node['key']!r} cannot depend on itself")
        unknown = set(node["after"]) - keys
        if unknown:
            raise GraphError(
                f"node {node['key']!r} depends on unknown key(s): {sorted(unknown)}"
            )
    if _has_cycle(nodes):
        raise GraphError("graph topology contains a cycle")


def _has_cycle(nodes: list[dict]) -> bool:
    edges = {node["key"]: node["after"] for node in nodes}
    visiting: set[str] = set()
    visited: set[str] = set()

    def visit(key: str) -> bool:
        if key in visiting:
            return True
        if key in visited:
            return False
        visiting.add(key)
        for dep in edges.get(key, []):
            if visit(dep):
                return True
        visiting.remove(key)
        visited.add(key)
        return False

    return any(visit(key) for key in edges)


def _compute_depths(nodes: list[dict]) -> dict[str, int]:
    by_key = {node["key"]: node["after"] for node in nodes}
    depths: dict[str, int] = {}

    def depth(key: str) -> int:
        if key in depths:
            return depths[key]
        after = by_key[key]
        depths[key] = 0 if not after else 1 + max(depth(dep) for dep in after)
        return depths[key]

    for key in by_key:
        depth(key)
    return depths


def _plan_hash(nodes: list[dict]) -> str:
    payload = json.dumps(nodes, sort_keys=True, cls=DjangoJSONEncoder)
    return hashlib.sha256(payload.encode()).hexdigest()


def _validated_budgets(budgets: dict | None) -> dict | None:
    if not budgets:
        return None
    checked = {}
    for key, value in budgets.items():
        if not isinstance(value, int) or isinstance(value, bool) or value < 0:
            raise GraphError(
                f"budget {key!r} must be a non-negative whole number of "
                f"requests, got {value!r}"
            )
        checked[str(key)] = value
    return checked


def get(graph_id) -> QraftGraph:
    try:
        return QraftGraph.objects.get(id=graph_id)
    except (QraftGraph.DoesNotExist, ValueError, TypeError) as exc:
        raise GraphError(f"unknown graph {graph_id!r}") from exc


@contextmanager
def correlating(graph_id, node, subject_type, subject_id):
    """Yield correlation fields while the graph row is locked."""
    if graph_id is None:
        yield member_labels(None, node, subject_type, subject_id)
        return
    with transaction.atomic():
        _locked(graph_id)
        yield member_labels(graph_id, node, subject_type, subject_id)


def member_labels(
    graph_id, node: str | None, subject_type: str | None, subject_id: str | None
) -> dict:
    if graph_id is None:
        if node:
            raise GraphError(f"node {node!r} was given without a graph")
        return {
            "graph_id": None,
            "node": None,
            "subject_type": subject_type,
            "subject_id": subject_id,
        }

    graph = get(graph_id)
    if graph.status != GraphStatus.RUNNING:
        raise GraphError(
            f"graph {graph.id} is {graph.status}; a settled graph is never reopened. "
            "Start a new graph with previous_graph pointing at this one."
        )
    if node and not graph.nodes.filter(key=node).exists():
        raise GraphError(f"graph {graph.id} declared no node named {node!r}")
    if subject_type is None and subject_id is None:
        subject_type, subject_id = graph.subject_type, graph.subject_id
    elif (subject_type, subject_id) != (graph.subject_type, graph.subject_id):
        raise GraphError(
            f"subject ({subject_type}, {subject_id}) differs from graph "
            f"{graph.id}'s ({graph.subject_type}, {graph.subject_id}); "
            "a graph implies its subject"
        )
    return {
        "graph_id": graph.id,
        "node": node,
        "subject_type": subject_type,
        "subject_id": subject_id,
    }


def bind_subject(graph_id, subject) -> None:
    from qraft.models import QraftBatchModel, QraftChainModel, QraftIterModel
    from qraft.tasks import parse_subject

    subject_type, subject_id = parse_subject(subject)
    if subject_type is None:
        raise GraphError("bind_subject needs a (subject_type, subject_id) pair")

    now = timezone.now()
    with transaction.atomic():
        graph = _locked(graph_id)
        if graph.status != GraphStatus.RUNNING:
            raise GraphError(
                f"graph {graph.id} is {graph.status}; "
                "a settled graph's subject is final"
            )
        bound = QraftGraph.objects.filter(
            pk=graph.pk,
            status=GraphStatus.RUNNING,
            subject_type__isnull=True,
            subject_id__isnull=True,
        ).update(subject_type=subject_type, subject_id=subject_id, date_updated=now)
        if not bound:
            raise GraphError(
                f"graph {graph.id} already names subject "
                f"({graph.subject_type}, {graph.subject_id}); a graph's subject is "
                "declared once"
            )
        for model in (QraftTask, QraftChainModel, QraftIterModel, QraftBatchModel):
            model.objects.filter(graph_id=graph.id, subject_type__isnull=True).update(
                subject_type=subject_type, subject_id=subject_id
            )

    _logger.debug("Bound subject %s:%s to graph %s", subject_type, subject_id, graph_id)


def _locked(graph_id) -> QraftGraph:
    try:
        return QraftGraph.objects.select_for_update().get(id=graph_id)
    except (QraftGraph.DoesNotExist, ValueError, TypeError) as exc:
        raise GraphError(f"unknown graph {graph_id!r}") from exc


def skip(graph_id, node_key: str, reason: str | None = None) -> None:
    now = timezone.now()
    with transaction.atomic():
        graph = _locked(graph_id)
        if graph.status != GraphStatus.RUNNING:
            raise GraphError(f"graph {graph.id} is {graph.status}; its nodes are final")
        if not graph.nodes.filter(key=node_key).exists():
            raise GraphError(f"graph {graph.id} declared no node named {node_key!r}")
        skipped = QraftGraphNode.objects.filter(
            graph_id=graph.id, key=node_key, status=NodeStatus.PENDING
        ).update(
            status=NodeStatus.SKIPPED,
            skip_reason=reason,
            settled_at=now,
        )
        if not skipped:
            current = QraftGraphNode.objects.get(graph_id=graph_id, key=node_key)
            raise GraphError(
                f"node {node_key!r} of graph {graph_id} is {current.status}; "
                "only a pending node may be skipped"
            )
        signals.send(
            signals.node_settled,
            QraftGraphNode,
            node_payload(QraftGraphNode.objects.get(graph_id=graph_id, key=node_key)),
        )
        _dispatch_frontier(graph.id)
        settled = _maybe_settle(graph, now)
    _dispatch_settled_hook(settled)


def cancel(graph_id) -> bool:
    return _explicit_settlement(graph_id, GraphStatus.CANCELLED)


def _explicit_settlement(graph_id, status: str) -> bool:
    graph = get(graph_id)
    with transaction.atomic():
        settled = _settle(graph, status, timezone.now())
    if settled is None:
        graph.refresh_from_db()
        raise GraphError(f"graph {graph_id} is already {graph.status}")
    _dispatch_settled_hook(settled)
    return True


def _dispatch_frontier(graph_id) -> None:
    """Dispatch every pending node whose dependencies are met."""
    graph = _locked(graph_id)
    if graph.status != GraphStatus.RUNNING:
        return
    ready = _ready_nodes(graph)
    for node in ready:
        _dispatch_node(graph, node)


def _ready_nodes(graph: QraftGraph) -> list[QraftGraphNode]:
    settled = {
        row["key"]: row["status"]
        for row in graph.nodes.filter(
            status__in=(NodeStatus.SUCCEEDED, NodeStatus.SKIPPED)
        ).values("key", "status")
    }
    ready = []
    for node in graph.nodes.filter(status=NodeStatus.PENDING).order_by(
        "depth", "position"
    ):
        if all(
            settled.get(dep) in (NodeStatus.SUCCEEDED, NodeStatus.SKIPPED)
            for dep in node.after
        ):
            ready.append(node)
    return ready


def _dispatch_node(graph: QraftGraph, node: QraftGraphNode) -> None:
    from qraft.retry import RetryPolicy
    from qraft.scheduler import schedule_attempt

    options = dict(node.options or {})
    retry_policy = RetryPolicy.from_options(options)
    cluster = options.get("cluster") or graph.cluster
    now = timezone.now()

    qraft_task = QraftTask.objects.create(
        func=node.func,
        task_args=node.task_args,
        task_kwargs=node.task_kwargs,
        success_hook=options.get("success_hook"),
        success_args=options.get("success_args", []),
        success_kwargs=options.get("success_kwargs", {}),
        failure_hook=options.get("failure_hook"),
        failure_args=options.get("failure_args", []),
        failure_kwargs=options.get("failure_kwargs", {}),
        hook_context=bool(options.get("hook_context", False)),
        retry_policy=retry_policy.to_dict() if retry_policy else {},
        stall_after=options.get("stall_after"),
        priority=options.get("priority", "default"),
        status=TaskStatus.PENDING,
        subject_type=graph.subject_type,
        subject_id=graph.subject_id,
        graph_id=graph.id,
        node=node.key,
        graph_node=node,
    )
    schedule_attempt(
        qraft_task,
        attempt_number=1,
        not_before=now,
        cluster=cluster,
    )
    QraftGraphNode.objects.filter(pk=node.pk, status=NodeStatus.PENDING).update(
        task=qraft_task,
        status=NodeStatus.RUNNING,
        dispatched_at=now,
    )


def handle_node_completion(qraft_task, attempt) -> bool:
    """
    Record a node's outcome and advance the graph.

    Returns True when the completion was handled (including superseded drops).
    """
    node = qraft_task.graph_node
    if node is None:
        return False

    outcome = _TASK_OUTCOMES.get(qraft_task.status)
    if outcome is None:
        return True

    now = timezone.now()
    with transaction.atomic():
        graph = _locked(node.graph_id)
        bound = (
            QraftGraphNode.objects.filter(pk=node.pk)
            .values_list("task_id", flat=True)
            .first()
        )
        if bound != qraft_task.id:
            _logger.info(
                "Graph %s node %s is bound to task %s, not %s; dropping the "
                "completion of a superseded generation",
                node.graph_id,
                node.key,
                bound,
                qraft_task.id,
            )
            return True

        recorded = QraftGraphNode.objects.filter(
            pk=node.pk, settled_at__isnull=True
        ).update(status=outcome, settled_at=now)
        if not recorded:
            return True

        node.refresh_from_db()
        signals.send(signals.node_settled, QraftGraphNode, node_payload(node))
        node_labels = {
            "kind": graph.kind,
            "key": node.key,
            "outcome": node.status,
        }
        metrics.counter_on_commit("qraft.node.settled", **node_labels)
        duration = metrics.seconds_between(node.dispatched_at, node.settled_at)
        if duration is not None:
            metrics.histogram_on_commit("qraft.node.duration", duration, **node_labels)

        if graph.status == GraphStatus.RUNNING:
            _dispatch_frontier(graph.id)
            settled = _maybe_settle(graph, now)
        else:
            settled = None

    _dispatch_settled_hook(settled)
    return True


def _maybe_settle(graph: QraftGraph, now):
    """Settle the graph when quiescent."""
    statuses = set(graph.nodes.values_list("status", flat=True))
    if NodeStatus.RUNNING in statuses or NodeStatus.PENDING in statuses:
        running = graph.nodes.filter(status=NodeStatus.RUNNING).exists()
        if running:
            return None
        if _ready_nodes(graph):
            return None

    if NodeStatus.FAILED in statuses:
        return _settle(graph, GraphStatus.FAILED, now)
    if NodeStatus.CANCELLED in statuses:
        return _settle(graph, GraphStatus.CANCELLED, now)
    if statuses <= {NodeStatus.SUCCEEDED, NodeStatus.SKIPPED}:
        return _settle(graph, GraphStatus.SUCCEEDED, now)
    return None


def _settle(graph: QraftGraph, status: str, now):
    summary = build_summary(graph, status, now)
    settled = QraftGraph.objects.filter(
        pk=graph.pk, settled_at__isnull=True, status=GraphStatus.RUNNING
    ).update(status=status, settled_at=now, summary=summary, date_updated=now)
    if not settled:
        return None
    graph.refresh_from_db()

    signals.send(signals.graph_settled, QraftGraph, graph_payload(graph))
    graph_labels = {
        "subject_type": graph.subject_type,
        "kind": graph.kind,
        "outcome": graph.status,
    }
    metrics.counter_on_commit("qraft.graph.settled", **graph_labels)
    duration = metrics.seconds_between(graph.date_started, graph.settled_at)
    if duration is not None:
        metrics.histogram_on_commit("qraft.graph.duration", duration, **graph_labels)
        if graph.status == GraphStatus.SUCCEEDED:
            metrics.histogram_on_commit(
                "qraft.graph.report_to_ready",
                duration,
                subject_type=graph.subject_type,
                kind=graph.kind,
            )
    return graph


def _dispatch_settled_hook(graph) -> None:
    if graph is None or not graph.on_settled:
        return
    from qraft.dispatchers import _dispatch_workflow_hook

    _dispatch_workflow_hook(
        workflow_type="graph",
        workflow_id=graph.id,
        hook_type="settled",
        hook_path=graph.on_settled,
        hook_args=[],
        hook_kwargs=dict(graph.on_settled_kwargs or {}),
        context=hook_context(graph),
    )


def hook_context(graph: QraftGraph) -> dict:
    return {
        "graph_id": str(graph.id),
        "subject_type": graph.subject_type,
        "subject_id": graph.subject_id,
        "kind": graph.kind,
        "revision": graph.revision,
        "metadata": graph.metadata,
        "outcome": graph.status,
        "generation": graph.generation,
        "nodes": {
            node.key: node.status
            for node in graph.nodes.all().order_by("depth", "position")
        },
        "started_at": graph.date_started.isoformat() if graph.date_started else None,
        "settled_at": graph.settled_at.isoformat() if graph.settled_at else None,
        "duration_s": metrics.seconds_between(graph.date_started, graph.settled_at),
        "previous_graph_id": (
            str(graph.previous_graph_id) if graph.previous_graph_id else None
        ),
    }


def node_payload(node: QraftGraphNode) -> dict:
    graph = node.graph
    return {
        "graph_id": str(graph.id),
        "node_key": node.key,
        "generation": node.generation,
        "outcome": node.status,
        "subject_type": graph.subject_type,
        "subject_id": graph.subject_id,
        "kind": graph.kind,
        "settled_at": node.settled_at.isoformat() if node.settled_at else None,
    }


def graph_payload(graph: QraftGraph) -> dict:
    return {
        "graph_id": str(graph.id),
        "subject_type": graph.subject_type,
        "subject_id": graph.subject_id,
        "kind": graph.kind,
        "revision": graph.revision,
        "outcome": graph.status,
        "generation": graph.generation,
        "nodes": {
            node.key: node.status
            for node in graph.nodes.all().order_by("depth", "position")
        },
        "previous_graph_id": (
            str(graph.previous_graph_id) if graph.previous_graph_id else None
        ),
        "date_started": (
            graph.date_started.isoformat() if graph.date_started else None
        ),
        "settled_at": graph.settled_at.isoformat() if graph.settled_at else None,
        "overdue_flagged_at": (
            graph.overdue_flagged_at.isoformat() if graph.overdue_flagged_at else None
        ),
    }


def build_summary(
    graph: QraftGraph, status: str, now, reason: str | None = None
) -> dict:
    from qraft.context import aggregate_graph_usage

    nodes = []
    for node in graph.nodes.all().order_by("depth", "position"):
        nodes.append(
            {
                "key": node.key,
                "task_id": str(node.task_id) if node.task_id else None,
                "outcome": node.status,
                "dispatched_at": (
                    node.dispatched_at.isoformat() if node.dispatched_at else None
                ),
                "settled_at": node.settled_at.isoformat() if node.settled_at else None,
                "duration_s": metrics.seconds_between(
                    node.dispatched_at, node.settled_at
                ),
                "skip_reason": node.skip_reason,
                "generation": node.generation,
            }
        )
    summary = {
        "outcome": status,
        "nodes": nodes,
        "duration_s": metrics.seconds_between(graph.date_started, now),
        "usage": aggregate_graph_usage(graph.id),
    }
    if reason:
        summary["reason"] = reason
    return summary


def replay_settled_hooks(grace: float) -> int:
    from qraft.models import WorkflowHookDispatch

    cutoff = timezone.now() - timedelta(seconds=grace)
    dispatched = WorkflowHookDispatch.objects.filter(
        workflow_type="graph", hook_type="settled"
    ).values("workflow_id")
    pending = (
        QraftGraph.objects.filter(settled_at__lt=cutoff)
        .exclude(on_settled=None)
        .exclude(on_settled="")
        .exclude(id__in=dispatched)
    )
    replayed = 0
    for graph in pending:
        _logger.warning(
            "Re-offering on_settled for QraftGraph %s; it settled at %s but the "
            "hook was never dispatched",
            graph.id,
            graph.settled_at,
        )
        _dispatch_settled_hook(graph)
        replayed += 1
    return replayed


def flag_overdue() -> int:
    from django.conf import settings

    after = getattr(settings, "QRAFT_GRAPH_OVERDUE_AFTER", None) or getattr(
        settings, "QRAFT_RUN_OVERDUE_AFTER", None
    )
    if not after:
        return 0

    now = timezone.now()
    cutoff = now - timedelta(seconds=after)
    overdue = QraftGraph.objects.filter(
        status=GraphStatus.RUNNING,
        overdue_flagged_at__isnull=True,
        date_started__lt=cutoff,
    )
    flagged = 0
    for graph in overdue:
        if not QraftGraph.objects.filter(
            pk=graph.pk, overdue_flagged_at__isnull=True
        ).update(overdue_flagged_at=now):
            continue
        graph.overdue_flagged_at = now
        _logger.warning(
            "QraftGraph %s has been running for %.0fs (subject %s:%s)",
            graph.id,
            (now - graph.date_started).total_seconds(),
            graph.subject_type,
            graph.subject_id,
        )
        signals.send(signals.graph_overdue, QraftGraph, graph_payload(graph))
        flagged += 1
    return flagged


def _blocked_by(graph: QraftGraph, node: QraftGraphNode) -> list[str]:
    if node.status != NodeStatus.PENDING:
        return []
    settled = dict(
        graph.nodes.filter(status__in=SETTLED_NODE_STATUSES).values_list(
            "key", "status"
        )
    )
    return [
        dep
        for dep in node.after
        if settled.get(dep) not in (NodeStatus.SUCCEEDED, NodeStatus.SKIPPED)
    ]


def snapshot(graph_id) -> dict:
    """JSON-safe visibility contract for dashboards and consumers."""
    from qraft.context import aggregate_usage

    graph = get(graph_id)
    now = timezone.now()
    nodes_out = []
    frontier = []
    for node in graph.nodes.all().order_by("depth", "position"):
        blocked = _blocked_by(graph, node)
        if node.status == NodeStatus.PENDING and not blocked:
            frontier.append(node.key)
        attempts_out = []
        for task in QraftTask.objects.filter(graph_node=node).order_by("date_created"):
            for attempt in task.attempts.order_by("attempt_number"):
                attempts_out.append(
                    {
                        "attempt_id": str(attempt.id),
                        "attempt_number": attempt.attempt_number,
                        "success": attempt.success,
                        "exception_class": attempt.exception_class,
                        "duration_s": metrics.seconds_between(
                            attempt.date_started, attempt.date_completed
                        ),
                        "progress": attempt.progress,
                        "progress_advanced_age_s": metrics.seconds_between(
                            attempt.progress_advanced_at, now
                        ),
                        "stall_suspected": attempt.stall_suspected_at is not None,
                        "usage": attempt.usage,
                    }
                )
        nodes_out.append(
            {
                "key": node.key,
                "depth": node.depth,
                "after": node.after,
                "status": node.status,
                "generation": node.generation,
                "cluster": (node.options or {}).get("cluster") or graph.cluster,
                "recovery": node.recovery,
                "dispatched_at": (
                    node.dispatched_at.isoformat() if node.dispatched_at else None
                ),
                "settled_at": node.settled_at.isoformat() if node.settled_at else None,
                "attempts": attempts_out,
                "usage": aggregate_usage(node.task) if node.task_id else {},
                "blocked_by": blocked,
            }
        )
    return {
        "graph_id": str(graph.id),
        "status": graph.status,
        "generation": graph.generation,
        "subject_type": graph.subject_type,
        "subject_id": graph.subject_id,
        "kind": graph.kind,
        "revision": graph.revision,
        "metadata": graph.metadata,
        "budgets": graph.budgets,
        "date_started": graph.date_started.isoformat() if graph.date_started else None,
        "settled_at": graph.settled_at.isoformat() if graph.settled_at else None,
        "frontier": frontier,
        "nodes": nodes_out,
        "usage": build_summary(graph, graph.status, now)["usage"],
    }
