"""
Retention sweep for Django-Qraft.

Django-Q2 caps its own `Task` table with `save_limit`. Qraft's tables have no
equivalent, so `QraftTask`, `QraftTaskAttempt`, `HookDispatch` and the three
workflow tables grow forever. This prunes rows that have settled and fall
outside the retention window, which is an age (`conf.retention_days`), a count
(`conf.retention_max_tasks`), or the stricter of the two.

Two invariants shape what is safe to delete:

- Only terminal rows go. A task that is still PENDING or RUNNING is live work,
  however old the row looks, and a workflow that is still RUNNING may yet
  queue more members.
- A workflow is pruned as a unit. Deleting a member task out from under a live
  workflow would leave its counters pointing at rows that no longer exist, so
  member tasks are only pruned once their workflow is terminal (or gone).
- Graph membership is protected in every pass, not just the task pass. Deleting
  an iter or batch cascades to its member tasks, so a completed batch under a
  running graph would lose its rows through the workflow pass alone. Terminal
  graphs are pruned after the task pass and their nodes cascade with them; the
  `summary` written at settlement is what makes that safe.

Deletion runs in bounded batches: a first sweep over a table with millions of
rows must not hold one transaction open or build one enormous id list.
"""

import logging
from datetime import timedelta

from django.utils import timezone

from .conf import get_conf
from .models import (
    GraphStatus,
    QraftBatchModel,
    QraftChainModel,
    QraftGraph,
    QraftIterModel,
    QraftTask,
    TaskStatus,
    WorkflowHookDispatch,
    WorkflowStatus,
)

logger = logging.getLogger("qraft")

TERMINAL_TASK_STATUSES = (
    TaskStatus.SUCCEEDED,
    TaskStatus.FAILED,
    TaskStatus.EXHAUSTED,
)
TERMINAL_WORKFLOW_STATUSES = (
    WorkflowStatus.SUCCEEDED,
    WorkflowStatus.FAILED,
    WorkflowStatus.CANCELLED,
)

# Ceiling on batches per model per sweep, so one call can never loop forever
# against a table that is being written to as fast as it is pruned.
MAX_BATCHES = 1000


def _delete_in_batches(queryset, batch_size: int) -> int:
    """
    Delete a queryset in id-keyed batches, returning how many rows went.

    The slice is re-evaluated each round rather than materialised up front,
    so memory stays flat regardless of how far behind the sweep has fallen.

    The delete re-applies the queryset's own filter, not just `pk__in`: a row
    can be revived between the select and the delete (a DLQ requeue flips a
    terminal task back to PENDING and gives it a SCHEDULED attempt), and a
    bare pk delete would cascade that live work away.
    """
    model = queryset.model
    deleted = 0
    for _ in range(MAX_BATCHES):
        ids = list(queryset.values_list("pk", flat=True)[:batch_size])
        if not ids:
            break
        _count, details = queryset.filter(pk__in=ids).delete()
        batch_deleted = details.get(model._meta.label, 0)
        if not batch_deleted:
            break
        deleted += batch_deleted
    else:
        logger.warning(
            "Retention sweep hit the %d-batch ceiling for %s; more rows remain",
            MAX_BATCHES,
            model.__name__,
        )
    return deleted


def _count_cutoff(max_tasks: int):
    """
    Timestamp of the oldest settled task that a count-based bound keeps.

    Turning the count into a cutoff lets the count-based mode reuse every
    workflow-safety filter the age-based mode already has, instead of a
    second deletion path that would have to repeat them. Rows sharing that
    exact timestamp survive, so a tie keeps slightly more than N rather than
    deleting a row the user asked to keep.
    """
    oldest_kept = (
        QraftTask.objects.filter(status__in=TERMINAL_TASK_STATUSES)
        .order_by("-date_updated")
        .values_list("date_updated", flat=True)[max_tasks - 1 : max_tasks]
    )
    return oldest_kept[0] if oldest_kept else None


def log_retention_policy() -> None:
    """
    State the effective retention policy once, at cluster start.

    A user whose rows are disappearing must be able to see which rule did it,
    above all when the bound was inherited from Q_CLUSTER rather than written
    down in QRAFT_CLUSTER.
    """
    conf = get_conf()
    if not conf.retention_enabled():
        logger.info("Qraft retention: %s", "disabled; settled rows are kept forever")
        return

    rules = []
    if conf.retention_days:
        rules.append(f"older than {conf.retention_days} day(s)")
    if conf.retention_max_tasks:
        source = (
            "inherited from Q_CLUSTER['save_limit']"
            if conf.retention_inherited_from_save_limit
            else "retention_max_tasks"
        )
        rules.append(f"beyond the newest {conf.retention_max_tasks} task(s) [{source}]")
    logger.info(
        "Qraft retention: pruning settled rows %s, every %ss",
        " or ".join(rules),
        conf.retention_interval,
    )


def _live_workflow_ids(model) -> list:
    """Ids of workflows that have not settled yet, whose tasks must be kept."""
    return list(
        model.objects.exclude(status__in=TERMINAL_WORKFLOW_STATUSES).values_list(
            "id", flat=True
        )
    )


def _open_graph_ids() -> list:
    """Ids of graphs still running, whose members are evidence and must be kept."""
    return list(
        QraftGraph.objects.filter(status=GraphStatus.RUNNING).values_list(
            "id", flat=True
        )
    )


def sweep_retention(
    retention_days: float | None = None,
    max_tasks: int | None = None,
    batch_size: int | None = None,
) -> dict[str, int]:
    """
    Prune terminal Qraft rows beyond the retention window.

    Two bounds are supported and both are ceilings on how much history is
    kept, so a row goes as soon as either one says so: the stricter (later)
    cutoff wins when both are configured.

    Attempts, hook dispatches and chain steps are not swept directly: they
    cascade from the task or workflow they belong to.

    Args:
        retention_days: Override for conf.retention_days (age-based).
        max_tasks: Override for conf.retention_max_tasks (count-based).
        batch_size: Override for conf.retention_batch_size.

    Returns:
        Rows deleted, keyed by model name.
    """
    conf = get_conf()
    retention_days = (
        retention_days if retention_days is not None else conf.retention_days
    )
    max_tasks = max_tasks if max_tasks is not None else conf.retention_max_tasks
    if not retention_days and not max_tasks:
        return {}
    batch_size = batch_size if batch_size is not None else conf.retention_batch_size

    cutoffs = []
    if retention_days:
        cutoffs.append(timezone.now() - timedelta(days=retention_days))
    if max_tasks:
        count_cutoff = _count_cutoff(max_tasks)
        if count_cutoff is not None:
            cutoffs.append(count_cutoff)
    if not cutoffs:
        # Count-based only, and the table is still under the cap.
        return {}

    cutoff = max(cutoffs)
    deleted: dict[str, int] = {}

    # A terminal workflow can still have live members: cancel does not revoke
    # a running task, and FAILED can land while stragglers run. Deleting the
    # workflow would cascade those away mid-flight, so it waits for them.
    live_member = (TaskStatus.PENDING, TaskStatus.RUNNING)
    open_graphs = _open_graph_ids()
    workflow_querysets = (
        QraftChainModel.objects.exclude(steps__qraft_task__status__in=live_member),
        QraftIterModel.objects.exclude(tasks__status__in=live_member),
        QraftBatchModel.objects.exclude(tasks__status__in=live_member),
    )
    for queryset in workflow_querysets:
        count = _delete_in_batches(
            queryset.filter(
                status__in=TERMINAL_WORKFLOW_STATUSES, date_updated__lt=cutoff
            ).exclude(graph_id__in=open_graphs),
            batch_size,
        )
        if count:
            deleted[queryset.model.__name__] = count

    tasks = QraftTask.objects.filter(
        status__in=TERMINAL_TASK_STATUSES, date_updated__lt=cutoff
    ).exclude(qraft_iter_id__in=_live_workflow_ids(QraftIterModel))
    tasks = tasks.exclude(qraft_batch_id__in=_live_workflow_ids(QraftBatchModel))
    tasks = tasks.exclude(chain_step__chain__id__in=_live_workflow_ids(QraftChainModel))
    tasks = tasks.exclude(graph_id__in=open_graphs)

    count = _delete_in_batches(tasks, batch_size)
    if count:
        deleted["QraftTask"] = count

    count = _delete_in_batches(
        QraftGraph.objects.exclude(status=GraphStatus.RUNNING)
        .filter(date_updated__lt=cutoff)
        .exclude(qrafttask_members__status__in=live_member),
        batch_size,
    )
    if count:
        deleted["QraftGraph"] = count

    # WorkflowHookDispatch has no FK to its workflow, so nothing cascades to
    # it; its own age is the only signal available.
    count = _delete_in_batches(
        WorkflowHookDispatch.objects.filter(date_created__lt=cutoff), batch_size
    )
    if count:
        deleted["WorkflowHookDispatch"] = count

    if deleted:
        logger.info("Retention sweep pruned %s (older than %s)", deleted, cutoff)
    return deleted
