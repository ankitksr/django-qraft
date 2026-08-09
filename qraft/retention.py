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

Deletion runs in bounded batches: a first sweep over a table with millions of
rows must not hold one transaction open or build one enormous id list.
"""

import logging
from datetime import timedelta

from django.utils import timezone

from .conf import get_conf
from .models import (
    QraftBatchModel,
    QraftChainModel,
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
    """
    model = queryset.model
    deleted = 0
    for _ in range(MAX_BATCHES):
        ids = list(queryset.values_list("pk", flat=True)[:batch_size])
        if not ids:
            break
        count, _details = model.objects.filter(pk__in=ids).delete()
        if not count:
            break
        deleted += len(ids)
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
        logger.info("Qraft retention: disabled; settled rows are kept forever")
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

    for model in (QraftChainModel, QraftIterModel, QraftBatchModel):
        count = _delete_in_batches(
            model.objects.filter(
                status__in=TERMINAL_WORKFLOW_STATUSES, date_updated__lt=cutoff
            ),
            batch_size,
        )
        if count:
            deleted[model.__name__] = count

    tasks = QraftTask.objects.filter(
        status__in=TERMINAL_TASK_STATUSES, date_updated__lt=cutoff
    ).exclude(qraft_iter_id__in=_live_workflow_ids(QraftIterModel))
    tasks = tasks.exclude(qraft_batch_id__in=_live_workflow_ids(QraftBatchModel))
    tasks = tasks.exclude(chain_step__chain__id__in=_live_workflow_ids(QraftChainModel))

    count = _delete_in_batches(tasks, batch_size)
    if count:
        deleted["QraftTask"] = count

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
