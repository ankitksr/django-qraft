"""
Enhanced async_task implementation with dual-phase hooks and rich retry policies.
"""

import logging
import warnings
from typing import Any, Callable

from django.db import IntegrityError, transaction
from django.utils.module_loading import import_string
from django_q.tasks import async_task as q2_async_task

from qraft.brokers import QraftOrmBroker, priority_list_key
from qraft.conf import executing_cluster
from qraft.models import QraftTask, QraftTaskAttempt, TaskStatus
from qraft.models.tasks import TaskPriority
from qraft.retry import RetryPolicy

_logger = logging.getLogger("qraft")

# Keys django_q's own async_task() treats specially (django_q/tasks.py
# opt_keys), and that Qraft either forces to a fixed value or validates at
# the top of async_task(). django_q checks q_options for each of these
# *before* falling back to the plain keyword argument, so a caller who
# cannot pass one of these directly (rejected above) could still smuggle it
# in via q_options and silently override the invariant. Not every opt_key is
# here - only the ones Qraft itself sets or validates; e.g. `timeout` and
# `group` are plain passthroughs with nothing to protect.
_RESERVED_Q_OPTIONS = frozenset(
    {"hook", "save", "sync", "cached", "ack_failure", "broker", "cluster"}
)


def _existing_task_for_key(idempotency_key: str) -> str | None:
    """
    Return the q2_task_id of the task already enqueued under this key.

    Returns None if no QraftTask holds this key yet. A QraftTask that does
    hold the key still counts as existing even while its latest attempt is a
    SCHEDULED backoff retry sitting on a null q2_task_id (the scheduler only
    stamps that column once a dispatcher claims the attempt) - so this walks
    attempts newest-first for the last one that actually reached the broker,
    rather than trusting `latest_attempt` alone.
    """
    existing = QraftTask.objects.filter(idempotency_key=idempotency_key).first()
    if existing is None:
        return None

    q2_task_id = (
        existing.attempts.filter(q2_task_id__isnull=False)
        .order_by("-attempt_number")
        .values_list("q2_task_id", flat=True)
        .first()
    )
    if q2_task_id is None:
        return None

    _logger.info(
        "Idempotency key %s already used by QraftTask %s; skipping re-enqueue",
        idempotency_key,
        existing.id,
    )
    return q2_task_id


def _reclaim_dead_task_key(idempotency_key: str) -> None:
    """
    Clear a FAILED/EXHAUSTED task's idempotency key so a fresh enqueue can
    claim it.

    A single UPDATE scoped to the key and the dead statuses: race-tolerant
    against a concurrent caller doing the same reclaim, since the losing
    UPDATE just matches zero rows.
    """
    QraftTask.objects.filter(
        idempotency_key=idempotency_key,
        status__in=(TaskStatus.FAILED, TaskStatus.EXHAUSTED),
    ).update(idempotency_key=None)


def _importable_func_path(func: Callable) -> str:
    """
    Derive `module.name` for `func` and verify it round-trips back to `func`.

    A retry re-queues the task by dotted path, not by the live object, so
    the path has to actually resolve back to the callable that was passed
    in. `func.__module__.func.__name__` looks right for a plain module-level
    function but is wrong for a bound method (the module is the class's, and
    `__name__` is unqualified - it imports a different object or nothing at
    all) and crashes outright for a functools.partial (no `__name__`).
    Lambdas and locals already fail loudly at enqueue via pickling, so this
    only needs to catch the importability gap.
    """
    name = getattr(func, "__name__", None)
    module = getattr(func, "__module__", None)
    if not name or not module:
        raise ValueError(
            f"async_task() cannot derive an import path for {func!r}: it has "
            "no __module__/__name__ (e.g. a functools.partial). Pass the "
            "dotted-path string form of func instead."
        )

    func_path = f"{module}.{name}"
    try:
        resolved = import_string(func_path)
    except ImportError as exc:
        raise ValueError(
            f"async_task() cannot import {func_path!r} to re-run {func!r} on "
            "retry: it is not reachable at module level (e.g. a bound "
            "method). Pass a plain module-level function, or the "
            "dotted-path string form of func, instead."
        ) from exc

    if resolved is not func:
        raise ValueError(
            f"async_task() cannot safely re-run {func!r}: importing "
            f"{func_path!r} resolves to a different object ({resolved!r}), "
            "which happens for bound methods sharing a name with a "
            "module-level function. Pass a plain module-level function, or "
            "the dotted-path string form of func, instead."
        )

    return func_path


def _create_and_enqueue(qraft_metadata: dict, func, args, q2_kwargs: dict):
    """
    Create a QraftTask, queue it to Django-Q2, and record attempt 1.

    All three happen in one transaction so a failed enqueue leaves no
    half-built task behind.

    Returns:
        tuple[QraftTask, str]: the created task and its Django-Q2 task ID
    """
    with transaction.atomic():
        qraft_task = QraftTask.objects.create(**qraft_metadata)
        q2_task_id = q2_async_task(func, *args, **q2_kwargs)
        QraftTaskAttempt.objects.create(
            qraft_task=qraft_task,
            attempt_number=1,
            q2_task_id=q2_task_id,
            # Recorded so later attempts can inherit it. Omitting `cluster`
            # means django_q resolves the broker against this process's own
            # cluster name, which is what the fallback spells out.
            cluster=q2_kwargs.get("cluster") or executing_cluster(),
        )
    return qraft_task, q2_task_id


def async_task(
    func: str | Callable,
    *args,
    # Legacy Django-Q2 parameters
    hook: str | None = None,
    group: str | None = None,
    save: bool | None = None,
    timeout: int | None = None,
    ack_failure: bool | None = None,
    sync: bool = False,
    cached: bool | int | None = None,
    broker: Any | None = None,
    task_name: str | None = None,
    q_options: dict | None = None,
    # New Qraft parameters (config-only)
    qraft_options: dict | None = None,
    **kwargs,
) -> str:
    """
    Enhanced async_task for Django-Qraft.

    This function wraps Django-Q2's `async_task` while adding:
      - Dual-phase hooks (success/failure)
      - Rich retry policies
      - Structured persistence via `QraftTask` and `QraftTaskAttempt`

    Args:
        func:
            Callable or dotted-path string for the function to execute.
        *args:
            Positional arguments passed to the task function.
        hook (str, optional):
            Legacy Django-Q2 hook. Deprecated. If no Qraft hooks are provided,
            this will be treated as a `success_hook` with a warning.
        group, timeout, broker:
            Same semantics as Django-Q2 async_task.
        ack_failure:
            Forced to True - Qraft schedules its own retries, so Django-Q2
            must never also redeliver a failed message. `False` is rejected.
        save:
            Forced to True regardless of what is passed (only `False` is
            rejected outright). Qraft's hook handler resolves completion by
            reading the saved Django-Q2 Task row; with `Conf.SAVE_LIMIT < 0`
            Django-Q2 only writes that row for a successful task when
            `save=True` was requested, so leaving it unset would silently
            drop the row a successful task needs.
        sync:
            Not supported - must be left `False`. Qraft's hook handler
            expects the QraftTaskAttempt row to already exist when the task
            finishes; a synchronous run finishes (and fires the hook)
            in-process before `_create_and_enqueue` has committed that row.
        cached:
            Not supported - must be left `None`. A cached result is never
            persisted as a Django-Q2 Task row, so the hook handler has
            nothing to resolve completion against.
        task_name:
            Optional human-readable name for the task (appears in Django-Q2 admin).
            Fully preserved - Qraft uses database linkage instead of encoding metadata.
        q_options (dict, optional):
            Legacy Django-Q2 options. May not set `hook`, `save`, `sync`,
            `cached`, `ack_failure`, `broker`, or `cluster` - those raise a
            ValueError, since django_q would otherwise let them silently
            override the invariants enforced above.
        qraft_options (dict, optional):
            New configuration namespace for Qraft.
            Supported keys:
                success_hook (str): dotted path for success handler.
                success_args (tuple): positional args for success handler.
                success_kwargs (dict): keyword args for success handler.
                failure_hook (str): dotted path for failure handler.
                failure_args (tuple): positional args for failure handler.
                failure_kwargs (dict): keyword args for failure handler.
                max_attempts (int): maximum retry attempts.
                base_delay (float): base delay between retries in seconds.
                backoff_strategy (str): one of 'fixed', 'linear', 'exponential'.
                jitter (bool): whether to add random jitter to retries.
                jitter_max (float): maximum jitter as fraction of delay.
                retry_exceptions (list[str]): exception class names to retry on.
                skip_exceptions (list[str]): exception class names to skip.
                idempotency_key (str): caller-supplied dedupe key. A second
                    call with the same key is a no-op: it returns the
                    original call's q2_task_id instead of enqueueing again.
                    The key is a permanent dedupe by default, not a "retry
                    if failed" signal - even a FAILED/EXHAUSTED task blocks
                    re-enqueue. Callers that want to run again must use a
                    new key, unless idempotency_retry_dead is set.
                idempotency_retry_dead (bool): when True, a FAILED/EXHAUSTED
                    task under idempotency_key no longer blocks re-enqueue -
                    its key is cleared and this call proceeds as a fresh
                    enqueue holding the key. A live (not yet terminal) task
                    under the key is still deduped either way. Default
                    False preserves the permanent-dedupe behavior above.
                priority ("high" | "default" | "low"): priority lane to
                    enqueue into. Only takes effect if the cluster's broker
                    is `qraft.brokers.QraftOrmBroker` (see that module's
                    docstring); otherwise the task is queued normally.
        **kwargs:
            Extra keyword arguments passed to the task function.

    Returns:
        str: Django-Q2 Task ID (can be used to look up QraftTaskAttempt).
    """
    if sync:
        raise ValueError(
            "async_task(sync=True) is not supported: completion tracking "
            "relies on the QraftTaskAttempt row existing before the task "
            "runs, but a synchronous run finishes (and fires the hook) "
            "before that row is committed. Call the function directly for "
            "synchronous execution."
        )
    if save is False:
        raise ValueError(
            "async_task(save=False) is not supported: the hook handler "
            "resolves completion by reading the saved Django-Q2 Task row, "
            "which save=False never writes."
        )
    if cached is not None:
        raise ValueError(
            "async_task(cached=...) is not supported: a cached result is "
            "never persisted as a Django-Q2 Task row, so the hook handler "
            "has nothing to resolve completion against."
        )
    if ack_failure is False:
        raise ValueError(
            "async_task(ack_failure=False) is not supported: Qraft owns "
            "retries, and leaving a failed message unacknowledged makes the "
            "broker redeliver attempt N while Qraft has already scheduled "
            "attempt N+1. Use qraft_options={'max_attempts': ...} instead."
        )

    if q_options is None:
        q_options = {}

    reserved_in_q_options = _RESERVED_Q_OPTIONS.intersection(q_options)
    if reserved_in_q_options:
        raise ValueError(
            f"async_task(q_options={{...}}) must not set "
            f"{sorted(reserved_in_q_options)}: django_q checks q_options for "
            "each of these before the plain keyword argument, so passing "
            "them here would silently override Qraft's own invariants "
            "(hook routing, retry ownership, save/sync/cached rejection, "
            "priority-lane targeting). Pass them as direct async_task() "
            "keyword arguments instead."
        )

    if qraft_options is None:
        qraft_options = {}

    # Idempotency: a task already enqueued under this key is never
    # re-enqueued, regardless of its current status. Callers that want to
    # run again must use a new key.
    idempotency_key = qraft_options.get("idempotency_key")
    if idempotency_key:
        if qraft_options.get("idempotency_retry_dead"):
            _reclaim_dead_task_key(idempotency_key)
        existing_q2_task_id = _existing_task_for_key(idempotency_key)
        if existing_q2_task_id is not None:
            return existing_q2_task_id

    priority = qraft_options.get("priority", TaskPriority.DEFAULT)
    if priority not in TaskPriority.values:
        raise ValueError(
            f"Invalid priority {priority!r}; must be one of {TaskPriority.values}"
        )

    # Extract Qraft hook configuration
    success_hook = qraft_options.get("success_hook")
    failure_hook = qraft_options.get("failure_hook")

    # Handle overlap with legacy `hook`
    if hook and (success_hook or failure_hook):
        warnings.warn(
            "Both `hook` and Qraft hooks provided. Ignoring legacy `hook`. "
            "Use `success_hook`/`failure_hook` in qraft_options instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        hook = None
    elif hook and not (success_hook or failure_hook):
        success_hook = hook
        warnings.warn(
            "`hook` is deprecated. Please use `success_hook` in qraft_options.",
            DeprecationWarning,
            stacklevel=2,
        )

    # Build retry policy
    retry_policy = RetryPolicy.from_options(qraft_options)

    # Prepare Qraft metadata
    func_path = func if isinstance(func, str) else _importable_func_path(func)
    qraft_metadata = {
        "func": func_path,
        "task_args": list(args),  # Store for retry re-queueing
        "task_kwargs": kwargs,  # Store for retry re-queueing
        "success_hook": success_hook,
        "success_args": qraft_options.get("success_args", ()),
        "success_kwargs": qraft_options.get("success_kwargs", {}),
        "failure_hook": failure_hook,
        "failure_args": qraft_options.get("failure_args", ()),
        "failure_kwargs": qraft_options.get("failure_kwargs", {}),
        "retry_policy": retry_policy.to_dict() if retry_policy else {},
        "status": TaskStatus.RUNNING,
        "idempotency_key": idempotency_key,
        "priority": priority,
    }

    # Extract cluster routing from qraft_options
    cluster = qraft_options.get("cluster")

    # Priority lanes: only takes effect against a cluster running
    # qraft.brokers.QraftOrmBroker (see that module's docstring). The lane is
    # keyed off the target cluster because django_q drops `cluster=` whenever
    # an explicit broker is supplied. Explicit `broker=` from the caller wins
    # over priority routing.
    if broker is None:
        list_key = priority_list_key(priority, cluster)
        if list_key is not None:
            broker = QraftOrmBroker(list_key=list_key)

    # Build Q2 task options. The remaining ones below are only added when the
    # caller gave a value, since passing e.g. timeout=None differs from
    # omitting it (Django-Q2 checks if the key exists in the task dict).
    q2_kwargs = {
        "hook": "qraft.hooks.qraft_hook_handler",
        "q_options": q_options,
        # Qraft owns retries; without this the broker never acknowledges a
        # failed message and redelivers attempt N while attempt N+1 is
        # already scheduled.
        "ack_failure": True,
        # Forced True unconditionally (save=False is already rejected
        # above): when Conf.SAVE_LIMIT < 0, Django-Q2's monitor skips saving
        # a successful Task row unless the task itself asks for save=True.
        # Hook delivery is a post_save receiver on that row, so no row means
        # no hook fires - and the lease reaper later mistakes the (already
        # succeeded, but unrecorded) task for an orphan and re-executes it.
        "save": True,
        **kwargs,
    }
    if cluster is not None:
        q2_kwargs["cluster"] = cluster
    # Preserve user's task_name if provided (no longer overwritten for linkage)
    # Hook handler uses query-based lookup via QraftTaskAttempt.q2_task_id
    if task_name is not None:
        q2_kwargs["task_name"] = task_name
    if group is not None:
        q2_kwargs["group"] = group
    if timeout is not None:
        q2_kwargs["timeout"] = timeout
    if broker is not None:
        q2_kwargs["broker"] = broker

    # A concurrent call with the same idempotency_key can lose the race here:
    # IntegrityError aborts that transaction, so the fallback lookup below
    # runs outside it, against the winner's already-committed row.
    try:
        qraft_task, q2_task_id = _create_and_enqueue(
            qraft_metadata, func, args, q2_kwargs
        )
    except IntegrityError:
        existing_q2_task_id = (
            _existing_task_for_key(idempotency_key) if idempotency_key else None
        )
        if existing_q2_task_id is None:
            raise
        return existing_q2_task_id

    _logger.debug(
        "Created QraftTask %s with attempt 1 (q2: %s, name: %s) for func %s",
        qraft_task.id,
        q2_task_id,
        task_name or "(none)",
        func_path,
    )

    return q2_task_id


def _create_workflow_task(
    func: str,
    args: list,
    kwargs: dict,
    qraft_options: dict,
    qraft_iter_id: str | None = None,
    qraft_batch_id: str | None = None,
):
    """
    Internal helper to create and queue a QraftTask for workflow execution.

    This function is used by workflow primitives (Chain, Iter, Batch) to create
    tasks with parent linkage. Unlike async_task(), this expects serialized args
    and handles workflow-specific FK linkage.

    Args:
        func: Dotted path to the task function
        args: Positional arguments (already serialized as list)
        kwargs: Keyword arguments (already serialized as dict)
        qraft_options: Qraft-specific options (retry policy, etc.)
        qraft_iter_id: UUID of parent QraftIter (if part of iter)
        qraft_batch_id: UUID of parent QraftBatch (if part of batch)

    Returns:
        QraftTask: Created task instance

    Note:
        - Chain tasks don't use this pattern (OneToOne from ChainStep instead)
        - Workflow tasks don't have task-level hooks (workflow-level only)
    """
    from qraft.models import QraftBatchModel, QraftIterModel

    retry_policy = RetryPolicy.from_options(qraft_options)

    qraft_metadata = {
        "func": func,
        "task_args": args,
        "task_kwargs": kwargs,
        "retry_policy": retry_policy.to_dict() if retry_policy else {},
        "status": TaskStatus.RUNNING,
        # Workflow tasks don't have task-level hooks (workflow-level only)
        "success_hook": None,
        "failure_hook": None,
    }
    if qraft_iter_id:
        qraft_metadata["qraft_iter"] = QraftIterModel.objects.get(id=qraft_iter_id)
    elif qraft_batch_id:
        qraft_metadata["qraft_batch"] = QraftBatchModel.objects.get(id=qraft_batch_id)

    # Workflow ID doubles as the Q2 group, for result aggregation
    workflow_id = qraft_iter_id or qraft_batch_id
    q2_kwargs = {
        "hook": "qraft.hooks.qraft_hook_handler",
        "group": str(workflow_id) if workflow_id else None,
        # See async_task(): Qraft's retries must not race Django-Q2 redelivery.
        "ack_failure": True,
        # See async_task(): under SAVE_LIMIT < 0 a successful task saves no
        # Task row unless it asks, and no row means the hook never fires -
        # here that leaves the member uncounted and the workflow hung.
        "save": True,
        **kwargs,
    }
    cluster = qraft_options.get("cluster")
    if cluster is not None:
        q2_kwargs["cluster"] = cluster

    qraft_task, q2_task_id = _create_and_enqueue(qraft_metadata, func, args, q2_kwargs)

    _logger.debug(
        "Created workflow QraftTask %s (q2: %s, iter=%s, batch=%s) for func %s",
        qraft_task.id,
        q2_task_id,
        qraft_iter_id or "None",
        qraft_batch_id or "None",
        func,
    )

    return qraft_task
