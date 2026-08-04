"""
Enhanced async_task implementation with dual-phase hooks and rich retry policies.
"""

import logging
import warnings
from typing import Any, Callable

from django.db import IntegrityError, transaction
from django_q.tasks import async_task as q2_async_task

from qraft.brokers import QraftOrmBroker, priority_list_key
from qraft.models import QraftTask, QraftTaskAttempt, TaskStatus
from qraft.models.tasks import TaskPriority
from qraft.retry import RetryPolicy

_logger = logging.getLogger("qraft")


def _existing_task_for_key(idempotency_key: str) -> str | None:
    """
    Return the q2_task_id of the task already enqueued under this key.

    Returns None if no QraftTask holds this key yet.
    """
    existing = QraftTask.objects.filter(idempotency_key=idempotency_key).first()
    if existing is None:
        return None

    attempt = existing.latest_attempt
    if attempt is None:
        return None

    _logger.info(
        "Idempotency key %s already used by QraftTask %s; skipping re-enqueue",
        idempotency_key,
        existing.id,
    )
    return attempt.q2_task_id


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
        group, save, timeout, ack_failure, sync, cached, broker:
            Same semantics as Django-Q2 async_task.
        task_name:
            Optional human-readable name for the task (appears in Django-Q2 admin).
            Fully preserved - Qraft uses database linkage instead of encoding metadata.
        q_options (dict, optional):
            Legacy Django-Q2 options.
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
                    The key is a permanent dedupe, not a "retry if failed"
                    signal - even a FAILED/EXHAUSTED task blocks re-enqueue.
                    Callers that want to run again must use a new key.
                priority ("high" | "default" | "low"): priority lane to
                    enqueue into. Only takes effect if the cluster's broker
                    is `qraft.brokers.QraftOrmBroker` (see that module's
                    docstring); otherwise the task is queued normally.
        **kwargs:
            Extra keyword arguments passed to the task function.

    Returns:
        str: Django-Q2 Task ID (can be used to look up QraftTaskAttempt).
    """
    if q_options is None:
        q_options = {}

    if qraft_options is None:
        qraft_options = {}

    # Idempotency: a task already enqueued under this key is never
    # re-enqueued, regardless of its current status. Callers that want to
    # run again must use a new key.
    idempotency_key = qraft_options.get("idempotency_key")
    if idempotency_key:
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
    func_path = func if isinstance(func, str) else f"{func.__module__}.{func.__name__}"
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
    # qraft.brokers.QraftOrmBroker (see that module's docstring). Explicit
    # `broker=` from the caller wins over priority routing.
    if broker is None:
        list_key = priority_list_key(priority)
        if list_key is not None:
            broker = QraftOrmBroker(list_key=list_key)

    # Build Q2 task options, only including non-None values
    # This is important because passing save=None is different from not passing save
    # (Django-Q2 checks if "save" key exists in task dict)
    q2_kwargs = {
        "hook": "qraft.hooks.qraft_hook_handler",
        "q_options": q_options,
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
    if save is not None:
        q2_kwargs["save"] = save
    if timeout is not None:
        q2_kwargs["timeout"] = timeout
    if ack_failure is not None:
        q2_kwargs["ack_failure"] = ack_failure
    if sync:
        q2_kwargs["sync"] = sync
    if cached is not None:
        q2_kwargs["cached"] = cached
    if broker is not None:
        q2_kwargs["broker"] = broker

    # Create QraftTask, queue to Q2, and create attempt atomically.
    # A concurrent call with the same idempotency_key can lose the race here:
    # IntegrityError aborts this transaction, so the fallback lookup below
    # runs outside it, against the winner's already-committed row.
    try:
        with transaction.atomic():
            qraft_task = QraftTask.objects.create(**qraft_metadata)
            q2_task_id = q2_async_task(func, *args, **q2_kwargs)
            QraftTaskAttempt.objects.create(
                qraft_task=qraft_task,
                attempt_number=1,
                q2_task_id=q2_task_id,
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

    # Build retry policy
    retry_policy = RetryPolicy.from_options(qraft_options)

    # Prepare Qraft metadata
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

    # Add workflow parent linkage
    if qraft_iter_id:
        qraft_metadata["qraft_iter"] = QraftIterModel.objects.get(id=qraft_iter_id)
    elif qraft_batch_id:
        qraft_metadata["qraft_batch"] = QraftBatchModel.objects.get(id=qraft_batch_id)

    # Queue via Django-Q2 with Qraft's global hook handler
    # Use workflow ID as group for result aggregation
    group = None
    if qraft_iter_id:
        group = str(qraft_iter_id)
    elif qraft_batch_id:
        group = str(qraft_batch_id)

    # Extract cluster routing from qraft_options
    cluster = qraft_options.get("cluster")

    # Build q2_async_task kwargs
    q2_kwargs = {
        "hook": "qraft.hooks.qraft_hook_handler",
        "group": group,
        **kwargs,
    }
    if cluster is not None:
        q2_kwargs["cluster"] = cluster

    # Create QraftTask, queue to Q2, and create attempt atomically
    with transaction.atomic():
        qraft_task = QraftTask.objects.create(**qraft_metadata)

        try:
            q2_task_id = q2_async_task(func, *args, **q2_kwargs)
        except Exception:
            raise

        QraftTaskAttempt.objects.create(
            qraft_task=qraft_task,
            attempt_number=1,
            q2_task_id=q2_task_id,
        )

    _logger.debug(
        "Created workflow QraftTask %s (q2: %s, iter=%s, batch=%s) for func %s",
        qraft_task.id,
        q2_task_id,
        qraft_iter_id or "None",
        qraft_batch_id or "None",
        func,
    )

    return qraft_task
