"""
Enhanced async_task implementation with dual-phase hooks and rich retry policies.
"""

import logging
import warnings
from typing import Any, Callable

from django_q.tasks import async_task as q2_async_task

from qraft.models import QraftTask, QraftTaskAttempt, TaskStatus
from qraft.retry import RetryPolicy

_logger = logging.getLogger("qraft")


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
        **kwargs:
            Extra keyword arguments passed to the task function.

    Returns:
        str: Django-Q2 Task ID (can be used to look up QraftTaskAttempt).
    """
    if q_options is None:
        q_options = {}

    if qraft_options is None:
        qraft_options = {}

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
    }

    # Create QraftTask first so we can link it to the attempt
    qraft_task = QraftTask.objects.create(**qraft_metadata)

    # Build Q2 task options, only including non-None values
    # This is important because passing save=None is different from not passing save
    # (Django-Q2 checks if "save" key exists in task dict)
    q2_kwargs = {
        "hook": "qraft.hooks.qraft_hook_handler",
        "q_options": q_options,
        **kwargs,
    }
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

    # Queue via Django-Q2 with Qraft's global hook handler
    q2_task_id = q2_async_task(func, *args, **q2_kwargs)

    # Create initial attempt record
    QraftTaskAttempt.objects.create(
        qraft_task=qraft_task,
        attempt_number=1,
        q2_task_id=q2_task_id,
    )

    _logger.debug(
        "Created QraftTask %s with attempt 1 (q2: %s, name: %s) for func %s",
        qraft_task.id,
        q2_task_id,
        task_name or "(none)",
        func_path,
    )

    return q2_task_id
