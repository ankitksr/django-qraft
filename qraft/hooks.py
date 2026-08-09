"""
Dual phase hook dispatcher.
Handles success and failure hooks with custom arguments.

Hooks are dispatched as async tasks over workers by default, preventing
the monitor process from becoming a bottleneck.
"""

import logging
import re
import uuid

from django.db import transaction
from django.utils.module_loading import import_string
from django_q.tasks import async_task as q2_async_task

from .conf import executing_cluster, get_conf
from .retry import QRAFT_MARKER_PREFIX, handle_task_retry

_logger = logging.getLogger("django-q")

# Traceback tail lines look like "module.path.ExceptionClass: message"
_EXC_PATTERN = re.compile(r"^([\w.]+):\s", re.MULTILINE)

# "p-{hex}" placeholder must fit in the q2_task_id CharField(max_length=32)
_PLACEHOLDER_HEX_LEN = 30


def _extract_exception_class(result: str | None) -> str | None:
    """
    Extract exception class name from a task result string.

    The result format from worker.py is: "<message> : <traceback>"
    The traceback ends with a line like: "module.ExceptionClass: message"

    Args:
        result: Task result string containing error info

    Returns:
        Exception class name (e.g., "TransientError") or None if not parseable
    """
    if not result or not isinstance(result, str):
        return None

    matches = _EXC_PATTERN.findall(result)
    if not matches:
        return None

    # Last match is the actual exception, not a chained cause; strip any
    # module prefix to leave the bare class name.
    return matches[-1].rsplit(".", 1)[-1]


def dispatch_hook_once(dispatch_model, lookup: dict, hook_path: str, enqueue) -> None:
    """
    Queue a hook task at most once per `lookup`, tracked by `dispatch_model`.

    A placeholder q2_task_id is written inside the same get_or_create that
    claims the (unique) lookup, so two racing dispatchers can't both enqueue.
    The row is removed again if enqueueing fails, leaving a later retry free
    to dispatch.

    Args:
        dispatch_model: HookDispatch or WorkflowHookDispatch
        lookup: Field values forming the model's uniqueness constraint
        hook_path: Dotted import path to the hook function
        enqueue: Zero-arg callable that queues the hook and returns its q2 id
    """
    placeholder_id = f"p-{uuid.uuid4().hex[:_PLACEHOLDER_HEX_LEN]}"
    try:
        with transaction.atomic():
            dispatch, created = dispatch_model.objects.get_or_create(
                **lookup,
                defaults={"hook_path": hook_path, "q2_task_id": placeholder_id},
            )

        if not created:
            _logger.debug("%s already dispatched, skipping", dispatch)
            return

        dispatch.q2_task_id = enqueue()
        dispatch.save(update_fields=["q2_task_id"])

        _logger.debug("Queued %s as task %s", dispatch, dispatch.q2_task_id)

    except Exception:
        dispatch_model.objects.filter(**lookup, q2_task_id=placeholder_id).delete()
        _logger.exception(
            "Failed to dispatch hook '%s' for %s", hook_path, dispatch_model.__name__
        )


def _parse_qraft_marker(marker: str) -> tuple[str, int] | None:
    """
    Parse a Qraft marker string into (task_id, attempt_number).

    Marker format: "qraft:{task_id}:{attempt_number}"

    Returns None if the marker is invalid or the UUID is malformed.
    """
    parts = marker.split(":")
    if len(parts) != 3 or parts[0] != QRAFT_MARKER_PREFIX:
        return None
    try:
        task_id = parts[1]
        # Validate UUID format
        uuid.UUID(task_id)
        return task_id, int(parts[2])
    except (ValueError, IndexError):
        return None


def attempt_from_marker(task_name: str | None, q2_task_id: str):
    """
    Get or create the QraftTaskAttempt a marker-carrying task belongs to.

    Compatibility path, kept for one release. Qraft's own dispatcher stamps
    the q2 task id on the attempt row before enqueueing, so every delivery it
    makes resolves through the fast lookup and never reaches here. What still
    does is a pre-2.0 Django-Q2 `Schedule` row written by the previous version
    and fired after the upgrade: that run has no attempt row of its own, and
    the marker in its task_name is the only link back to the QraftTask.

    Called both from the execution lease (worker-side, at pre_execute) and
    from the hook handler (monitor-side, at completion) - whichever runs
    first creates the row.

    Returns:
        QraftTaskAttempt, or None if the name carries no usable Qraft marker.
    """
    from .models import QraftTask, QraftTaskAttempt
    from .models.tasks import AttemptState

    if not task_name or not task_name.startswith(f"{QRAFT_MARKER_PREFIX}:"):
        return None

    parsed = _parse_qraft_marker(task_name)
    if not parsed:
        _logger.warning("Invalid Qraft marker '%s' for task %s", task_name, q2_task_id)
        return None

    qraft_task_id, attempt_number = parsed
    try:
        qraft_task = QraftTask.objects.get(id=qraft_task_id)
    except QraftTask.DoesNotExist:
        _logger.error("QraftTask %s not found", qraft_task_id)
        return None

    attempt, created = QraftTaskAttempt.objects.get_or_create(
        qraft_task=qraft_task,
        attempt_number=attempt_number,
        defaults={"q2_task_id": q2_task_id, "cluster": executing_cluster()},
    )

    if not created:
        # get_or_create only caches the parent on the create branch
        attempt.qraft_task = qraft_task
        # A row already here is normally the dispatcher's, stamped and QUEUED
        # already, so this is a no-op. It still fires when a legacy Schedule
        # delivery lands on an attempt the dispatcher had scheduled but not
        # yet claimed, which is the state that has to be adopted.
        if attempt.q2_task_id != q2_task_id:
            attempt.q2_task_id = q2_task_id
            attempt.state = AttemptState.QUEUED
            attempt.save(update_fields=["q2_task_id", "state"])

    _logger.debug(
        "Resolved QraftTask %s attempt %d (q2: %s, created=%s) via task_name",
        qraft_task.id,
        attempt_number,
        q2_task_id,
        created,
    )
    return attempt


def _resolve_attempt(q2_task):
    """
    Find the QraftTaskAttempt a finished Django-Q2 task belongs to.

    Initial attempts are pre-created by async_task() and found by q2 task id.
    Retries are queued through a Schedule instead, so they carry a Qraft
    marker in their task_name; the lease normally creates their attempt row
    at pre_execute, and this falls back to creating it here.

    Returns:
        QraftTaskAttempt, or None if the task isn't a Qraft task.
    """
    from .models import QraftTaskAttempt

    try:
        attempt = QraftTaskAttempt.objects.select_related("qraft_task").get(
            q2_task_id=q2_task.id
        )
    except QraftTaskAttempt.DoesNotExist:
        pass
    else:
        _logger.debug(
            "Processing QraftTask %s attempt %d (q2: %s) via query",
            attempt.qraft_task_id,
            attempt.attempt_number,
            q2_task.id,
        )
        return attempt

    attempt = attempt_from_marker(q2_task.name, q2_task.id)
    if attempt is None:
        _logger.debug(
            "No Qraft marker or attempt found for task %s, skipping", q2_task.id
        )
    return attempt


def qraft_hook_handler(q2_task):
    """
    Global hook handler that dispatches Qraft hooks based on task outcome.

    This is registered as the hook for all Qraft-enhanced tasks. Django-Q2
    calls this with the task object after completion.

    Args:
        q2_task: Django-Q2 Task object passed by the monitor process
    """
    from .dispatchers import route_workflow_completion
    from .models import QraftTask, TaskStatus

    attempt = _resolve_attempt(q2_task)
    if attempt is None:
        return
    qraft_task = attempt.qraft_task

    # Update attempt outcome and task status atomically
    with transaction.atomic():
        # Re-fetch with lock to prevent concurrent status updates
        qraft_task = QraftTask.objects.select_for_update().get(id=qraft_task.id)

        attempt.success = q2_task.success
        attempt.date_completed = q2_task.stopped
        if not q2_task.success:
            attempt.exception_class = _extract_exception_class(q2_task.result)
        attempt.save(update_fields=["success", "date_completed", "exception_class"])

        # Update task status (may be updated to EXHAUSTED or PENDING by retry handler)
        qraft_task.status = (
            TaskStatus.SUCCEEDED if q2_task.success else TaskStatus.FAILED
        )
        qraft_task.save(update_fields=["status", "date_updated"])

    # A scheduled retry means the task is not terminal yet
    if not q2_task.success and handle_task_retry(
        qraft_task, attempt, result_text=q2_task.result
    ):
        return

    # Workflow tasks get workflow-level hooks only, never task-level ones
    if not route_workflow_completion(qraft_task, attempt):
        HookDispatcher(qraft_task, attempt).dispatch(q2_task)


class HookDispatcher:
    """
    Handles dual-phase hook dispatching for task completion.
    """

    def __init__(self, qraft_task, attempt):
        self.qraft_task = qraft_task
        self.attempt = attempt

    def dispatch(self, q2_task):
        """
        Dispatch appropriate hook based on task outcome.

        This method assumes retry logic has already been handled by the caller.
        It only dispatches success or failure hooks.

        Args:
            q2_task: Django-Q2 Task model instance
        """
        if q2_task.success:
            self.dispatch_success_hook(q2_task)
        else:
            self.dispatch_failure_hook(q2_task)

    def dispatch_success_hook(self, q2_task):
        """Dispatch success hook if configured."""
        if not self.qraft_task.success_hook:
            return

        self._call_hook(
            hook_path=self.qraft_task.success_hook,
            args=self.qraft_task.success_args or [],
            kwargs=self.qraft_task.success_kwargs or {},
            task=q2_task,
            hook_type="success",
        )

    def dispatch_failure_hook(self, q2_task):
        """Dispatch failure hook if configured."""
        if not self.qraft_task.failure_hook:
            return

        self._call_hook(
            hook_path=self.qraft_task.failure_hook,
            args=self.qraft_task.failure_args or [],
            kwargs=self.qraft_task.failure_kwargs or {},
            task=q2_task,
            hook_type="failure",
        )

    def _call_hook(self, hook_path, args, kwargs, task, hook_type):
        """
        Dispatch a hook function as an async task or call it synchronously.

        By default, hooks are queued as async tasks to run over workers,
        preventing the monitor from becoming a bottleneck. Set sync_hooks=True
        in settings for legacy synchronous behavior.

        Args:
            hook_path: Dotted import path to hook function
            args: Positional arguments for hook
            kwargs: Keyword arguments for hook
            task: Django-Q2 Task instance
            hook_type: 'success' or 'failure' for logging
        """
        if get_conf().sync_hooks:
            self._call_hook_sync(hook_path, args, kwargs, hook_type)
        else:
            self._call_hook_async(hook_path, args, kwargs, hook_type)

    def _call_hook_async(self, hook_path, args, kwargs, hook_type):
        """
        Queue hook as an async task over workers.

        Creates a HookDispatch record for idempotency and tracking.
        """
        from .models import HookDispatch

        dispatch_hook_once(
            HookDispatch,
            {"qraft_task": self.qraft_task, "hook_type": hook_type},
            hook_path,
            lambda: q2_async_task(
                hook_path,
                *args,
                **kwargs,
                task_name=f"hook:{hook_type}:{self.qraft_task.id}",
                hook=None,  # No hook on hook tasks - prevents recursion
                cluster=executing_cluster(),
                # A hook that keeps failing must not be redelivered forever;
                # nothing above this layer retries it.
                ack_failure=True,
            ),
        )

    def _call_hook_sync(self, hook_path, args, kwargs, hook_type):
        """
        Call hook synchronously (legacy behavior).

        Used when sync_hooks=True in settings.
        """
        try:
            hook_func = import_string(hook_path)
            hook_func(*args, **kwargs)

            _logger.debug(
                "Successfully called %s hook '%s' for QraftTask %s",
                hook_type,
                hook_path,
                self.qraft_task.id,
            )

        except ImportError as e:
            _logger.error(
                "Failed to import %s hook '%s' for QraftTask %s: %s",
                hook_type,
                hook_path,
                self.qraft_task.id,
                e,
            )
        except Exception as e:
            _logger.error(
                "%s hook '%s' failed for QraftTask %s: %s",
                hook_type.title(),
                hook_path,
                self.qraft_task.id,
                e,
                exc_info=True,
            )
