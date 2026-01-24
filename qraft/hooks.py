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

from .conf import get_conf
from .retry import RetryPolicy

_logger = logging.getLogger("django-q")

# Regex to extract exception class from Python traceback
# Matches lines like "module.path.ExceptionClass: message" or "ExceptionClass: message"
_EXC_PATTERN = re.compile(r"^([\w.]+):\s", re.MULTILINE)


def _extract_exception_class(result: str | None) -> str | None:
    """
    Extract exception class name from a task result string.

    The result format from worker.py is: "<message> : <traceback>"
    The traceback ends with a line like: "module.ExceptionClass: message"

    Uses regex matching against Python's standardized traceback format.

    Args:
        result: Task result string containing error info

    Returns:
        Exception class name (e.g., "TransientError") or None if not parseable
    """
    if not result or not isinstance(result, str):
        return None

    matches = _EXC_PATTERN.findall(result)
    if matches:
        # Take the last match (the actual exception, not chained causes)
        exc_path = matches[-1]
        # Return just the class name (after the last dot if fully qualified)
        return exc_path.rsplit(".", 1)[-1]

    return None


def _parse_qraft_marker(marker: str) -> tuple[str, int] | None:
    """
    Parse a Qraft marker string into (task_id, attempt_number).

    Marker format: "qraft:{task_id}:{attempt_number}"

    Returns None if the marker is invalid.
    """
    parts = marker.split(":")
    if len(parts) != 3 or parts[0] != "qraft":
        return None
    try:
        return parts[1], int(parts[2])
    except (ValueError, IndexError):
        return None


def qraft_hook_handler(q2_task):
    """
    Global hook handler that dispatches Qraft hooks based on task outcome.

    This is registered as the hook for all Qraft-enhanced tasks. Django-Q2
    calls this with the task object after completion.

    Identifies Qraft tasks via:
    1. Query-based lookup by q2_task.id (fast path for initial tasks)
    2. Fallback to task_name parsing for retry tasks (which aren't pre-created)

    Args:
        q2_task: Django-Q2 Task object passed by the monitor process
    """
    from .models import QraftTask, QraftTaskAttempt, TaskStatus

    # Fast path: Try query-based lookup first (for initial tasks created via async_task)
    try:
        attempt = QraftTaskAttempt.objects.select_related("qraft_task").get(
            q2_task_id=q2_task.id
        )
        qraft_task = attempt.qraft_task

        _logger.debug(
            "Processing QraftTask %s attempt %d (q2: %s) via query",
            qraft_task.id,
            attempt.attempt_number,
            q2_task.id,
        )

    except QraftTaskAttempt.DoesNotExist:
        # Fallback: Parse task_name for retry tasks (scheduled via Schedule, not async_task)
        if not q2_task.name or not q2_task.name.startswith("qraft:"):
            _logger.debug(
                "No Qraft marker or attempt found for task %s, skipping", q2_task.id
            )
            return

        parsed = _parse_qraft_marker(q2_task.name)
        if not parsed:
            _logger.warning(
                "Invalid Qraft marker '%s' for task %s", q2_task.name, q2_task.id
            )
            return

        qraft_task_id, attempt_number = parsed

        # Look up QraftTask
        try:
            qraft_task = QraftTask.objects.get(id=qraft_task_id)
        except QraftTask.DoesNotExist:
            _logger.error("QraftTask %s not found", qraft_task_id)
            return

        # Create attempt record for retry (initial attempts are created in async_task)
        attempt, created = QraftTaskAttempt.objects.get_or_create(
            qraft_task=qraft_task,
            attempt_number=attempt_number,
            defaults={"q2_task_id": q2_task.id},
        )

        # If attempt existed but q2_task_id differs (shouldn't happen, but defensive)
        if not created and attempt.q2_task_id != q2_task.id:
            attempt.q2_task_id = q2_task.id
            attempt.save(update_fields=["q2_task_id"])

        _logger.debug(
            "Processing QraftTask %s attempt %d (q2: %s, created=%s) via task_name",
            qraft_task.id,
            attempt_number,
            q2_task.id,
            created,
        )

    # Update attempt outcome and task status atomically
    with transaction.atomic():
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

    # Dispatch hooks via dispatcher
    dispatcher = HookDispatcher(qraft_task, attempt)
    dispatcher.dispatch(q2_task)


class HookDispatcher:
    """
    Handles dual-phase hook dispatching for task completion.
    """

    def __init__(self, qraft_task, attempt):
        self.qraft_task = qraft_task
        self.attempt = attempt

    def dispatch(self, q2_task):
        """
        Main dispatch method called after task completion.

        Handles retry logic for failed tasks before dispatching hooks.
        If a retry is scheduled, the failure hook is NOT called.

        Args:
            q2_task: Django-Q2 Task model instance
        """
        if q2_task.success:
            self._dispatch_success_hook(q2_task)
        else:
            # Try to retry before dispatching failure hook
            if self._handle_retry(q2_task):
                # Retry was scheduled, don't call failure hook yet
                return
            self._dispatch_failure_hook(q2_task)

    def _dispatch_success_hook(self, q2_task):
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

    def _dispatch_failure_hook(self, q2_task):
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
        conf = get_conf()

        if conf.sync_hooks:
            self._call_hook_sync(hook_path, args, kwargs, task, hook_type)
        else:
            self._call_hook_async(hook_path, args, kwargs, task, hook_type)

    def _call_hook_async(self, hook_path, args, kwargs, task, hook_type):
        """
        Queue hook as an async task over workers.

        Creates a HookDispatch record for idempotency and tracking.
        """
        from .models import HookDispatch

        try:
            # Check if hook already dispatched (idempotency guard)
            # Use a placeholder for q2_task_id to avoid unique constraint issues
            # Format: p-{short_uuid} to fit in 32 chars (2 + 30 = 32)
            placeholder_id = f"p-{uuid.uuid4().hex[:30]}"
            with transaction.atomic():
                hook_dispatch, created = HookDispatch.objects.get_or_create(
                    qraft_task=self.qraft_task,
                    hook_type=hook_type,
                    defaults={
                        "hook_path": hook_path,
                        "q2_task_id": placeholder_id,
                    },
                )

            if not created:
                _logger.debug(
                    "Hook %s already dispatched for QraftTask %s, skipping",
                    hook_type,
                    self.qraft_task.id,
                )
                return

            # Queue hook as async task (no Qraft hook handler to prevent recursion)
            q2_task_id = q2_async_task(
                hook_path,
                *args,
                **kwargs,
                task_name=f"hook:{hook_type}:{self.qraft_task.id}",
                hook=None,  # No hook on hook tasks - prevents recursion
            )

            # Update with actual Q2 task ID
            hook_dispatch.q2_task_id = q2_task_id
            hook_dispatch.save(update_fields=["q2_task_id"])

            _logger.debug(
                "Queued %s hook '%s' as task %s for QraftTask %s",
                hook_type,
                hook_path,
                q2_task_id,
                self.qraft_task.id,
            )

        except Exception as e:
            _logger.error(
                "Failed to dispatch %s hook for QraftTask %s: %s",
                hook_type,
                self.qraft_task.id,
                e,
                exc_info=True,
            )

    def _call_hook_sync(self, hook_path, args, kwargs, task, hook_type):
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

    def _handle_retry(self, q2_task) -> bool:
        """
        Handle retry logic based on policy.

        Args:
            q2_task: Django-Q2 Task instance that failed

        Returns:
            True if retry was scheduled, False otherwise
        """
        from .models import TaskStatus

        policy = self.qraft_task.retry_policy
        if not policy:
            return False

        retry_policy = RetryPolicy.from_dict(policy)

        # Use exception class from the attempt (already extracted and stored)
        exc_class_name = self.attempt.exception_class

        current_attempt = self.qraft_task.attempt_count

        if retry_policy.should_retry(current_attempt, exc_class_name):
            # Schedule retry with backoff delay
            retry_policy.schedule_retry(self.qraft_task)
            return True

        # All retries exhausted
        self.qraft_task.status = TaskStatus.EXHAUSTED
        self.qraft_task.save(update_fields=["status", "date_updated"])

        _logger.info(
            "QraftTask %s exhausted all %d retry attempts",
            self.qraft_task.id,
            current_attempt,
        )

        return False
