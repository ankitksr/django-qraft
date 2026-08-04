"""
django.tasks (DEP 14, Django 6.0+) backend that engines Django's official
Tasks API on top of Qraft's existing pipeline (``qraft.tasks.async_task()``
and the ``QraftTask``/``QraftTaskAttempt`` models).

Coded against ``django.tasks.backends.base.BaseTaskBackend`` and
``django.tasks.base`` as shipped in Django 6.0
(https://docs.djangoproject.com/en/6.0/ref/tasks/,
https://docs.djangoproject.com/en/6.0/topics/tasks/). The prose docs don't
show a full custom-backend example, so behavior here (feature flags,
``validate_task()`` semantics, ``TaskResult``/``Task`` dataclass shape) was
confirmed directly against the Django 6.0 source
(``django/tasks/backends/base.py``, ``django/tasks/base.py``).

``django.tasks`` only exists on Django >= 6.0, so the import below is
guarded: this module stays import-safe for the rest of the package on older
Django, and only raises once Django actually tries to instantiate this
backend (i.e. when it's named in ``TASKS``).
"""

import logging

from django.core.exceptions import ImproperlyConfigured, ValidationError
from django.utils.module_loading import import_string

try:
    from django.tasks.backends.base import BaseTaskBackend
    from django.tasks.base import (
        DEFAULT_TASK_QUEUE_NAME,
        Task,
        TaskError,
        TaskResult,
        TaskResultStatus,
    )
    from django.tasks.exceptions import InvalidTask, TaskResultDoesNotExist
except ImportError as exc:
    raise ImproperlyConfigured(
        "qraft.backend.QraftTaskBackend requires Django 6.0+ (django.tasks, "
        "DEP 14). Upgrade Django or use qraft.tasks.async_task() directly."
    ) from exc

from qraft.models import QraftTask, QraftTaskAttempt, TaskStatus
from qraft.models.tasks import TaskPriority
from qraft.tasks import async_task as qraft_async_task

_logger = logging.getLogger("qraft")

# django.tasks priority is an int in [-100, 100]; Qraft only has three lanes
# (see qraft.brokers). Anything above/below the default lane maps to
# high/low; the exact int is not preserved round-trip through get_result().
_LANE_FOR_PRIORITY = {
    1: TaskPriority.HIGH,
    0: TaskPriority.DEFAULT,
    -1: TaskPriority.LOW,
}
_PRIORITY_FOR_LANE = {
    TaskPriority.HIGH: 10,
    TaskPriority.DEFAULT: 0,
    TaskPriority.LOW: -10,
}

_RESULT_STATUS = {
    TaskStatus.PENDING: TaskResultStatus.READY,
    TaskStatus.RUNNING: TaskResultStatus.RUNNING,
    TaskStatus.SUCCEEDED: TaskResultStatus.SUCCESSFUL,
    TaskStatus.FAILED: TaskResultStatus.FAILED,
    TaskStatus.EXHAUSTED: TaskResultStatus.FAILED,
}


def _lane_for_priority(priority: int) -> str:
    return _LANE_FOR_PRIORITY.get((priority > 0) - (priority < 0), TaskPriority.DEFAULT)


class QraftTaskBackend(BaseTaskBackend):
    """
    Engine for django.tasks backed by Qraft's async_task()/QraftTask.

    Deferred execution (``run_after``) and coroutine tasks aren't wired into
    Qraft's pipeline, so both stay unsupported (``supports_defer`` and
    ``supports_async_task`` default to ``False`` on the base class) and are
    rejected by ``validate_task()`` before enqueueing.
    """

    supports_get_result = True
    supports_priority = True

    def validate_task(self, task):
        super().validate_task(task)
        if task.takes_context:
            # Qraft calls the target function directly with the stored
            # args/kwargs; there's no context-injection layer to supply a
            # leading `context` argument at execution time.
            raise InvalidTask(
                "QraftTaskBackend does not support tasks that take context."
            )

    def enqueue(self, task, args, kwargs):
        self.validate_task(task)

        lane = _lane_for_priority(task.priority)
        q2_task_id = qraft_async_task(
            task.func, *args, qraft_options={"priority": lane}, **kwargs
        )
        attempt = QraftTaskAttempt.objects.select_related("qraft_task").get(
            q2_task_id=q2_task_id
        )
        return self._build_result(task, attempt.qraft_task)

    def get_result(self, result_id):
        try:
            qraft_task = QraftTask.objects.get(id=result_id)
        except (
            QraftTask.DoesNotExist,
            ValidationError,
            ValueError,
            TypeError,
        ) as exc:
            raise TaskResultDoesNotExist(result_id) from exc

        task = Task(
            priority=_PRIORITY_FOR_LANE[qraft_task.priority],
            func=import_string(qraft_task.func),
            backend=self.alias,
            queue_name=DEFAULT_TASK_QUEUE_NAME,
            run_after=None,
        )
        return self._build_result(task, qraft_task)

    def _build_result(self, task, qraft_task):
        attempts = list(qraft_task.attempts.all())
        first_attempt = attempts[0] if attempts else None
        latest_attempt = attempts[-1] if attempts else None

        errors = []
        return_value = None
        if latest_attempt is not None and latest_attempt.exception_class:
            # QraftTaskAttempt only stores the bare exception class name
            # (see qraft.hooks._extract_exception_class), not a fully
            # qualified path, so TaskError.exception_class may fail to
            # import_string() when a caller reads it.
            errors.append(
                TaskError(
                    exception_class_path=latest_attempt.exception_class,
                    traceback="",
                )
            )
        elif latest_attempt is not None:
            q2_task = latest_attempt.get_q2_task()
            if q2_task is not None:
                return_value = q2_task.result

        result = TaskResult(
            task=task,
            id=str(qraft_task.id),
            status=_RESULT_STATUS[qraft_task.status],
            enqueued_at=qraft_task.date_created,
            started_at=first_attempt.date_created if first_attempt else None,
            last_attempted_at=latest_attempt.date_created if latest_attempt else None,
            finished_at=latest_attempt.date_completed if latest_attempt else None,
            args=qraft_task.task_args,
            kwargs=qraft_task.task_kwargs,
            backend=self.alias,
            errors=errors,
            worker_ids=[a.q2_task_id for a in attempts],
        )
        if return_value is not None:
            object.__setattr__(result, "_return_value", return_value)
        return result
