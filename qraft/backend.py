"""
django.tasks (DEP 14, Django 6.0+) backend that engines Django's official
Tasks API on top of Qraft's existing pipeline (``qraft.tasks.async_task()``
and the ``QraftTask``/``QraftTaskAttempt`` models).

Coded against ``django.tasks.backends.base.BaseTaskBackend``,
``django.tasks.base`` and ``django.tasks.backends.immediate.ImmediateBackend``
as shipped in Django 6.0
(https://docs.djangoproject.com/en/6.0/ref/tasks/,
https://docs.djangoproject.com/en/6.0/topics/tasks/). The prose docs don't
show a full custom-backend example, so behavior here (feature flags,
``validate_task()`` semantics, ``TaskResult``/``Task``/``TaskContext``
dataclass shape, and how ``ImmediateBackend`` builds/injects ``TaskContext``)
was confirmed directly against the Django 6.0 source
(``django/tasks/backends/base.py``, ``django/tasks/base.py``,
``django/tasks/backends/immediate.py``).

``django.tasks`` only exists on Django >= 6.0, so the import below is
guarded: this module stays import-safe for the rest of the package on older
Django, and only raises once Django actually tries to instantiate this
backend (i.e. when it's named in ``TASKS``).
"""

import logging

from django.core.exceptions import ImproperlyConfigured, ValidationError
from django.db import transaction
from django.utils.module_loading import import_string
from django_q.tasks import async_task as q2_async_task

try:
    from django.tasks.backends.base import BaseTaskBackend
    from django.tasks.base import (
        DEFAULT_TASK_QUEUE_NAME,
        Task,
        TaskContext,
        TaskError,
        TaskResult,
        TaskResultStatus,
    )
    from django.tasks.exceptions import TaskResultDoesNotExist
except ImportError as exc:
    raise ImproperlyConfigured(
        "qraft.backend.QraftTaskBackend requires Django 6.0+ (django.tasks, "
        "DEP 14). Upgrade Django or use qraft.tasks.async_task() directly."
    ) from exc

from qraft.brokers import QraftOrmBroker, priority_lanes_available, priority_list_key
from qraft.conf import executing_cluster
from qraft.models import QraftTask, QraftTaskAttempt, TaskStatus
from qraft.models.tasks import TaskPriority
from qraft.runner import _resolve_target
from qraft.runner import run_task as run_task  # re-exported: resolved by dotted path
from qraft.tasks import async_task as qraft_async_task

_logger = logging.getLogger("qraft")

# Dotted path to run_task_with_context() below - queued in place of the real
# target function whenever Task.takes_context is set.
_CONTEXT_WRAPPER_PATH = "qraft.backend.run_task_with_context"
_PLAIN_WRAPPER_PATH = "qraft.backend.run_task"

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

    Coroutine tasks aren't wired into Qraft's pipeline, so they stay
    unsupported (``supports_async_task`` defaults to ``False`` on the base
    class) and are rejected by ``validate_task()`` before enqueueing.

    ``run_after`` (deferred execution) and ``takes_context`` (``TaskContext``
    injection) are supported - see ``_enqueue_deferred()`` and
    ``run_task_with_context()`` below.
    """

    supports_get_result = True
    supports_defer = True

    @property
    def supports_priority(self) -> bool:
        """
        Whether priority lanes actually reach a worker in this deployment.

        Qraft's own async_task() downgrades an unroutable priority to the
        default lane with a warning, because a task that runs at the wrong
        priority still runs. django.tasks asks a yes/no capability question
        instead, so answering honestly here lets Django reject the enqueue
        with a clear message rather than have the backend quietly ignore
        what the caller asked for.
        """
        return priority_lanes_available()

    def enqueue(self, task, args, kwargs):
        self.validate_task(task)

        lane = _lane_for_priority(task.priority)
        func_path = f"{task.func.__module__}.{task.func.__name__}"
        args = list(args)

        if task.run_after is not None:
            return self._enqueue_deferred(task, func_path, args, kwargs, lane)

        if task.takes_context:
            return self._enqueue_with_context(task, func_path, args, kwargs, lane)

        # The @task decorator replaces the module attribute with a Task
        # wrapper, so neither task.func (pickle identity) nor the dotted
        # path (resolves to the non-callable wrapper) can be queued
        # directly. run_task unwraps at execution time.
        q2_task_id = qraft_async_task(
            _PLAIN_WRAPPER_PATH,
            func_path,
            list(args),
            kwargs,
            qraft_options={"priority": lane},
        )
        attempt = QraftTaskAttempt.objects.select_related("qraft_task").get(
            q2_task_id=q2_task_id
        )
        qraft_task = attempt.qraft_task
        # Record the real target for get_result()/observability, not the
        # wrapper that Django-Q2 executes.
        qraft_task.func = func_path
        qraft_task.task_args = list(args)
        qraft_task.task_kwargs = kwargs
        qraft_task.save(update_fields=["func", "task_args", "task_kwargs"])
        return self._build_result(task, qraft_task)

    def _enqueue_with_context(self, task, func_path, args, kwargs, lane):
        """
        Enqueue a ``takes_context=True`` task for immediate execution.

        Queues ``run_task_with_context`` (this module) in place of the real
        target - Qraft dispatches the raw function straight to Django-Q2, so
        the ``TaskContext`` has to be built and injected worker-side, at the
        point the wrapper is actually called. ``QraftTask.func``/args/kwargs
        still record the real target, so ``get_result()`` reports it
        untouched.
        """
        broker = None
        list_key = priority_list_key(lane)
        if list_key is not None:
            broker = QraftOrmBroker(list_key=list_key)

        with transaction.atomic():
            qraft_task = QraftTask.objects.create(
                func=func_path,
                task_args=args,
                task_kwargs=kwargs,
                status=TaskStatus.RUNNING,
                priority=lane,
            )
            # ack_failure: Qraft owns retries, so Django-Q2 must not also
            # redeliver a failed message (see qraft.tasks.async_task).
            q2_kwargs = {
                "hook": "qraft.hooks.qraft_hook_handler",
                "ack_failure": True,
            }
            if broker is not None:
                q2_kwargs["broker"] = broker
            q2_task_id = q2_async_task(
                _CONTEXT_WRAPPER_PATH,
                func_path,
                str(qraft_task.id),
                self.alias,
                args,
                kwargs,
                **q2_kwargs,
            )
            QraftTaskAttempt.objects.create(
                qraft_task=qraft_task,
                attempt_number=1,
                q2_task_id=q2_task_id,
                cluster=executing_cluster(),
            )
        return self._build_result(task, qraft_task)

    def _enqueue_deferred(self, task, func_path, args, kwargs, lane):
        """
        Enqueue a ``run_after``-deferred task.

        Mirrors ``RetryPolicy.schedule_retry()``: attempt 1 is created up
        front as a SCHEDULED row due at ``task.run_after``, and Qraft's
        dispatcher enqueues it then. The ``QraftTask`` stays ``PENDING``
        until that happens, so ``get_result()`` keeps reporting ``READY``.

        ``dispatch_func`` only has to be set for ``takes_context``: every
        other deferred task runs through the same ``run_task`` unwrapping
        that the immediate path uses, which the dispatcher applies by default.
        """
        # Imported here, not at module scope: a worker resolves this module by
        # dotted path mid-run, and a fresh module-level edge into the model
        # layer can race a concurrent import in another thread of the same
        # process (seen as a partially-initialised qraft.models.tasks).
        from qraft.scheduler import schedule_attempt

        qraft_task = QraftTask.objects.create(
            func=func_path,
            task_args=args,
            task_kwargs=kwargs,
            status=TaskStatus.PENDING,
            priority=lane,
        )

        dispatch_func = dispatch_args = None
        if task.takes_context:
            dispatch_func = _CONTEXT_WRAPPER_PATH
            dispatch_args = [func_path, str(qraft_task.id), self.alias, args, kwargs]

        schedule_attempt(
            qraft_task,
            1,
            task.run_after,
            # The web process's own cluster, matching where the immediate
            # path would have sent it. A null here would have made the task
            # runnable only by a cluster named after the default prefix.
            cluster=executing_cluster(),
            dispatch_func=dispatch_func,
            dispatch_args=dispatch_args,
        )

        return self._build_result(task, qraft_task)

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

        # import_string returns the @task wrapper when the module attribute
        # was replaced by the decorator; Task validation wants the bare
        # function underneath.
        func = import_string(qraft_task.func)
        if isinstance(func, Task):
            func = func.func

        task = Task(
            priority=_PRIORITY_FOR_LANE[qraft_task.priority],
            func=func,
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
            # date_started (stamped by the lease at pre_execute), not
            # date_created: a SCHEDULED attempt's row exists long before any
            # worker touches it, and must not read as "started".
            started_at=first_attempt.date_started if first_attempt else None,
            last_attempted_at=latest_attempt.date_started if latest_attempt else None,
            finished_at=latest_attempt.date_completed if latest_attempt else None,
            args=qraft_task.task_args,
            kwargs=qraft_task.task_kwargs,
            backend=self.alias,
            errors=errors,
            # A deferred task's attempt exists before it is enqueued, so its
            # q2 id is still null at this point and there is no worker to name.
            worker_ids=[a.q2_task_id for a in attempts if a.q2_task_id],
        )
        if return_value is not None:
            object.__setattr__(result, "_return_value", return_value)
        return result


def run_task_with_context(func_path, qraft_task_id, backend_alias, args, kwargs):
    """
    Worker-side entry point for ``takes_context=True`` tasks.

    django.tasks backends inject a ``TaskContext`` as the target function's
    first positional argument (see ``ImmediateBackend._execute_task()`` in
    ``django/tasks/backends/immediate.py``); Qraft dispatches the raw
    function straight to Django-Q2, with no layer of its own in between to
    do that injection. ``QraftTaskBackend`` queues this function instead
    whenever ``Task.takes_context`` is set (see ``_enqueue_with_context()``
    and ``_enqueue_deferred()`` above), and it builds/injects the context
    here, at the point the worker actually calls it.

    ``qraft_task_id`` is resolved through ``get_result()`` rather than
    ``qraft.context.current_attempt()`` because the id is what the enqueue
    side has to hand: it is baked into the queued arguments at enqueue time,
    which is the only point that knows it for a deferred task.
    """
    from django.tasks import task_backends

    func = _resolve_target(func_path)
    task_result = task_backends[backend_alias].get_result(qraft_task_id)
    context = TaskContext(task_result=task_result)
    return func(context, *args, **kwargs)
