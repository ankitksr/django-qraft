"""
Universal worker-side task runner.

Resolves a dotted function path and calls it, transparently unwrapping the
django.tasks ``@task`` decorator's wrapper when present (see
``qraft.backend.QraftTaskBackend``) - the decorator replaces the module
attribute with a non-callable ``Task`` object, so the dotted path alone
resolves to something that can't be called directly.

``RetryPolicy.schedule_retry()`` and ``qraft.dlq.requeue()`` schedule this
function (rather than the stored dotted path) for every retry/requeue, so a
``@task``-decorated function keeps working across a retry. Importable on any
Django version - the ``django.tasks`` import is optional and only attempted
when actually resolving a target.
"""

import asyncio
import inspect
import logging

from django.utils.module_loading import import_string

_logger = logging.getLogger("qraft")


def _resolve_target(func_path: str):
    """Resolve a dotted path, unwrapping the django.tasks @task wrapper if present."""
    func = import_string(func_path)
    try:
        from django.tasks import Task
    except ImportError:
        return func
    return func.func if isinstance(func, Task) else func


def call_target(func, *args, **kwargs):
    """
    Call a resolved target, running a coroutine function to completion.

    ``asyncio.run()`` per call, not a shared loop: workers (both the standard
    process worker and threads in threaded_worker) have no running loop, and
    one coroutine occupies one worker slot exactly like a sync task, so the
    lease/timeout/recycle semantics are unchanged.
    """
    if inspect.iscoroutinefunction(func):
        return asyncio.run(func(*args, **kwargs))
    return func(*args, **kwargs)


class RedeliveredAttempt(Exception):
    """
    A delivery of an attempt that has already used its execution allowance.

    Raised instead of calling the function, so the attempt resolves through
    the normal failure path and the retry policy - not the broker's delivery
    loop - decides whether an attempt N+1 happens.
    """


def guard_delivery() -> None:
    """
    Refuse a delivery the execution claim rejected.

    Django-Q2 redelivers any message it never got an acknowledgement for, so
    a monitor crash re-runs a task whose attempt is still unresolved. The
    claim lives in `qraft.lease.claim_delivery()`; this is the point where
    refusing it becomes an exception, because a `pre_execute` receiver
    cannot raise (django_q's worker wraps none around the send).

    A delivery with no Qraft attempt behind it - a plain django_q task, a
    hook task - is never refused.
    """
    from qraft import context, lease

    q2_task_id = context.current_q2_task_id()
    if not q2_task_id:
        return
    if lease.claim_delivery(q2_task_id) == lease.REFUSED_DELIVERY:
        raise RedeliveredAttempt(
            f"q2 task {q2_task_id} is a repeat delivery of an attempt that has "
            "already executed; not running it again"
        )


def close_lease() -> None:
    """
    Record that the target finished and stop heartbeating for it.

    The lease's liveness signal answers "is the worker alive", and until here
    that was read as "is the attempt progressing". The two come apart when the
    Django-Q2 monitor dies holding the result queue's write lock: the worker
    returns from the task function, blocks forever in `result_queue.put()`, and
    the heartbeat thread keeps refreshing a lease for work that is already
    over. The reaper's liveness test then says "alive" - correctly, and
    uselessly - and the attempt is never resolved.

    `returned_at` is stamped first so an attempt that is reaped after this
    point carries the evidence that its work ran; stopping the heartbeat is
    what lets the reaper get to it at all. Neither step may fail the task: the
    function has already run, and its result is what the caller is owed.
    """
    from django.utils import timezone

    from qraft import context, lease

    q2_task_id = context.current_q2_task_id()
    if not q2_task_id:
        return
    try:
        from qraft.models import QraftTaskAttempt

        QraftTaskAttempt.objects.filter(
            q2_task_id=q2_task_id, success__isnull=True
        ).update(returned_at=timezone.now())
    except Exception:
        _logger.exception(
            "Could not stamp returned_at for q2 task %s; the attempt stays "
            "indistinguishable from a worker that died mid-execution",
            q2_task_id,
        )
    lease.stop_heartbeat(q2_task_id)


def run_task(func_path: str, args, kwargs):
    """Worker-side entry point: resolve func_path and call it with args/kwargs."""
    guard_delivery()
    try:
        return call_target(_resolve_target(func_path), *args, **kwargs)
    finally:
        # A raise finished the function too, and its failure result travels the
        # same wedgeable path back to the monitor.
        close_lease()


def dispatch_spec(func_path: str, args, kwargs):
    """
    Decide what to hand Django-Q2 for a dotted-path target.

    A coroutine function must not be enqueued directly - the worker would
    call it and drop the unawaited coroutine while the monitor records a
    success. Reroute it through ``run_task``, which awaits (see
    ``call_target``). A path that doesn't import from this process is left
    alone; the worker fails loudly on it either way.

    Returns ``(func, args, kwargs)`` ready to splat into ``q2_async_task``.
    """
    try:
        target = _resolve_target(func_path)
    except ImportError:
        return func_path, args, kwargs
    if not inspect.iscoroutinefunction(target):
        return func_path, args, kwargs
    return "qraft.runner.run_task", (func_path, list(args), dict(kwargs)), {}
