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

from django.utils.module_loading import import_string


def _resolve_target(func_path: str):
    """Resolve a dotted path, unwrapping the django.tasks @task wrapper if present."""
    func = import_string(func_path)
    try:
        from django.tasks import Task
    except ImportError:
        return func
    return func.func if isinstance(func, Task) else func


def run_task(func_path: str, args, kwargs):
    """Worker-side entry point: resolve func_path and call it with args/kwargs."""
    return _resolve_target(func_path)(*args, **kwargs)
