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

from . import metrics, signals
from .brokers import broker_for_cluster
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
    Claim and enqueue share one transaction: with the ORM broker the queue row
    lives in the same database, so both commit or neither does (the reasoning
    of qraft.scheduler._claim_and_enqueue). With a broker that writes outside
    the database an enqueue failure removes the claim again and the hook is
    lost - nothing above this layer retries it. The other direction also
    exists on non-ORM brokers: the external enqueue can commit while the
    claim transaction rolls back, so a later redelivery can claim again and
    the hook runs twice.

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
        # The atomic block already rolled the claim back; this only fires for
        # a claim that escaped it (an enqueue that committed independently).
        dispatch_model.objects.filter(**lookup, q2_task_id=placeholder_id).delete()
        _logger.exception(
            "Failed to dispatch hook '%s' for %s", hook_path, dispatch_model.__name__
        )


def hook_context(attempt, qraft_task, outcome: str) -> dict:
    """
    The `context` keyword hooks receive under `hook_context=True`.

    Assembled from rows the handler already holds and frozen into the hook
    task's arguments, so it survives the same crashes the HookDispatch row
    does. `result_ref` is the Django-Q2 task id whose `result` holds the
    return value.
    """
    payload = signals.attempt_payload(attempt, qraft_task, outcome)
    return {
        "task_id": payload["task_id"],
        "attempt_id": payload["attempt_id"],
        "attempt_number": payload["attempt_number"],
        "outcome": outcome,
        "exception_class": payload["exception_class"],
        "run_id": payload["run_id"],
        "stage": payload["stage"],
        "subject_type": payload["subject_type"],
        "subject_id": payload["subject_id"],
        "result_ref": attempt.q2_task_id,
        "traceparent": attempt.trace_context,
        "date_started": payload["date_started"],
        "date_completed": payload["date_completed"],
    }


def announce_resolution(attempt, qraft_task, outcome: str, settled: bool) -> None:
    """
    Send `attempt_finished` (and `task_settled` when no retry follows) and count
    the resolution. Called from inside the resolving transaction, so both the
    signals and the counts ride `on_commit`.
    """
    from .models import QraftTask, QraftTaskAttempt

    payload = signals.attempt_payload(attempt, qraft_task, outcome)
    signals.send(signals.attempt_finished, QraftTaskAttempt, payload)
    if settled:
        signals.send(signals.task_settled, QraftTask, payload)

    labels = {"func": qraft_task.func, "cluster": attempt.cluster, "outcome": outcome}
    metrics.counter_on_commit(
        "qraft.attempt.finished",
        exception_class=attempt.exception_class or "",
        **labels,
    )
    duration = metrics.seconds_between(attempt.date_started, attempt.date_completed)
    if duration is not None:
        metrics.histogram_on_commit("qraft.attempt.duration", duration, **labels)


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
    does is a Django-Q2 `Schedule` row written by a pre-1.3 release
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
        # chain_step (and its chain) ride along on the one lookup the monitor
        # serializes on: workflow membership never changes after creation, so
        # the joined answer stays authoritative and the routing step needs no
        # query of its own for the common, workflow-less task.
        attempt = QraftTaskAttempt.objects.select_related(
            "qraft_task",
            "qraft_task__chain_step",
            "qraft_task__chain_step__chain",
        ).get(q2_task_id=q2_task.id)
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


def _owning_workflow_cancelled(qraft_task) -> bool:
    """
    Fresh read of the owning workflow's status, True when it is CANCELLED.

    Cancel stops future orchestration, and a retry is future orchestration: a
    cancelled workflow must not keep generating new executions through a
    member's whole backoff series. The status is re-read here rather than
    trusted from the instance, since a cancel can commit at any point after
    the member was resolved from the queue.
    """
    from .models import QraftChainStep, WorkflowStatus

    try:
        step = qraft_task.chain_step
    except QraftChainStep.DoesNotExist:
        step = None

    workflow = (
        step.chain
        if step is not None
        else qraft_task.qraft_iter or qraft_task.qraft_batch
    )
    if workflow is None:
        return False
    status = (
        workflow.__class__.objects.filter(pk=workflow.pk)
        .values_list("status", flat=True)
        .first()
    )
    return status == WorkflowStatus.CANCELLED


def qraft_hook_handler(q2_task):
    """
    Global hook handler that dispatches Qraft hooks based on task outcome.

    This is registered as the hook for all Qraft-enhanced tasks. Django-Q2
    calls this with the task object after completion.

    Args:
        q2_task: Django-Q2 Task object passed by the monitor process
    """
    from . import runs
    from .dispatchers import route_workflow_completion
    from .models import QraftTask, QraftTaskAttempt, TaskStatus

    attempt = _resolve_attempt(q2_task)
    if attempt is None:
        return
    qraft_task = attempt.qraft_task

    # Update attempt outcome and task status atomically
    retry_scheduled = False
    with transaction.atomic():
        # Re-fetch with lock to prevent concurrent status updates
        qraft_task = QraftTask.objects.select_for_update().get(id=qraft_task.id)

        exception_class = (
            None if q2_task.success else _extract_exception_class(q2_task.result)
        )
        # Compare-and-set: only an unresolved attempt may be resolved. A late
        # result for an attempt already settled (a slow worker finishing after
        # the reaper resolved its attempt as orphaned) is dropped - flipping
        # the outcome here would double-count the member in parallel workflows.
        resolved = QraftTaskAttempt.objects.filter(
            id=attempt.id, success__isnull=True
        ).update(
            success=q2_task.success,
            date_completed=q2_task.stopped,
            exception_class=exception_class,
        )
        if not resolved:
            _logger.warning(
                "Attempt %d of QraftTask %s already resolved; dropping late "
                "result from q2 task %s",
                attempt.attempt_number,
                qraft_task.id,
                q2_task.id,
            )
            return
        attempt.success = q2_task.success
        attempt.date_completed = q2_task.stopped
        attempt.exception_class = exception_class

        # Update task status (may be updated to EXHAUSTED or PENDING by retry handler)
        qraft_task.status = (
            TaskStatus.SUCCEEDED if q2_task.success else TaskStatus.FAILED
        )
        qraft_task.save(update_fields=["status", "date_updated"])

        # The retry (schedule_attempt - DB-only, no broker I/O) commits with
        # the failed completion: a crash leaves either both or neither, never
        # a task durably FAILED with its retry lost.
        if not q2_task.success:
            if _owning_workflow_cancelled(attempt.qraft_task):
                _logger.info(
                    "QraftTask %s belongs to a cancelled workflow; not retrying",
                    qraft_task.id,
                )
            else:
                retry_scheduled = handle_task_retry(
                    qraft_task, attempt, result_text=q2_task.result
                )

        # A scheduled retry leaves no post-commit work, so the resolution is
        # fully routed the moment it commits.
        if retry_scheduled:
            QraftTaskAttempt.objects.filter(id=attempt.id).update(routed=True)

        announce_resolution(
            attempt,
            qraft_task,
            "succeeded" if q2_task.success else "failed",
            settled=not retry_scheduled,
        )

    # A scheduled retry means the task is not terminal yet
    if retry_scheduled:
        return

    # Workflow tasks get workflow-level hooks only, never task-level ones.
    # Routed with the pre-lock instance: its select_related already answered
    # the membership question, and membership is immutable after creation.
    if not route_workflow_completion(attempt.qraft_task, attempt):
        HookDispatcher(qraft_task, attempt).dispatch(q2_task.success)
    # A no-op unless this task is a run stage's bound unit; a workflow member
    # carries the same run and stage but never settles one.
    runs.note_unit_settled(qraft_task)
    # Routing, run settlement and hook dispatch are all idempotent (counted
    # flag, step-index CAS, stage settled_at CAS, HookDispatch unique rows), so
    # the flag needs setting only after they finish; a crash above leaves it
    # False for the reaper to replay.
    QraftTaskAttempt.objects.filter(id=attempt.id).update(routed=True)


class HookDispatcher:
    """
    Handles dual-phase hook dispatching for task completion.
    """

    def __init__(self, qraft_task, attempt):
        self.qraft_task = qraft_task
        self.attempt = attempt

    def dispatch(self, success):
        """
        Dispatch appropriate hook based on task outcome.

        This method assumes retry logic has already been handled by the caller.
        It only dispatches success or failure hooks.

        Args:
            success: Whether the task succeeded. A bool rather than the Q2
                task object, so the reaper can replay dispatch from the
                attempt row after the Q2 row is gone.
        """
        if success:
            self.dispatch_success_hook()
        else:
            self.dispatch_failure_hook()

    def dispatch_success_hook(self):
        """Dispatch success hook if configured."""
        if not self.qraft_task.success_hook:
            return

        self._call_hook(
            hook_path=self.qraft_task.success_hook,
            args=self.qraft_task.success_args or [],
            kwargs=self._hook_kwargs(self.qraft_task.success_kwargs, "succeeded"),
            hook_type="success",
        )

    def dispatch_failure_hook(self):
        """Dispatch failure hook if configured."""
        if not self.qraft_task.failure_hook:
            return

        self._call_hook(
            hook_path=self.qraft_task.failure_hook,
            args=self.qraft_task.failure_args or [],
            kwargs=self._hook_kwargs(self.qraft_task.failure_kwargs, "failed"),
            hook_type="failure",
        )

    def _hook_kwargs(self, configured: dict | None, outcome: str) -> dict:
        """The caller's kwargs, plus `context` when the task opted in."""
        kwargs = dict(configured or {})
        if self.qraft_task.hook_context:
            kwargs["context"] = hook_context(self.attempt, self.qraft_task, outcome)
        return kwargs

    def _call_hook(self, hook_path, args, kwargs, hook_type):
        """
        Dispatch a hook function as an async task or call it synchronously.

        By default, hooks are queued as async tasks to run over workers,
        preventing the monitor from becoming a bottleneck. Set sync_hooks=True
        in settings for legacy synchronous behavior.

        Args:
            hook_path: Dotted import path to hook function
            args: Positional arguments for hook
            kwargs: Keyword arguments for hook
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
        from .runner import dispatch_spec

        func, args, kwargs = dispatch_spec(hook_path, args, kwargs)
        dispatch_hook_once(
            HookDispatch,
            {"qraft_task": self.qraft_task, "hook_type": hook_type},
            hook_path,
            lambda: q2_async_task(
                func,
                *args,
                **kwargs,
                task_name=f"hook:{hook_type}:{self.qraft_task.id}",
                hook=None,  # No hook on hook tasks - prevents recursion
                cluster=executing_cluster(),
                # Named explicitly: `cluster=` alone would resolve the broker
                # against this process's Conf, not the target cluster's.
                broker=broker_for_cluster(executing_cluster()),
                # A hook that keeps failing must not be redelivered forever;
                # nothing above this layer retries it.
                ack_failure=True,
            ),
        )

    def _call_hook_sync(self, hook_path, args, kwargs, hook_type):
        """
        Call hook synchronously (legacy behavior).

        Used when sync_hooks=True in settings. Runs inline in the monitor
        with no HookDispatch record, so the idempotency/duplicate-delivery
        protection _call_hook_async gets from that record does not apply here.
        """
        from .runner import call_target

        try:
            hook_func = import_string(hook_path)
            call_target(hook_func, *args, **kwargs)

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
