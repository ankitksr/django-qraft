"""
Runs: one row spanning a pipeline whose stages enqueue each other.

The application declares the stages a run expects, binds exactly one
completion unit (a task or a workflow) under each, and the run settles by
derivation: a failed or cancelled stage fails the run, and a run whose every
stage has SUCCEEDED or been SKIPPED succeeds. Both transitions are the
`settled_at` compare-and-swap, so a run settles exactly once and is never
mutated afterwards; a rerun is a new run linked by `previous_run`.

Every edge rule is enforced at the bind, where a mistake is cheap and the
operator has not yet built a run on a wrong assumption. The durable
completion event is the `on_settled` hook, keyed `(run_id, "settled")`;
`run_settled` the signal fires beside it for observers.
"""

import logging
from contextlib import contextmanager
from datetime import timedelta

from django.conf import settings
from django.db import transaction
from django.utils import timezone

from qraft import metrics, signals
from qraft.models import QraftTask, TaskStatus, WorkflowStatus
from qraft.models.runs import (
    QraftRun,
    QraftRunStage,
    RunStatus,
    StageStatus,
    UnitType,
)

_logger = logging.getLogger("qraft.runs")

# The unit outcome each terminal status maps onto its stage row.
_TASK_OUTCOMES = {
    TaskStatus.SUCCEEDED: StageStatus.SUCCEEDED,
    TaskStatus.FAILED: StageStatus.FAILED,
    TaskStatus.EXHAUSTED: StageStatus.FAILED,
}
_WORKFLOW_OUTCOMES = {
    WorkflowStatus.SUCCEEDED: StageStatus.SUCCEEDED,
    WorkflowStatus.FAILED: StageStatus.FAILED,
    WorkflowStatus.CANCELLED: StageStatus.CANCELLED,
}

# Stage outcomes that fail the whole run the moment they are recorded. Waiting
# for every bound unit before deciding only delays the same answer while
# hiding it from the dashboard.
_FAILING_STAGE_STATUSES = (StageStatus.FAILED, StageStatus.CANCELLED)


class RunError(ValueError):
    """Raised when a run or stage transition is refused."""


def _workflow_unit_type(workflow) -> str:
    from qraft.models import QraftBatchModel, QraftChainModel

    if isinstance(workflow, QraftChainModel):
        return UnitType.CHAIN
    if isinstance(workflow, QraftBatchModel):
        return UnitType.BATCH
    return UnitType.ITER


def unit_type_of(unit) -> str:
    """Which `UnitType` a task or workflow row binds as."""
    if isinstance(unit, QraftTask):
        return UnitType.TASK
    return _workflow_unit_type(unit)


def start(
    subject,
    stages,
    kind: str | None = None,
    revision: str | None = None,
    metadata: dict | None = None,
    started_at=None,
    on_settled: str | None = None,
    on_settled_kwargs: dict | None = None,
    previous_run=None,
    budgets: dict | None = None,
) -> str:
    """
    Open a run over `subject`, declaring the stages it expects.

    Args:
        subject: `(subject_type, subject_id)`; the id is stored as a string.
            May be None and named later with `bind_subject()`, for a pipeline
            whose first stage is what creates the subject.
        stages: Stage names, in declaration order. Order is display order -
            Qraft does not enforce that one stage waits for another.
        kind: Execution fingerprint (`shadow`, `parity`, ...); a metric label,
            so keep its vocabulary bounded.
        revision: Pipeline or input revision. Never a metric label.
        metadata: Application data. Never a metric label.
        started_at: When the work became due; defaults to now. Backdate it and
            report-to-ready includes the time the first task spent queued.
        on_settled: Dotted path to the durable completion hook.
        on_settled_kwargs: Extra keyword arguments for that hook.
        previous_run: The run this one reruns, if any.
        budgets: Key -> allowance, spent by `qraft.context.consume_budget()`.
            A ceiling on what one pipeline may ask of a provider across every
            stage and every retry; a key not declared here is unmetered.

    Returns:
        str: the run id, ready to pass as `qraft_options={"run": run_id}`.
    """
    from qraft.tasks import parse_subject

    subject_type, subject_id = parse_subject(subject)
    if not stages:
        raise RunError("a run must declare at least one stage")
    names = list(stages)
    if len(set(names)) != len(names):
        raise RunError(f"stage names must be unique within a run, got {names}")
    budgets = _validated_budgets(budgets)

    with transaction.atomic():
        run = QraftRun.objects.create(
            subject_type=subject_type,
            subject_id=subject_id,
            kind=kind,
            revision=revision,
            metadata=metadata,
            date_started=started_at or timezone.now(),
            on_settled=on_settled,
            on_settled_kwargs=on_settled_kwargs or {},
            previous_run_id=getattr(previous_run, "id", previous_run),
            budgets=budgets,
        )
        QraftRunStage.objects.bulk_create(
            [
                QraftRunStage(run=run, name=name, position=position)
                for position, name in enumerate(names)
            ]
        )

    _logger.debug(
        "Started QraftRun %s for %s:%s with stages %s",
        run.id,
        subject_type,
        subject_id,
        names,
    )
    return str(run.id)


def _validated_budgets(budgets: dict | None) -> dict | None:
    """
    Budgets are whole counts of requests, checked at `start()`.

    A float would be truncated by the decrement's `RETURNING` cast and read
    back as a different number than the one stored, so it is refused here
    rather than silently rounded.
    """
    if not budgets:
        return None
    checked = {}
    for key, value in budgets.items():
        if not isinstance(value, int) or isinstance(value, bool) or value < 0:
            raise RunError(
                f"budget {key!r} must be a non-negative whole number of "
                f"requests, got {value!r}"
            )
        checked[str(key)] = value
    return checked


def get(run_id) -> QraftRun:
    """The run row, or a RunError naming the id that does not exist."""
    try:
        return QraftRun.objects.get(id=run_id)
    except (QraftRun.DoesNotExist, ValueError, TypeError) as exc:
        raise RunError(f"unknown run {run_id!r}") from exc


def member_labels(
    run_id, stage: str | None, subject_type: str | None, subject_id: str | None
) -> dict:
    """
    The correlation fields a new task or workflow carries, validated.

    Raises when the run is terminal (rule 1), when it did not declare the
    stage (rule 3), or when the caller named a different subject - so the
    mistake surfaces at enqueue rather than at settlement. A run implies its
    subject: a member that names none inherits it.
    """
    if run_id is None:
        if stage:
            raise RunError(f"stage {stage!r} was given without a run")
        return {
            "run_id": None,
            "stage": None,
            "subject_type": subject_type,
            "subject_id": subject_id,
        }

    run = get(run_id)
    if run.status != RunStatus.OPEN:
        raise RunError(
            f"run {run.id} is {run.status}; a settled run is never reopened. "
            "Start a new run with previous_run pointing at this one."
        )
    if stage and not run.stages.filter(name=stage).exists():
        raise RunError(f"run {run.id} declared no stage named {stage!r}")
    if subject_type is None and subject_id is None:
        subject_type, subject_id = run.subject_type, run.subject_id
    elif (subject_type, subject_id) != (run.subject_type, run.subject_id):
        raise RunError(
            f"subject ({subject_type}, {subject_id}) differs from run {run.id}'s "
            f"({run.subject_type}, {run.subject_id}); a run implies its subject"
        )
    return {
        "run_id": run.id,
        "stage": stage,
        "subject_type": subject_type,
        "subject_id": subject_id,
    }


def bind_subject(run_id, subject) -> None:
    """
    Name the subject of a run that started without one.

    The first stage of a pipeline is often the one that creates the domain
    object the run is about: an ingest task that makes the worksheet the rest
    of the run scores. Forcing the subject at `start()` makes such a run key
    on whatever row existed beforehand, and the dashboard filter then splits
    one domain object across two subject types.

    Allowed once, while the run is OPEN and its subject is unset. Refused on
    a second call and on a settled run - the same "declare it once" rule the
    stage bind follows, and for the same reason: everything already labelled
    with this run has been labelled with this subject.

    The run and every member already bound to it - tasks, chains, iters and
    batches - are updated in one transaction under the run's row lock, which
    is the lock `runs.correlating()` takes before inserting a member. A
    member being created concurrently therefore either lands before the bind
    (and is updated by it) or reads the bound subject.

    Args:
        run_id: The run to bind.
        subject: `(subject_type, subject_id)`; the id is stored as a string.
    """
    from qraft.models import QraftBatchModel, QraftChainModel, QraftIterModel
    from qraft.tasks import parse_subject

    subject_type, subject_id = parse_subject(subject)
    if subject_type is None:
        raise RunError("bind_subject needs a (subject_type, subject_id) pair")

    now = timezone.now()
    with transaction.atomic():
        run = _locked(run_id)
        if run.status != RunStatus.OPEN:
            raise RunError(
                f"run {run.id} is {run.status}; a settled run's subject is final"
            )
        bound = QraftRun.objects.filter(
            pk=run.pk,
            status=RunStatus.OPEN,
            subject_type__isnull=True,
            subject_id__isnull=True,
        ).update(subject_type=subject_type, subject_id=subject_id, date_updated=now)
        if not bound:
            raise RunError(
                f"run {run.id} already names subject "
                f"({run.subject_type}, {run.subject_id}); a run's subject is "
                "declared once"
            )
        for model in (QraftTask, QraftChainModel, QraftIterModel, QraftBatchModel):
            model.objects.filter(run_id=run.id, subject_type__isnull=True).update(
                subject_type=subject_type, subject_id=subject_id
            )

    _logger.debug("Bound subject %s:%s to run %s", subject_type, subject_id, run_id)


@contextmanager
def correlating(run_id, stage, subject_type, subject_id):
    """
    Yield a member's correlation fields while the run row is locked.

    The lock is what makes `bind_subject()` safe against a member being
    created at the same moment: the insert either precedes the bind, which
    then updates it, or waits for it and reads the bound subject. A member
    with no run takes no lock and no transaction.
    """
    if run_id is None:
        yield member_labels(None, stage, subject_type, subject_id)
        return
    with transaction.atomic():
        _locked(run_id)
        yield member_labels(run_id, stage, subject_type, subject_id)


def _locked(run_id) -> QraftRun:
    """The run row under `select_for_update()`, or a RunError naming the id."""
    try:
        return QraftRun.objects.select_for_update().get(id=run_id)
    except (QraftRun.DoesNotExist, ValueError, TypeError) as exc:
        raise RunError(f"unknown run {run_id!r}") from exc


def bind(run_id, stage: str, unit) -> None:
    """
    Make `unit` the one completion unit of `stage`.

    A compare-and-swap on the stage row under `select_for_update()` on the
    run, so the terminal check and the bind are one decision: a cancel racing
    a bind either loses (the bind commits first and the cancel settles the
    run) or wins (the bind raises), never both.
    """
    now = timezone.now()
    with transaction.atomic():
        run = _locked(run_id)
        if run.status != RunStatus.OPEN:
            raise RunError(f"run {run.id} is {run.status}; nothing more may bind to it")
        if not run.stages.filter(name=stage).exists():
            raise RunError(f"run {run.id} declared no stage named {stage!r}")

        # PENDING, not just "no unit": a SKIPPED stage also has a null
        # unit_id, and a stage the application decided to skip must not be
        # bound afterwards. A stage runs once per run.
        bound = QraftRunStage.objects.filter(
            run_id=run.id, name=stage, status=StageStatus.PENDING
        ).update(
            unit_type=unit_type_of(unit),
            unit_id=unit.id,
            status=StageStatus.BOUND,
            bound_at=now,
        )
    if not bound:
        current = QraftRunStage.objects.get(run_id=run_id, name=stage)
        raise RunError(
            f"stage {stage!r} of run {run_id} is already {current.status}"
            f" (unit {current.unit_id}); a stage runs once per run"
        )
    _logger.debug(
        "Bound %s %s to stage %s of run %s",
        unit_type_of(unit),
        unit.id,
        stage,
        run_id,
    )


def skip(run_id, stage: str, reason: str | None = None) -> None:
    """
    Mark an unbound stage SKIPPED. A skip is a decision the application made,
    so a run whose remaining stages all succeed still settles SUCCEEDED.
    """
    now = timezone.now()
    with transaction.atomic():
        run = QraftRun.objects.select_for_update().get(id=run_id)
        if run.status != RunStatus.OPEN:
            raise RunError(f"run {run.id} is {run.status}; its stages are final")
        if not run.stages.filter(name=stage).exists():
            raise RunError(f"run {run.id} declared no stage named {stage!r}")
        skipped = QraftRunStage.objects.filter(
            run_id=run.id, name=stage, status=StageStatus.PENDING
        ).update(
            status=StageStatus.SKIPPED,
            skip_reason=reason,
            settled_at=now,
        )
        if not skipped:
            current = QraftRunStage.objects.get(run_id=run_id, name=stage)
            raise RunError(
                f"stage {stage!r} of run {run_id} is {current.status}; "
                "only a stage with no unit may be skipped"
            )
        settled = _derive(run, StageStatus.SKIPPED, now)
    _dispatch_settled_hook(settled)


def cancel(run_id) -> bool:
    """
    Settle the run CANCELLED. Units already in flight finish and their
    outcomes are recorded on their stage rows, which no longer move the run.
    """
    return _explicit_settlement(run_id, RunStatus.CANCELLED)


def abandon(run_id, reason: str | None = None) -> bool:
    """
    Settle the run ABANDONED: an OPEN run an operator has decided will never
    complete, typically because a stage was never enqueued.
    """
    return _explicit_settlement(run_id, RunStatus.ABANDONED, reason)


def _explicit_settlement(run_id, status: str, reason: str | None = None) -> bool:
    run = get(run_id)
    with transaction.atomic():
        settled = _settle(run, status, timezone.now(), reason=reason)
    if settled is None:
        run.refresh_from_db()
        raise RunError(f"run {run_id} is already {run.status}")
    _dispatch_settled_hook(settled)
    return True


def note_unit_settled(unit) -> None:
    """
    Record a settled unit's outcome on the stage it owns, and settle the run
    if that outcome decides it.

    Called from inside the window the attempt's `routed` flag protects, so a
    crash between the unit's settlement and this call is replayed by
    `reaper.replay_unrouted()`. A no-op for anything that is not a stage's
    bound unit: a workflow member carries the same `run` and `stage` for
    correlation, but the `unit_type`/`unit_id` match below is what keeps its
    outcome off the stage row.

    A terminal run still records the outcome. A cancel does not revoke work
    already in flight, and what that work did belongs on its stage row and in
    the summary; only the run's own status is closed to it. Replaying this
    against an already-settled run also re-offers the durable `on_settled`
    hook, which is what closes the crash window between the settlement commit
    and the hook's dispatch - `WorkflowHookDispatch` makes the second offer a
    no-op when the first got through.
    """
    run_id = getattr(unit, "run_id", None)
    stage = getattr(unit, "stage", None)
    outcome = _outcome_of(unit)
    if not run_id or not stage or outcome is None:
        return

    now = timezone.now()
    with transaction.atomic():
        try:
            run = QraftRun.objects.select_for_update().get(id=run_id)
        except QraftRun.DoesNotExist:
            return
        recorded = QraftRunStage.objects.filter(
            run_id=run.id,
            name=stage,
            unit_type=unit_type_of(unit),
            unit_id=unit.id,
            settled_at__isnull=True,
        ).update(status=outcome, settled_at=now)
        settled = (
            _derive(run, outcome, now)
            if recorded and run.status == RunStatus.OPEN
            else None
        )
    _dispatch_settled_hook(settled or (run if run.settled_at else None))


def _outcome_of(unit) -> str | None:
    """The stage status a settled unit produces, or None while it is live."""
    if isinstance(unit, QraftTask):
        return _TASK_OUTCOMES.get(unit.status)
    return _WORKFLOW_OUTCOMES.get(unit.status)


def _derive(run: QraftRun, outcome: str, now):
    """
    Apply the two settlement rules to a run whose stage just moved.

    Must run inside the transaction that recorded the stage, under the run's
    row lock: the stage read below has to see that write and no other.
    """
    if outcome in _FAILING_STAGE_STATUSES:
        return _settle(run, RunStatus.FAILED, now)
    statuses = set(run.stages.values_list("status", flat=True))
    if statuses <= {StageStatus.SUCCEEDED, StageStatus.SKIPPED}:
        return _settle(run, RunStatus.SUCCEEDED, now)
    return None


def _settle(run: QraftRun, status: str, now, reason: str | None = None):
    """
    The settlement compare-and-swap, plus the signal and the metrics it fires.

    Returns the settled run, or None when this call did not settle it (already
    settled, or a replay). The signal rides `on_commit` from inside the
    caller's transaction; the durable hook is dispatched by the caller once
    the run's row lock is released, since queueing it is broker work.
    """
    summary = build_summary(run, status, now, reason)
    settled = QraftRun.objects.filter(
        pk=run.pk, settled_at__isnull=True, status=RunStatus.OPEN
    ).update(status=status, settled_at=now, summary=summary, date_updated=now)
    if not settled:
        return None
    run.refresh_from_db()

    signals.send(signals.run_settled, QraftRun, signals.run_payload(run))
    run_labels = {
        "subject_type": run.subject_type,
        "kind": run.kind,
        "outcome": run.status,
    }
    metrics.counter_on_commit("qraft.run.settled", **run_labels)
    duration = metrics.seconds_between(run.date_started, run.settled_at)
    if duration is not None:
        metrics.histogram_on_commit("qraft.run.duration", duration, **run_labels)
        if run.status == RunStatus.SUCCEEDED:
            metrics.histogram_on_commit(
                "qraft.run.report_to_ready",
                duration,
                subject_type=run.subject_type,
                kind=run.kind,
            )
    return run


def _dispatch_settled_hook(run) -> None:
    """
    Queue `on_settled` once, keyed `(run_id, "settled")`.

    A run settles once and is never reopened, so that pair is a stable event
    identity the application can dedupe on - which is what makes this, not
    `run_settled`, the thing a "worksheet ready" transition hangs on.
    """
    if run is None or not run.on_settled:
        return
    from qraft.dispatchers import _dispatch_workflow_hook

    _dispatch_workflow_hook(
        workflow_type="run",
        workflow_id=run.id,
        hook_type="settled",
        hook_path=run.on_settled,
        hook_args=[],
        hook_kwargs=dict(run.on_settled_kwargs or {}),
        context=hook_context(run),
    )


def hook_context(run: QraftRun) -> dict:
    """The `context` keyword argument `on_settled` receives."""
    return {
        "run_id": str(run.id),
        "subject_type": run.subject_type,
        "subject_id": run.subject_id,
        "kind": run.kind,
        "revision": run.revision,
        "metadata": run.metadata,
        "outcome": run.status,
        "stages": {
            stage.name: stage.status for stage in run.stages.all().order_by("position")
        },
        "started_at": run.date_started.isoformat() if run.date_started else None,
        "settled_at": run.settled_at.isoformat() if run.settled_at else None,
        "duration_s": metrics.seconds_between(run.date_started, run.settled_at),
        "previous_run_id": str(run.previous_run_id) if run.previous_run_id else None,
    }


def build_summary(run: QraftRun, status: str, now, reason: str | None = None) -> dict:
    """
    The snapshot written in the settlement transaction.

    Retention may later prune the member tasks and attempts this is computed
    from; the run keeps the answer, which is what makes that pruning safe.
    """
    from qraft.context import aggregate_run_usage

    stages = []
    for stage in run.stages.all().order_by("position"):
        stages.append(
            {
                "name": stage.name,
                "unit_type": stage.unit_type,
                "unit_id": str(stage.unit_id) if stage.unit_id else None,
                "outcome": stage.status,
                "bound_at": stage.bound_at.isoformat() if stage.bound_at else None,
                "settled_at": (
                    stage.settled_at.isoformat() if stage.settled_at else None
                ),
                "duration_s": metrics.seconds_between(stage.bound_at, stage.settled_at),
                "skip_reason": stage.skip_reason,
            }
        )
    summary = {
        "outcome": status,
        "stages": stages,
        "duration_s": metrics.seconds_between(run.date_started, now),
        "usage": aggregate_run_usage(run.id),
    }
    if reason:
        summary["reason"] = reason
    return summary


def replay_settled_hooks(grace: float) -> int:
    """
    Re-offer `on_settled` for runs that settled but never dispatched it.

    The hook is queued after the settlement commits, so a crash in that window
    loses it. A unit settling again replays its own path, but `skip`, `cancel`
    and `abandon` are run-level operations with no attempt behind them and
    nothing to replay them - which would make the durability D17 promises
    conditional on how the run happened to settle. This sweep closes that:
    a settled run older than `grace` whose `WorkflowHookDispatch` row is
    missing gets the hook offered again, and `dispatch_hook_once` keeps it to
    one if the first offer did get through.

    Returns the number of runs re-offered.
    """
    from qraft.models import WorkflowHookDispatch

    cutoff = timezone.now() - timedelta(seconds=grace)
    dispatched = WorkflowHookDispatch.objects.filter(
        workflow_type="run", hook_type="settled"
    ).values("workflow_id")
    pending = (
        QraftRun.objects.filter(settled_at__lt=cutoff)
        .exclude(on_settled=None)
        .exclude(on_settled="")
        .exclude(id__in=dispatched)
    )
    replayed = 0
    for run in pending:
        _logger.warning(
            "Re-offering on_settled for QraftRun %s; it settled at %s but the "
            "hook was never dispatched",
            run.id,
            run.settled_at,
        )
        _dispatch_settled_hook(run)
        replayed += 1
    return replayed


def flag_overdue() -> int:
    """
    Flag OPEN runs older than `QRAFT_RUN_OVERDUE_AFTER` seconds.

    Observation only. An unenqueued stage is an application defect, and the
    operator's tools are `skip`, `cancel` and `abandon`; a run Qraft failed on
    a timer would hide which of the three was right.
    """
    after = getattr(settings, "QRAFT_RUN_OVERDUE_AFTER", None)
    if not after:
        return 0

    now = timezone.now()
    cutoff = now - timedelta(seconds=after)
    overdue = QraftRun.objects.filter(
        status=RunStatus.OPEN,
        overdue_flagged_at__isnull=True,
        date_started__lt=cutoff,
    )
    flagged = 0
    for run in overdue:
        if not QraftRun.objects.filter(
            pk=run.pk, overdue_flagged_at__isnull=True
        ).update(overdue_flagged_at=now):
            continue
        run.overdue_flagged_at = now
        _logger.warning(
            "QraftRun %s has been open for %.0fs (subject %s:%s)",
            run.id,
            (now - run.date_started).total_seconds(),
            run.subject_type,
            run.subject_id,
        )
        signals.send(signals.run_overdue, QraftRun, signals.run_payload(run))
        flagged += 1
    return flagged
