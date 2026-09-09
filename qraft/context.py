"""
Task-execution context: current-attempt lookup, usage accounting, and
progress reporting.

`_current_q2_task_id` is set by a `pre_execute` receiver so that task code
running deep in a call stack (e.g. inside an LLM client wrapper) can find
its own QraftTaskAttempt without threading it through every call. The lease
then binds the resolved attempt's ids (`bind_attempt`), which is what the
logging filter and the signal payloads read.

Progress and usage writes are atomic: one statement on Postgres (`jsonb ||`
with a `CASE` for the advanced timestamp), a locked read-modify-write
elsewhere. Two reporters in one attempt - a threaded task, a coroutine
fan-out - therefore never lose an increment.
"""

import json
import logging
from contextvars import ContextVar
from decimal import Decimal

from asgiref.sync import sync_to_async
from django.core.serializers.json import DjangoJSONEncoder
from django.db import connection, transaction
from django.db.models import Exists, OuterRef
from django.utils import timezone

from qraft.conf import get_conf

_logger = logging.getLogger("qraft")

_current_q2_task_id: ContextVar[str | None] = ContextVar(
    "_current_q2_task_id", default=None
)
_bound_context: ContextVar[dict | None] = ContextVar("_bound_context", default=None)
# (q2_task_id, verdict, claimed_at) from qraft.lease.claim_delivery(), so the
# lease and the runner share one compare-and-swap per delivery.
_delivery_claim: ContextVar[tuple | None] = ContextVar("_delivery_claim", default=None)

CONTEXT_KEYS = (
    "task_id",
    "attempt_id",
    "attempt_number",
    "func",
    "run_id",
    "stage",
    "subject_type",
    "subject_id",
)

# Usage keys that hold money. Whatever a caller passes - Decimal, float or a
# numeric string - is stored as its decimal string form and summed as Decimal.
# Money is never summed as a float: two calls recording 0.1 and 0.2 must total
# 0.3, not 0.30000000000000004.
MONEY_KEYS = frozenset({"cost", "estimated_cost"})


def _is_numeric(value) -> bool:
    """Whether a usage value accumulates (bools are flags, not counters)."""
    return isinstance(value, (int, float, Decimal)) and not isinstance(value, bool)


def _money(value) -> Decimal | None:
    """Parse a stored money value; None when it is not a decimal string/number."""
    if isinstance(value, bool) or value is None:
        return None
    try:
        return Decimal(str(value))
    except (ArithmeticError, ValueError):
        return None


def _add(existing, value):
    """Sum two numeric usage values, keeping Decimal exact."""
    if isinstance(existing, Decimal) or isinstance(value, Decimal):
        return Decimal(str(existing)) + Decimal(str(value))
    return existing + value


def _on_pre_execute(sender, func, task, **kwargs):
    """django_q `pre_execute` receiver: record the executing task's q2 id."""
    _current_q2_task_id.set(task.get("id"))
    _bound_context.set(None)
    # Deliberately not clearing `_delivery_claim` here. The lease's own
    # `pre_execute` receiver claims the delivery and memoises the verdict, and
    # clearing it from this receiver would make the two receivers' connect
    # order decide whether `runner.guard_delivery` re-claims an attempt that
    # has already spent its execution budget - and so refuses every task. The
    # memo is keyed by q2 task id and `clear_context()` drops it at the end of
    # an attempt, so a stale entry is never read.


def current_q2_task_id() -> str | None:
    """The Django-Q2 task id of the delivery this thread is executing."""
    return _current_q2_task_id.get()


def delivery_claim() -> tuple | None:
    """The `(q2_task_id, verdict, claimed_at)` this delivery already claimed."""
    return _delivery_claim.get()


def set_delivery_claim(claim: tuple) -> None:
    """Publish a delivery verdict so the lease and the runner share one claim."""
    _delivery_claim.set(claim)


def bind_attempt(attempt, qraft_task=None) -> dict:
    """Publish the executing attempt's ids to the logging filter and signals."""
    task = qraft_task if qraft_task is not None else attempt.qraft_task
    bound = {
        "task_id": str(task.id),
        "attempt_id": str(attempt.id),
        "attempt_number": attempt.attempt_number,
        "func": task.func,
        "run_id": str(run_id) if (run_id := getattr(task, "run_id", None)) else None,
        "stage": getattr(task, "stage", None),
        "subject_type": task.subject_type,
        "subject_id": task.subject_id,
    }
    _bound_context.set(bound)
    return bound


def clear_context() -> None:
    """
    Forget the executing attempt.

    A pool thread outlives the task it ran, so without this its next log
    records - and anything else reading `current_context()` - carry the
    previous attempt's ids.
    """
    _current_q2_task_id.set(None)
    _bound_context.set(None)
    _delivery_claim.set(None)


def current_context() -> dict:
    """
    Ids of the executing attempt, or a dict of Nones outside a task.

    Resolves lazily from the q2 task id when the lease has not bound the
    attempt yet (a `pre_execute` receiver registered before the lease's);
    any lookup failure yields the empty context rather than an exception, as
    this runs inside logging.
    """
    bound = _bound_context.get()
    if bound is not None:
        return bound
    empty = dict.fromkeys(CONTEXT_KEYS)
    if _current_q2_task_id.get() is None:
        return empty
    try:
        attempt = current_attempt()
    except Exception:
        return empty
    if attempt is None:
        return empty
    return bind_attempt(attempt)


def current_attempt_id() -> str | None:
    """
    Id of the executing attempt, for the ownership pattern: put it in the same
    statement as any write that must not survive a superseding attempt.
    """
    return current_context().get("attempt_id")


def current_attempt():
    """
    Return the QraftTaskAttempt for the task currently executing, or None.

    None both outside a task and when the running task has no Qraft record
    (e.g. a plain Django-Q2 task not created via qraft.async_task).
    """
    from qraft.models.tasks import QraftTaskAttempt

    q2_task_id = _current_q2_task_id.get()
    if q2_task_id is None:
        return None

    try:
        return QraftTaskAttempt.objects.select_related("qraft_task").get(
            q2_task_id=q2_task_id
        )
    except QraftTaskAttempt.DoesNotExist:
        return None


def _table(model) -> str:
    return connection.ops.quote_name(model._meta.db_table)


def _json(value) -> str:
    """
    Encode with the encoder the JSON columns themselves use.

    The Postgres paths below build their payloads with raw SQL, so a plain
    `json.dumps` would refuse the UUIDs, dates and Decimals the ORM path
    accepts - the same call would work on SQLite and raise on Postgres.
    """
    return json.dumps(value, cls=DjangoJSONEncoder)


def _jsonb(value) -> str | None:
    return None if value is None else _json(value)


# --- usage -----------------------------------------------------------------


def _merge_usage(existing: dict, fields: dict, entry: dict | None) -> dict:
    """Python-side merge: numeric keys accumulate, others take the latest value."""
    usage = dict(existing)
    for key, value in fields.items():
        current = usage.get(key)
        if key in MONEY_KEYS and (increment := _money(value)) is not None:
            total = _money(current)
            usage[key] = str(total + increment if total is not None else increment)
        elif _is_numeric(value) and _is_numeric(current):
            usage[key] = _add(current, value)
        else:
            usage[key] = value
    if entry is not None:
        usage["entries"] = [*(usage.get("entries") or []), entry]
    return usage


def _pg_usage_update(attempt_id, fields: dict, entry: dict | None) -> None:
    """
    Merge `fields` into `usage` in one UPDATE.

    Numeric keys are summed with the stored value when that is a jsonb
    number; a stored non-number is overwritten, matching the Python merge.
    Money keys are summed as numeric text so jsonb keeps them as strings.
    """
    from qraft.models.tasks import QraftTaskAttempt

    pieces = ["COALESCE(usage, '{}'::jsonb)"]
    params: list = []
    plain = {}
    for key, value in fields.items():
        if key in MONEY_KEYS and _money(value) is not None:
            pieces.append(
                "jsonb_build_object(%s, to_jsonb((CASE WHEN (usage->>%s) ~ "
                "'^-?[0-9]+(\\.[0-9]+)?$' THEN (usage->>%s)::numeric ELSE 0 END "
                "+ %s::numeric)::text))"
            )
            params += [key, key, key, str(_money(value))]
        elif _is_numeric(value):
            pieces.append(
                "jsonb_build_object(%s, CASE WHEN jsonb_typeof(usage->%s) = 'number' "
                "THEN (usage->>%s)::numeric ELSE 0 END + %s::numeric)"
            )
            params += [key, key, key, str(value)]
        else:
            plain[key] = value
    if plain:
        pieces.append("%s::jsonb")
        params.append(_json(plain))
    if entry is not None:
        pieces.append(
            "jsonb_build_object('entries', COALESCE(usage->'entries', '[]'::jsonb) "
            "|| %s::jsonb)"
        )
        params.append(_json([entry]))

    sql = (
        f"UPDATE {_table(QraftTaskAttempt)} SET usage = {' || '.join(pieces)} "
        "WHERE id = %s"
    )
    params.append(attempt_id)
    with connection.cursor() as cursor:
        cursor.execute(sql, params)


def _locked_usage_update(attempt_id, fields: dict, entry: dict | None) -> None:
    from qraft.models.tasks import QraftTaskAttempt

    with transaction.atomic():
        row = QraftTaskAttempt.objects.select_for_update().get(id=attempt_id)
        row.usage = _merge_usage(row.usage or {}, fields, entry)
        row.save(update_fields=["usage"])


def _write_usage(attempt, fields: dict, entry: dict | None = None) -> None:
    if connection.vendor == "postgresql":
        _pg_usage_update(attempt.id, fields, entry)
    else:
        _locked_usage_update(attempt.id, fields, entry)


def record_usage(**fields) -> None:
    """
    Merge usage fields into the current attempt's `usage` JSON.

    Numeric fields (int/float/Decimal, excluding bool) accumulate across
    repeated calls within the same attempt (e.g. several LLM calls in one
    task); non-numeric fields (model name, etc.) are overwritten by the latest
    call. Money keys (`cost`) are kept as decimal strings. No-op outside a
    task.

    A call that names a `model` also appends one entry to `usage["entries"]`,
    priced by the configured resolver (see `qraft.pricing`) or by the caller's
    own `cost`. Entries are what make an attempt that calls two models
    priceable: the merge above keeps the latest model name while summing all
    tokens, so pricing the totals would bill one model's tokens at another's
    rate.
    """
    from qraft.pricing import usage_entry

    attempt = current_attempt()
    if attempt is None:
        _logger.debug("record_usage() called outside a task, ignoring")
        return

    _write_usage(attempt, fields, usage_entry(fields))


# --- budgets ---------------------------------------------------------------


class BudgetExhausted(Exception):
    """Raised by `consume_budget()` when a run's allowance for a key is spent."""


def _pg_consume_budget(run_id, key: str, n: int) -> int | None:
    """
    Decrement one budget key in a single guarded UPDATE.

    The `>= n` in the WHERE clause is the guard: a decrement that would take
    the key below zero matches no row, so two workers cannot both spend the
    last request. RETURNING hands back what is left.
    """
    from qraft.models.runs import QraftRun

    sql = (
        f"UPDATE {_table(QraftRun)} SET budgets = "
        "jsonb_set(budgets, ARRAY[%s], to_jsonb(((budgets->>%s)::numeric - "
        "%s::numeric))) WHERE id = %s AND jsonb_typeof(budgets->%s) = 'number' "
        "AND (budgets->>%s)::numeric >= %s::numeric "
        "RETURNING (budgets->>%s)::numeric"
    )
    with connection.cursor() as cursor:
        cursor.execute(sql, [key, key, n, run_id, key, key, n, key])
        row = cursor.fetchone()
    return int(row[0]) if row else None


def _locked_consume_budget(run_id, key: str, n: int) -> int | None:
    from qraft.models.runs import QraftRun

    with transaction.atomic():
        run = QraftRun.objects.select_for_update().only("id", "budgets").get(id=run_id)
        budgets = dict(run.budgets or {})
        remaining = budgets.get(key)
        if not _is_numeric(remaining) or remaining < n:
            return None
        budgets[key] = remaining - n
        run.budgets = budgets
        run.save(update_fields=["budgets"])
        return int(budgets[key])


def consume_budget(key: str, n: int = 1, run_id=None) -> int | None:
    """
    Spend `n` from the executing run's budget for `key`, atomically.

    A budget bounds how many provider requests one pipeline may make, across
    every stage and every retry of every stage - the question a token-bucket
    throttle cannot answer, because a bucket refills. Declare it at
    `runs.start(..., budgets={"openai_requests": 40})` and spend it one call
    at a time::

        qraft.context.consume_budget("openai_requests")   # before the call
        response = client.responses.create(...)
        qraft.context.record_usage(model=..., input_tokens=...)  # after it

    The decrement is one guarded statement (`RETURNING` on Postgres, a locked
    read-modify-write elsewhere), so a decrement that would go below zero
    matches nothing and two workers cannot both spend the last request.

    Args:
        key: Budget name, as declared on the run.
        n: How much to spend. Must be positive.
        run_id: The run to charge; defaults to the executing attempt's.

    Returns:
        int: what is left after the decrement, or None when nothing is
        metered - outside a run, or for a key the run does not declare.
        A key with no budget is unmetered by design: Qraft does not invent a
        limit the application never asked for.

    Raises:
        BudgetExhausted: the key is declared and has less than `n` left.
    """
    if n <= 0:
        raise ValueError(f"consume_budget(n={n!r}) must spend a positive amount")

    run_id = run_id if run_id is not None else current_context().get("run_id")
    if not run_id:
        _logger.debug("consume_budget() called outside a run, ignoring")
        return None

    if not _budget_declares(run_id, key):
        return None

    if connection.vendor == "postgresql":
        remaining = _pg_consume_budget(run_id, key, n)
    else:
        remaining = _locked_consume_budget(run_id, key, n)

    if remaining is None:
        raise BudgetExhausted(
            f"run {run_id} has no {key!r} budget left for {n} more; "
            "the stage must stop rather than spend past its allowance"
        )
    return remaining


def _budget_declares(run_id, key: str) -> bool:
    """Whether the run declares this key at all. An undeclared key is unmetered."""
    from qraft.models.runs import QraftRun

    budgets = (
        QraftRun.objects.filter(id=run_id).values_list("budgets", flat=True).first()
    )
    return bool(budgets) and key in budgets


def remaining_budget(key: str, run_id=None) -> int | None:
    """What is left of a budget key, without spending any. None when unmetered."""
    from qraft.models.runs import QraftRun

    run_id = run_id if run_id is not None else current_context().get("run_id")
    if not run_id:
        return None
    budgets = (
        QraftRun.objects.filter(id=run_id).values_list("budgets", flat=True).first()
    )
    value = (budgets or {}).get(key)
    return int(value) if _is_numeric(value) else None


# --- progress --------------------------------------------------------------


def _progress_payload(current, total, message, extra) -> dict:
    return {
        key: value
        for key, value in {
            "current": current,
            "total": total,
            "message": message,
            **extra,
        }.items()
        if value is not None
    }


def _pg_progress_update(attempt_id, payload, current, total, now, cutoff, force):
    """
    Merge `payload` into the attempt's progress in one statement, returning the
    merged payload, or None when the write was coalesced away.

    `advanced` compares the incoming current/total against the stored jsonb
    values (numeric equality, so 1 and 1.0 agree); it drives both the
    `advanced_at` CASE and the coalescing escape hatch.
    """
    from qraft.models.tasks import QraftTaskAttempt

    advanced = (
        "((%(current)s::jsonb IS NOT NULL AND (progress->'current') IS DISTINCT FROM "
        "%(current)s::jsonb) OR (%(total)s::jsonb IS NOT NULL AND "
        "(progress->'total') IS DISTINCT FROM %(total)s::jsonb))"
    )
    throttle = (
        "TRUE"
        if force or cutoff is None
        else f"(progress_reported_at IS NULL OR progress_reported_at <= %(cutoff)s "
        f"OR {advanced})"
    )
    sql = (
        f"UPDATE {_table(QraftTaskAttempt)} SET "
        "progress = COALESCE(progress, '{}'::jsonb) || %(payload)s::jsonb, "
        "progress_reported_at = %(now)s, "
        f"progress_advanced_at = CASE WHEN {advanced} THEN %(now)s "
        "ELSE progress_advanced_at END "
        f"WHERE id = %(id)s AND {throttle} RETURNING progress"
    )
    params = {
        "payload": _json(payload),
        "current": _jsonb(current),
        "total": _jsonb(total),
        "now": now,
        "cutoff": cutoff,
        "id": attempt_id,
    }
    with connection.cursor() as cursor:
        cursor.execute(sql, params)
        row = cursor.fetchone()
    if row is None:
        return None
    merged = row[0]
    return json.loads(merged) if isinstance(merged, str) else merged


def _locked_progress_update(attempt_id, payload, current, total, now, cutoff, force):
    from qraft.models.tasks import QraftTaskAttempt

    row = QraftTaskAttempt.objects.select_for_update().get(id=attempt_id)
    stored = row.progress or {}
    advanced = (current is not None and stored.get("current") != current) or (
        total is not None and stored.get("total") != total
    )
    if (
        not force
        and cutoff is not None
        and row.progress_reported_at is not None
        and row.progress_reported_at > cutoff
        and not advanced
    ):
        return None
    merged = {**stored, **payload}
    QraftTaskAttempt.objects.filter(id=attempt_id).update(
        progress=merged,
        progress_reported_at=now,
        progress_advanced_at=now if advanced else row.progress_advanced_at,
    )
    return merged


def _snapshot_progress(attempt, merged: dict) -> bool:
    """
    Copy the attempt's progress onto the task, only from the latest attempt.

    The WHERE is the guard: a late write from a superseded attempt (one that
    stalled while its retry ran) updates zero rows. The task row is locked
    first, and `scheduler.schedule_attempt()` takes the same lock before it
    inserts the next attempt, so the guard cannot read "no newer attempt" from
    a snapshot that insert invalidates a moment later.
    """
    from qraft.models.tasks import QraftTask, QraftTaskAttempt

    QraftTask.objects.select_for_update().filter(id=attempt.qraft_task_id).first()
    newer = QraftTaskAttempt.objects.filter(
        qraft_task_id=OuterRef("pk"), attempt_number__gt=attempt.attempt_number
    )
    return bool(
        QraftTask.objects.filter(id=attempt.qraft_task_id)
        .exclude(Exists(newer))
        .update(progress={**merged, "attempt_id": str(attempt.id)})
    )


def report_progress(
    current: int | None = None,
    total: int | None = None,
    message: str | None = None,
    force: bool = False,
    **extra,
) -> bool:
    """
    Merge a progress payload into the current attempt, and snapshot it onto the
    task when this attempt is the task's latest.

    `progress_reported_at` moves on every write; `progress_advanced_at` only
    when `current` or `total` changed. With `progress_min_interval` set, a call
    inside the interval is skipped unless something advanced or `force` is
    passed. Returns whether a write happened. No-op outside a task.
    """
    attempt = current_attempt()
    if attempt is None:
        _logger.debug("report_progress() called outside a task, ignoring")
        return False

    payload = _progress_payload(current, total, message, extra)
    now = timezone.now()
    interval = get_conf().progress_min_interval
    cutoff = now - timezone.timedelta(seconds=interval) if interval else None

    with transaction.atomic():
        if connection.vendor == "postgresql":
            merged = _pg_progress_update(
                attempt.id, payload, current, total, now, cutoff, force
            )
        else:
            merged = _locked_progress_update(
                attempt.id, payload, current, total, now, cutoff, force
            )
        if merged is None:
            return False
        _snapshot_progress(attempt, merged)
    return True


async def areport_progress(*args, **kwargs) -> bool:
    """Coroutine-task form of `report_progress`; the write runs off the loop."""
    return await sync_to_async(report_progress)(*args, **kwargs)


async def arecord_usage(**fields) -> None:
    """Coroutine-task form of `record_usage`; the write runs off the loop."""
    await sync_to_async(record_usage)(**fields)


# --- aggregation -----------------------------------------------------------


def _sum_usage(usages) -> dict:
    """
    Sum numeric keys across usage dicts; latest non-numeric wins; `entries`
    lists concatenate; money keys stay decimal strings.
    """
    aggregated: dict = {}
    entries: list = []
    for usage in usages:
        if not usage:
            continue
        for key, value in usage.items():
            if key == "entries":
                entries.extend(value or [])
            elif key in MONEY_KEYS and (increment := _money(value)) is not None:
                total = _money(aggregated.get(key)) or Decimal(0)
                aggregated[key] = str(total + increment)
            elif _is_numeric(value):
                aggregated[key] = _add(aggregated.get(key, 0), value)
            else:
                aggregated[key] = value
    if entries:
        aggregated["entries"] = entries
    summary = _cost_summary(aggregated)
    if summary is not None:
        # Under its own key, never `cost`: that one is the caller's own total
        # and overwriting it would corrupt the sum this function just made.
        aggregated["cost_summary"] = summary
    return aggregated


def _cost_summary(aggregated: dict) -> dict | None:
    """The priced view of an aggregate, when there is anything to price."""
    from qraft.pricing import cost

    if not (aggregated.get("entries") or aggregated.get("cost")):
        return None
    return cost(aggregated).as_dict()


def aggregate_usage(qraft_task) -> dict:
    """Sum usage across all of a QraftTask's attempts."""
    return _sum_usage(qraft_task.attempts.values_list("usage", flat=True))


def aggregate_workflow_usage(workflow) -> dict:
    """
    Sum usage across a workflow's tasks.

    `workflow` is a QraftIterModel/QraftBatchModel (usage summed across
    `workflow.tasks`) or a QraftChainModel (usage summed across the chain's
    steps' linked qraft_tasks).
    """
    if hasattr(workflow, "tasks"):
        qraft_tasks = workflow.tasks.all()
    else:
        qraft_tasks = [
            step.qraft_task
            for step in workflow.steps.all()
            if step.qraft_task_id is not None
        ]

    usages = [aggregate_usage(qraft_task) for qraft_task in qraft_tasks]
    return _sum_usage(usages)


def aggregate_run_usage(run_id) -> dict:
    """Sum usage across every attempt of every task in a run."""
    from qraft.models.tasks import QraftTaskAttempt

    return _sum_usage(
        QraftTaskAttempt.objects.filter(qraft_task__run_id=run_id).values_list(
            "usage", flat=True
        )
    )


def aggregate_subject_usage(subject_type: str, subject_id) -> dict:
    """Sum usage across every attempt of every task for one subject."""
    from qraft.models.tasks import QraftTaskAttempt

    return _sum_usage(
        QraftTaskAttempt.objects.filter(
            qraft_task__subject_type=subject_type,
            qraft_task__subject_id=str(subject_id),
        ).values_list("usage", flat=True)
    )
