# Runs

> **The Run API is not frozen.** `docs/future/graphs.md` proposes replacing `QraftRun` and
> `QraftRunStage` with a DAG primitive (`QraftGraph`/`QraftGraphNode`) and renames several
> fields in the process. Everything below describes the API as it ships today. If you build
> against it, expect field and behavior changes in a future release, not necessarily a
> deprecation window.

A run is a pipeline over one subject. The application declares the stages it expects up
front, and binds exactly one completion unit — a task or a workflow — under each stage as
the pipeline decides what to do next. A run never gets an explicit "I'm done" call. It
settles by derivation: Qraft watches the stages, and decides SUCCEEDED or FAILED from
their outcomes.

This is the shape for a pipeline whose stages enqueue each other from application code —
an ingest task that enqueues a rules task on success, which enqueues an AI task, and so
on — rather than a pipeline defined as one static list of steps. If your pipeline is a
fixed list of steps, use a [`QraftChain`](workflows.md) instead.

## Starting a run

```python
from qraft import runs

run_id = runs.start(
    subject=("worksheet", "4117"),
    stages=["ingest", "rules", "ai"],
)
```

The full signature:

```python
runs.start(
    subject, stages, kind=None, revision=None, metadata=None,
    started_at=None, on_settled=None, on_settled_kwargs=None,
    previous_run=None, budgets=None,
)
```

It returns the run id as a string, ready to pass into `qraft_options={"run": run_id}`.

- `subject` — a `(subject_type, subject_id)` pair; the id is coerced to a string. Pass
  `None` when the first stage is what creates the subject, and name it later with
  `bind_subject()`.
- `stages` — stage names, in declaration order. Order is display order only; Qraft does
  not enforce that one stage waits for another. Names must be unique within the run.
- `kind` — an execution fingerprint (`shadow`, `parity`, …). It becomes a metric label, so
  keep its vocabulary bounded.
- `revision` — a pipeline or input revision. Never a metric label.
- `metadata` — application data. Never a metric label.
- `started_at` — when the work became due. Defaults to now. Backdate it to the moment a
  report arrived, and the run's duration includes the time the first task spent queued.
- `on_settled` — a dotted path to the durable completion hook (below).
- `on_settled_kwargs` — extra keyword arguments for that hook.
- `previous_run` — the run this one reruns, if any.
- `budgets` — a request allowance per key, spent with `qraft.context.consume_budget()`
  (below).

`runs.start()` raises `RunError` if `stages` is empty or has duplicate names.

## Joining a run

A task joins a run and binds a stage by naming both in `qraft_options`:

```python
async_task(
    "revenue.tasks.ingest",
    worksheet_id,
    qraft_options={"run": run_id, "stage": "ingest"},
)
```

A workflow (`QraftChain`, `QraftIter`, `QraftBatch`) binds the same way, through
constructor keywords:

```python
from qraft.batch import QraftBatch

batch = QraftBatch(run=run_id, stage="rules")
batch.append("revenue.tasks.revenue_pass", worksheet_id)
batch.append("revenue.tasks.entity_pass", worksheet_id)
batch.run()
```

A plain task binds its stage at enqueue. A workflow binds at `run()` — the moment its
work is committed — and its members are created with the same `run` and `stage` for
correlation, but a member never binds or settles a stage; only the workflow itself does.

Binding is a stage-level compare-and-swap: the stage moves from PENDING to BOUND only if
it currently has no unit. Binding raises `RunError` when:

- the run is already terminal (settled or explicitly closed) — nothing more may bind to it
- the stage was never declared in `runs.start(stages=...)`
- the stage already has a unit — a stage runs once per run

A run implies its subject. A task or workflow that names a run but no subject inherits the
run's subject; naming a different subject than the run's raises.

`runs.member_labels(run_id, stage, subject_type, subject_id)` is what computes and
validates those correlation fields, and `runs.correlating(run_id, stage, subject_type,
subject_id)` is the context manager that does it while holding the run row locked — this
is what makes a member's creation and a concurrent `bind_subject()` call race safely rather
than silently.

## Settlement

A run has one open status and four terminal ones: SUCCEEDED, FAILED, CANCELLED,
ABANDONED. Two rules decide the derived outcomes, applied every time a bound stage's unit
settles:

- If the stage's outcome is FAILED or CANCELLED, the run becomes FAILED immediately. Qraft
  does not wait for the other stages to finish first — waiting only delays the same answer
  while hiding it from the dashboard.
- If every stage is SUCCEEDED or SKIPPED, the run becomes SUCCEEDED.

Both are the same compare-and-swap, on the run's `settled_at` column: it can only move
from unset to set once, so a run settles exactly once. **A settled run is never mutated
afterward.** If you need to rerun the pipeline, start a new run and pass the settled one as
`previous_run`; the two are linked, and the dashboard shows the link. Nothing about a
settled run's stages, subject, or status changes after the fact.

A task's outcome maps onto its stage as SUCCEEDED, FAILED (from either TaskStatus.FAILED
or TaskStatus.EXHAUSTED), and a workflow's outcome maps as SUCCEEDED, FAILED, or CANCELLED.
A cancel does not revoke work already in flight: units already running finish, and their
outcomes are recorded on their stage rows, but only the run's own status is closed to
further settlement once it is terminal.

## Explicit transitions

Three operations move a run outside the derived rules:

- **`runs.skip(run_id, stage, reason=None)`** — marks an unbound stage SKIPPED. A skip is a
  decision the application made, not a failure Qraft observed, so a run whose remaining
  stages all succeed still settles SUCCEEDED. Raises if the stage already has a unit —
  only a stage with no unit may be skipped.
- **`runs.cancel(run_id)`** — settles the run CANCELLED. Units already in flight finish;
  their outcomes land on their stage rows but no longer move the run.
- **`runs.abandon(run_id, reason=None)`** — settles the run ABANDONED. Use this when an
  operator has decided an OPEN run will never complete, typically because a stage was
  never enqueued.

Both `cancel()` and `abandon()` return `True` on success and raise `RunError` if the run is
already settled.

## The `on_settled` hook

`on_settled` is the durable completion event, dispatched exactly once through the same
`WorkflowHookDispatch` idempotency mechanism the workflow hooks use, keyed
`(run_id, "settled")`. Because a run settles once and is never reopened, that pair is a
stable identity your application can dedupe on — this, not the `run_settled` signal, is
what a "worksheet ready" transition should hang on. The signal is best-effort; the hook is
durable.

The hook receives one `context` dict (built by `runs.hook_context()`):

```python
{
    "run_id": "...",
    "subject_type": "worksheet",
    "subject_id": "4117",
    "kind": "shadow",
    "revision": "rules@v7",
    "metadata": {...},
    "outcome": "succeeded",
    "stages": {"ingest": "succeeded", "rules": "succeeded", "ai": "succeeded"},
    "started_at": "2026-09-10T12:00:00Z",
    "settled_at": "2026-09-10T12:04:31Z",
    "duration_s": 271.0,
    "previous_run_id": None,
}
```

### Replaying a lost hook

`on_settled` is queued after the settlement commits, so a crash in that window can lose
it. `runs.replay_settled_hooks(grace)` re-offers `on_settled` for every run that settled
more than `grace` seconds ago but has no `WorkflowHookDispatch` row for it — the same
dedup guarantee means a run whose hook did get through is untouched by the replay.

## Request budgets

A budget bounds how much one pipeline may ask of a provider across every stage and every
retry of every stage — a question a token-bucket throttle cannot answer, because a bucket
refills. Declare budgets at `runs.start(budgets={"openai_requests": 40})` as whole,
non-negative counts; a float raises `RunError` at start time rather than being silently
rounded.

Spend one call at a time from inside a task:

```python
from qraft.context import consume_budget, record_usage

consume_budget("openai_requests")   # before the call
response = client.responses.create(...)
record_usage(model=..., input_tokens=...)  # after it
```

- **`consume_budget(key, n=1, run_id=None)`** — spends `n` from the executing run's budget
  for `key`. `run_id` defaults to the executing attempt's run. The decrement is one guarded
  statement (`RETURNING` on Postgres, a locked read-modify-write elsewhere), so two workers
  can never both spend the last request. Returns what's left after the decrement, or `None`
  when nothing is metered — outside a run, or for a key the run never declared. A key with
  no budget is unmetered by design: Qraft does not invent a limit the application never
  asked for. Raises `BudgetExhausted` when the key is declared and has less than `n` left.
- **`remaining_budget(key, run_id=None)`** — what's left of a budget key, without spending
  any. `None` when unmetered.

## The overdue sweep

`QRAFT_RUN_OVERDUE_AFTER` (seconds; default `None`, meaning off) turns on a sweep that
flags an OPEN run whose `date_started` is older than the threshold. This is
**observation only**: the run's `overdue_flagged_at` is set once, `run_overdue` fires as a
signal, and the run's status is untouched. Nothing fails the run automatically. An
unenqueued stage is an application defect, and the tools for it are `skip`, `cancel`, and
`abandon` — a timer that failed the run on its own would hide which of those three was the
right call.
