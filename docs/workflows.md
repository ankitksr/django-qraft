# Workflow Primitives

Django-Qraft provides three workflow primitives for orchestrating complex task patterns:

- **QraftChain** — Sequential execution (pipeline)
- **QraftIter** — Parallel same-function execution (map)
- **QraftBatch** — Parallel different-function execution (fork-join)

All workflows support:
- Workflow-level success/failure hooks
- Cancellation
- Progress tracking (parallel workflows)
- Rich result objects

## QraftChain

Executes tasks one after another. Each step can have its own retry policy. If any step fails and exhausts retries, the chain fails. Failed chains can be resumed from the failed step.

```python
from qraft.chain import QraftChain

chain = QraftChain(
    on_success='myapp.hooks.pipeline_complete',
    on_failure='myapp.hooks.pipeline_failed',
)
chain.append('myapp.tasks.extract', source_id,
             qraft_options={'max_attempts': 3, 'cluster': 'io-workers'})
chain.append('myapp.tasks.transform', format='json')
chain.append('myapp.tasks.load', dest_id)
chain_id = chain.run()
```

### Resume from failure

```python
# Load existing chain and resume from failed step
chain = QraftChain(chain_id=chain_id)
if chain.status == 'failed':
    chain.resume()
```

### Approval steps

Mark a step `requires_approval=True` and the chain parks in `WAITING_APPROVAL` immediately before that step runs. Nothing is queued and no worker is held while it waits.

```python
chain = QraftChain(on_cancelled='myapp.hooks.publish_rejected')
chain.append('myapp.tasks.draft_release_notes', release_id)
chain.append('myapp.tasks.publish', release_id, requires_approval=True)
chain.run()
```

Resume or abandon it from anywhere — a view, the admin, a shell — by loading the chain by id:

```python
chain = QraftChain(chain_id=chain_id)
if chain.status == 'waiting_approval':
    chain.approve()              # queues the parked step, status → running
    # or
    chain.reject('not ready')    # status → cancelled, fires on_cancelled
```

`approve()` and `reject()` take a row lock and validate the transition, so two reviewers racing produce one decision and an `InvalidStatusTransition` for the loser.

`result(wait=...)` returns as soon as the chain parks instead of blocking to the timeout: `WAITING_APPROVAL` cannot clear without an external decision. The returned `WorkflowResult` holds the steps completed so far.

```python
partial = chain.result(wait=30000)   # returns at once if parked
print(len(partial))                  # steps finished before the gate
```

### Getting results

```python
result = chain.result(wait=30000)  # wait up to 30 seconds
for value in result:
    print(value)

# Or inspect detailed results
print(result.succeeded)
print(result.failed_results)
print(result.errors())
```

## QraftIter

Applies the same function to different inputs in parallel. All tasks share the same retry policy.

```python
from qraft.iter import QraftIter

iter_task = QraftIter(
    'myapp.tasks.process_report',
    qraft_options={'max_attempts': 3},
    cluster='io-workers',
    on_success='myapp.hooks.all_reports_ready',
    progress_hook='myapp.hooks.on_progress',
)
for report_id in report_ids:
    iter_task.append(report_id)
iter_id = iter_task.run()
```

### Progress tracking

The `progress_hook` is called after each task completes (before the final hook). It receives:

```python
def on_progress(workflow_id, workflow_type, completed_count,
                total_count, success_count, failure_count):
    pct = (completed_count / total_count) * 100
    print(f"Progress: {pct:.0f}%")
```

## QraftBatch

Executes different functions in parallel (fork-join pattern). Each task can have its own retry policy and cluster routing.

```python
from qraft.batch import QraftBatch

batch = QraftBatch(
    on_success='myapp.hooks.generate_report',
    on_failure='myapp.hooks.partial_failure',
)
batch.append('myapp.tasks.fetch_sales', region='NA',
          qraft_options={'max_attempts': 3, 'cluster': 'io-workers'})
batch.append('myapp.tasks.fetch_inventory', warehouse='main')
batch.append('myapp.tasks.fetch_shipping', carrier='fedex')
batch_id = batch.run()
```

> **Note**: `QraftBatch.add()` is deprecated in favor of `append()` for API consistency.

## Cancellation

All workflow types support cancellation:

```python
chain.cancel()  # or iter_task.cancel() or batch.cancel()
```

Cancellation is cooperative: in-flight tasks will complete, but no further steps are queued and no completion hooks fire. The `on_cancelled` hook (if configured) is called instead.

```python
chain = QraftChain(
    on_cancelled='myapp.hooks.pipeline_cancelled',
)
```

## Runs

A chain owns its steps up front. A pipeline whose stages enqueue each other from
application code does not, and forcing it into a static step list means a chain per
branch. A run is the lighter primitive for that shape: it declares the names of the
stages it expects, and the application binds whatever it likes under them.

```python
from qraft import runs

run_id = runs.start(
    subject=("worksheet", 4117),
    stages=["ingest", "rules", "ai"],
    kind="shadow",                           # a metric label; keep it bounded
    revision="rules@v7",                     # never a label
    started_at=report.received_at,           # optional; defaults to now
    on_settled="revenue.hooks.run_settled",  # optional durable hook
)

async_task("revenue.tasks.ingest", worksheet_id,
           qraft_options={"run": run_id, "stage": "ingest"})

batch = QraftBatch(run=run_id, stage="rules")   # the batch is the unit
batch.append("revenue.tasks.revenue_pass", worksheet_id)
batch.append("revenue.tasks.entity_pass", worksheet_id)
batch.run()
```

`start()` returns the run id. A run implies its subject: a task or workflow that names
the run and no subject inherits it, and one that names a different subject raises.

### Binding the subject later

The stage that creates the domain object a run is about is often the run's own first
stage. Keying the run on whatever row existed beforehand splits one domain object across
two subject types, and the dashboard's subject filter then answers half the question.

`subject` may be omitted at `start()` and named once afterwards:

```python
run_id = runs.start(None, stages=["ingest", "rules", "ai"])
async_task("revenue.tasks.ingest", event_log_id,
           qraft_options={"run": run_id, "stage": "ingest"})

# ...inside the ingest task, once the worksheet exists:
runs.bind_subject(run_id, ("worksheet", worksheet.pk))
```

`runs.bind_subject(run_id, subject)` is allowed once, while the run is `OPEN` and its
subject is unset; a second call and a settled run both raise `RunError`. It updates the
run and every member already bound to it — tasks, chains, iters and batches — in one
transaction under the run's row lock, so the dashboard and admin subject filters find
the whole run, not the part enqueued after the bind. A member created at the same moment
takes that same lock, so it either lands before the bind and is updated by it or waits
and reads the bound subject. `QraftRun.bind_subject(subject_type, subject_id)` is the
same call on the model instance.

Until the bind, a member that names a subject of its own still raises: that is the case
`bind_subject` exists for, and taking it silently would leave the run unlabelled.

### One stage, one completion unit

A stage is bound to exactly one unit — a `QraftTask` or one workflow — and settles when
that unit does. A plain `async_task` with `run` and `stage` binds at enqueue; a workflow
constructed with them binds at `run()`, the moment work is committed.

A stage that needs several tasks uses an `Iter` or a `Batch`, whose membership closes at
`run()` and whose completion the parallel dispatcher already tracks. Members inherit
`run` and `stage` so they filter and log correctly, but a member's outcome never touches
the stage row — only the unit's does.

"Any successful task settles the stage" was the alternative and it is wrong the moment a
stage has more than one task: a rules stage with three passes, one finished and two
running, would settle, and a later failure could not reopen it.

### Settlement

When a unit settles, its outcome is recorded on its stage row under the run's lock, and
two rules apply:

- a `FAILED` or `CANCELLED` stage fails the run, immediately;
- otherwise, once every stage is `SUCCEEDED` or `SKIPPED`, the run succeeds.

Both are the `settled_at` compare-and-swap, so a run settles exactly once. A run whose
remaining stages have no unit stays `OPEN` — that is correct and visible, not something
Qraft times out on.

Explicit close was rejected as the only mechanism: the code that knows the pipeline is
done is the last stage's success path, the one place a crash loses the call. Deriving
from "no live members" was rejected too, because between stage N committing and stage
N+1's row existing the run has zero live members.

### Edge rules

Each is enforced at the bind, where a mistake is cheap. All raise `runs.RunError`.

1. Enqueueing into a terminal run raises.
2. Binding a stage that already has a unit raises. A stage runs once per run.
3. Binding a stage the run did not declare raises.
4. `runs.skip(run_id, stage, reason)` marks an unbound stage `SKIPPED`; skipping a stage
   that has a unit raises. A skipped stage still lets the run succeed — a skip is a
   decision the application made, not a failure Qraft observed.
5. `runs.cancel(run_id)` settles the run `CANCELLED`. Further binds raise. Units already
   in flight are not revoked: they finish, and their outcomes are still recorded on their
   stage rows. Only the run's own status and its `summary` are closed to them — the
   snapshot is written at settlement and a settled run is never mutated (rule 9).
6. `runs.abandon(run_id, reason)` settles an `OPEN` run `ABANDONED` — the operator's tool
   for a stage that was never enqueued. Also a dashboard action and an admin action.
7. `QraftChain.resume()` on a chain bound to a run proceeds only while the run is `OPEN`.
   A bound chain fails its run the moment it fails, so in practice the expected path
   after a failure is a new run.
8. Stage order is display order. Qraft does not enforce that `rules` waits for `ingest`.
9. A settled run is never mutated. A rerun is a new run with `previous_run` pointing at
   the old one; the dashboard shows the link.

### The durable completion event

`on_settled` is dispatched through the same idempotency the workflow hooks use, with a
`WorkflowHookDispatch` row keyed `("run", run_id, "settled")`. Because a run settles once
and is never reopened, that pair is a stable event identity the application can dedupe
on. The hook receives one `context` dict: `run_id`, `subject_type`, `subject_id`, `kind`,
`revision`, `metadata`, `outcome`, `stages` (name to outcome), `started_at`,
`settled_at`, `duration_s`, `previous_run_id`.

The hook is queued after the settlement commits, so a crash in that window would lose it.
Two things close that: a replayed unit settlement offers the hook again, and the reaper
sweeps settled runs whose dispatch row is missing. The `WorkflowHookDispatch` row is what
keeps either recovery to one dispatch.

The `run_settled` signal fires beside it for observers. The hook is what a "worksheet
ready" transition hangs on — see [Signals](hooks.md#best-effort-by-design) for why.

### Summary, retention and overdue

At settlement the run writes a `summary`: per stage the unit type and id, outcome,
`bound_at`, `settled_at` and duration; the run's duration; and the usage aggregated over
every member attempt. Retention may later prune the member rows; the run keeps the
answer. Tasks and workflows under a run that is still `OPEN` are never pruned, in either
pass. A terminal run is pruned after its members, with its stages cascading — and only
once no member is still live, since a cancel does not revoke work in flight and that work
still has an outcome to record.

`QRAFT_RUN_OVERDUE_AFTER` (seconds, default None) turns on a sweep in the reaper thread:
an `OPEN` run older than the threshold gets `overdue_flagged_at` set once, `run_overdue`
is sent, and the dashboard shows a badge. Nothing fails automatically — an unenqueued
stage is an application defect, and `skip`, `cancel` and `abandon` are the tools for it.

### Why not nested workflows

A chain of chains would model this pipeline too, and it stays on the roadmap. It is the
heavier fit here: a chain owns its steps' definitions before execution and enqueues each
one itself, while the motivating pipeline decides what to enqueue next inside application
code. A run whose stage is a chain already works under this design.

## Correlation kwargs

Every constructor takes `subject=`, `run=`, `stage=` and `hook_context=`:

```python
batch = QraftBatch(subject=('worksheet', 4117), hook_context=True)
batch = QraftBatch(run=run_id, stage='rules')   # subject inherited from the run
```

`subject`, `run` and `stage` are copied onto every member the workflow creates, so a
member filters and logs under the same entity; membership itself is still tracked through
the workflow foreign keys, and a member never binds or settles a stage.
`hook_context` gives the workflow-level hooks — `on_cancelled` included — one extra
`context` keyword argument carrying the workflow's id, type, outcome and counters. See
[Hooks](hooks.md#hook-context).

## Settlement identity

A workflow settles exactly once per run through it. Status alone cannot tell a first
settlement from a replayed one: a chain's final step never advances
`current_step_index`, so a redelivered completion passes the index check; a cancel can
race a completion; and `resume()` legitimately settles the same chain twice.

Each workflow model therefore carries `settled_at`, and settlement is one conditional
update on it. `workflow_settled` and the workflow hook fire only when that update matched
a row; `resume()` clears the column in the same transaction that moves the chain back to
RUNNING, so the next settlement is a genuinely new one and fires again. A workflow that
settled before 1.4.0 has a terminal status with a null `settled_at`, and the update
excludes terminal statuses, so a replay against an old row sends nothing.

Resuming also starts a new *generation*, which two more rules follow. The chain's
`WorkflowHookDispatch` rows are cleared with `settled_at`, so the next settlement's hook
is dispatched rather than deduped against the settlement being resumed from. And the
step's bound `QraftTask` identifies the generation: routing re-reads it, so a completion
captured before the resume — the reaper replaying a batch it read a moment ago, say — is
dropped instead of settling the chain the resume started.

## Result Objects

All `result()` methods return a `WorkflowResult` object:

```python
result = batch.result(wait=60000)

# Iterate over values (backward compatible with list)
values = list(result)

# Inspect details
result.succeeded       # list[TaskResult] — successful tasks
result.failed_results  # list[TaskResult] — failed tasks
result.values          # list[Any] — raw values
result.errors()        # list[dict] — structured error info

# Each TaskResult has:
for tr in result.task_results:
    print(tr.task_id, tr.func, tr.success, tr.result, tr.error, tr.attempt_count)
```

## Models

Workflow state is stored in the database:

| Model | Description |
|-------|-------------|
| `QraftChainModel` | Chain metadata, status, current step |
| `QraftChainStep` | Individual step definition + link to QraftTask |
| `QraftIterModel` | Iter metadata, atomic counters |
| `QraftBatchModel` | Batch metadata, atomic counters |
| `WorkflowHookDispatch` | Idempotent workflow hook and run `on_settled` tracking |
| `QraftRun` | Run metadata, status, `settled_at`, `summary` |
| `QraftRunStage` | One declared stage and the unit bound to it |

All three workflow models carry `subject_type`, `subject_id`, `run`, `stage` and
`settled_at`; `QraftTask` carries the same four correlation columns. `run` is `SET_NULL`
on both: deleting a run must never delete work.

## Status Lifecycle

```
PENDING → RUNNING → SUCCEEDED
                  → FAILED → RUNNING (resume, chain only)
                  → WAITING_APPROVAL → RUNNING   (approve, chain only)
                                     → CANCELLED (reject)
         → CANCELLED (from PENDING or RUNNING)
```
