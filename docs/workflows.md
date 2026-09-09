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

## Graphs

A chain owns its steps up front. When the full pipeline topology is known at submission
time, declare it as an execution graph: explicit `after` edges between nodes, qraft-owned
dispatch, and quiescent settlement when nothing is running and nothing can start.

```python
from qraft import graphs

builder = graphs.Graph(
    subject=("worksheet", 4117),
    kind="shadow",                             # a metric label; keep it bounded
    revision="rules@v7",                       # never a label
    started_at=report.received_at,             # optional; defaults to now
    on_settled="revenue.hooks.graph_settled",  # optional durable hook
    budgets={"openai_requests": 40},           # optional request budgets
    cluster="io-workers",                      # default cluster for nodes
)
builder.node(
    "ingest", "revenue.tasks.ingest", worksheet_id,
    recovery="transactional",
    qraft_options={"max_attempts": 3},
)
builder.node(
    "rules", "revenue.tasks.rules_pass", worksheet_id,
    after=("ingest",),
    recovery="idempotent",
)
builder.node(
    "ai", "revenue.tasks.ai_summary", worksheet_id,
    after=("rules",),
    recovery="manual",
)
graph_id = builder.start()
```

`start()` validates the topology (unique keys, known dependencies, no cycles), writes
`QraftGraph` and `QraftGraphNode` rows in one transaction, and dispatches every root
node. Each dispatch creates a `QraftTask`, binds it to the node row, and enqueues the
first attempt through `scheduler.schedule_attempt()` — the same path retries use.

Every node must declare `recovery` (`transactional`, `idempotent`, or `manual`). The
column exists for phase-2 resume; today it is recorded on the node and surfaced in
`snapshot()`.

`start()` returns the graph id. A graph implies its subject: correlated work that names
the graph and no subject inherits it, and one that names a different subject raises.

### Binding the subject later

The node that creates the domain object the graph is about may need to run before the
subject exists. Keying the graph on whatever row existed beforehand splits one object
across two subject types.

`subject` may be omitted on the builder and named once afterwards:

```python
builder = graphs.Graph()
builder.node("ingest", "revenue.tasks.ingest", event_log_id, recovery="transactional")
builder.node("rules", "revenue.tasks.rules", after=("ingest",), recovery="transactional")
graph_id = builder.start()

# ...inside the ingest task, once the worksheet exists:
graphs.bind_subject(graph_id, ("worksheet", worksheet.pk))
```

`graphs.bind_subject(graph_id, subject)` is allowed once, while the graph is `RUNNING`
and the subject is unset; a second call and a settled graph both raise `GraphError`. It
updates the graph and every member already bound to it — tasks, chains, iters and batches —
in one transaction under the graph's row lock. `QraftGraph.bind_subject(subject_type,
subject_id)` is the same call on the model instance.

### Dispatch and settlement

Qraft owns dispatch. When a node settles (`SUCCEEDED`, `FAILED`, `SKIPPED`, or
`CANCELLED`), the frontier is re-evaluated and every pending node whose `after`
dependencies are all `SUCCEEDED` or `SKIPPED` is dispatched. Nodes at the same depth
with no dependency between them run in parallel.

The graph settles on quiescence — nothing `RUNNING`, nothing `PENDING` that could still
be dispatched — not on the first failure. A failed node blocks its descendants; siblings
already in flight finish. The graph outcome is `FAILED` if any node failed, `CANCELLED` if
any was cancelled, otherwise `SUCCEEDED` when every node is `SUCCEEDED` or `SKIPPED`.
Settlement is one compare-and-swap on `settled_at`, so it fires exactly once.

### Selective resume

A settled graph can be re-run in part. `resume(graph_id, nodes=None)` moves a set of
nodes back to pending at the next generation and dispatches the frontier again; every
other node keeps its result.

```python
graphs.preview_resume(graph_id)
# {"rerun": ["rules.entity"], "kept": ["ingest", "rules.revenue", "rules.nsf"]}

graphs.resume(graph_id)          # every failed node
graphs.resume(graph_id, ["rules.lender"])   # an explicit set
```

The rerun set is the named nodes plus **every node reachable from them along edges that
ever ran**. Re-running `rules.lender` therefore re-runs an `ai.lender` node that read it
and leaves `ai.category` alone. A node still pending is left out: it has no result to
invalidate, and the frontier dispatches it when its dependencies are met.

With `nodes=None` the named set is every failed node. Naming nodes explicitly is also how
a *succeeded* graph is partially re-run, against the same plan and revision; a graph that
succeeded refuses a resume that names nothing, so a re-run is always deliberate.

Resume is refused while the graph is running, which is what settling on quiescence buys:
there is no race between a resume and a straggler. It is also refused on a cancelled
graph, because a cancel is a decision rather than a fault.

Each rerun node's task link is cleared, so a completion from the generation before the
resume is dropped rather than settling the node. The graph's own `WorkflowHookDispatch`
rows are deleted in the same transaction, so the next settlement dispatches `on_settled`
again instead of deduping against the settlement being resumed from.

A wrong plan is not resumable. The plan is frozen at `start()`; the answer is a new graph
with `previous_graph` set.

### Publishing a node's output

Django-Q2 records a task's outcome after the worker returns, so a crash in between leaves
work committed and the attempt looking failed. A retry then redoes work that was already
durable, and a resume's "kept" set becomes a guess rather than a fact.

`graphs.publish()` closes that window for writes that go through the task's connection:

```python
def rules_node(event_log_id, category):
    result = evaluate_pass(event_log_id, category)      # slow, no transaction held

    with graphs.publish() as completion:                # short transaction
        engine_run = write_results(result)
        completion.succeed({"engine_run_id": engine_run.pk})
```

Inside the block the application's writes and the node's receipt commit together, so the
receipt exists if and only if the effect does. A body that raises rolls back both. A body
that never calls `succeed()` is refused, because a publication nobody declared is a bug
rather than a silent no-op.

Two requirements come with it. Every completion-critical write must use that transaction
and that connection — pointing two database aliases at the same server is not enough — and
nothing completion-critical may happen after `succeed()`. Computation belongs outside the
block; wrapping a minutes-long provider call in a transaction is the mistake this shape
exists to avoid.

Outside a graph node the block is an ordinary transaction that records nothing, so the
same function is callable from a plain task.

**Publication authority.** The block locks the node row and refuses an attempt the node is
no longer bound to. A straggler from before a resume therefore fails at the boundary
rather than writing behind the resume's back, and a node can publish only once per
generation. Several attempts may physically execute during a partition or a false orphan
diagnosis; at most one can publish. Writes made outside the block are not fenced.

**What recovery does with it.** When the reaper finds an unresolved attempt with a dead
lease and a commit stamp, it resolves the attempt as *succeeded* and advances the graph,
because the receipt is authoritative and the lost Django-Q2 result says nothing about
whether the work happened. Without a stamp the same attempt is reaped as `ResultLost` or
`OrphanedTask` and the retry policy decides.

For a provider call no transaction can help: the call may be billed a moment before the
worker dies. Declare such a node `idempotent`, key the application's own deduplication on
the identity `current_node()` supplies, and do not expect exactly-once.

### Approval gates

A node can park for a person before it runs:

```python
g.node("publish", "app.tasks.publish", after=["rules"],
       recovery="transactional", requires_approval=True)

graphs.approve(graph_id, "publish")
graphs.reject(graph_id, "publish", reason="numbers look wrong")
```

Once its dependencies are met the node goes to `WAITING_APPROVAL` instead of dispatching.
The gate stops that node and nothing else: siblings on the same frontier still run. When
no node is left running and one is parked, the graph itself reads `WAITING_APPROVAL` — it
is not settled and not failed, and the overdue sweep leaves it alone, because waiting on a
person is not running late.

`approve()` dispatches the node under the graph's lock, so a cancel arriving afterwards
finds either a parked node or a dispatched one and never the gap between them. `reject()`
cancels the node with its reason and the graph settles cancelled.

### Edge rules

Each is enforced where the mistake is cheap. All raise `graphs.GraphError`.

1. `graphs.skip(graph_id, node_key, reason)` marks a `PENDING` node `SKIPPED`; skipping a
   running or settled node raises. Skipped dependencies unblock descendants the same way
   successful ones do.
2. `graphs.cancel(graph_id)` settles the graph `CANCELLED`. A second call raises. Work
   already in flight is not revoked; nodes still record their outcomes.
3. Correlating work onto a terminal graph raises.
4. A settled graph is never mutated. A rerun is a new graph with `previous_graph` pointing
   at the old one.
5. `start(request_key=...)` is idempotent: the same key and plan hash return the existing
   graph; the same key with a different plan raises.

### The durable completion event

`on_settled` is dispatched through the same idempotency workflow hooks use, with a
`WorkflowHookDispatch` row keyed `("graph", graph_id, "settled")`. The hook receives one
`context` dict: `graph_id`, `subject_type`, `subject_id`, `kind`, `revision`, `metadata`,
`outcome`, `generation`, `nodes` (key to outcome), `started_at`, `settled_at`,
`duration_s`, `previous_graph_id`.

The hook is queued after settlement commits. `replay_settled_hooks()` re-offers it for
settled graphs whose dispatch row is missing.

`graph_settled` and `node_settled` fire beside the hook for observers. Metrics:
`qraft.graph.settled`, `qraft.graph.duration`, `qraft.graph.report_to_ready` (success
only), `qraft.node.settled`, `qraft.node.duration`.

### Snapshot, budgets and overdue

`graphs.snapshot(graph_id)` returns a JSON-safe view: graph metadata, `frontier` (pending
nodes ready to dispatch), per-node status, `blocked_by`, attempts, usage, and recovery
mode. It is the visibility contract for dashboards and consumers.

`budgets` on the builder declares request allowances; `qraft.context.consume_budget(key)`
spends them atomically.

`QRAFT_GRAPH_OVERDUE_AFTER` (seconds, default None; falls back to `QRAFT_RUN_OVERDUE_AFTER`)
turns on a reaper sweep: a `RUNNING` graph older than the threshold gets
`overdue_flagged_at` set once and `graph_overdue` is sent. Nothing fails automatically —
`skip` and `cancel` are the tools for it.

At settlement the graph writes a `summary`: per-node task id, outcome, dispatch and settle
times, duration, skip reason, and generation; the graph duration; and usage aggregated
over every member attempt.

### Correlation from inside a node task

`qraft.context.current_node()` returns the executing node's identity — graph id, node key,
generation, task and attempt ids, subject, and graph metadata — or `None` outside a graph
node delivery.

## Correlation kwargs

Every constructor takes `subject=`, `graph=`, `node=` and `hook_context=`:

```python
batch = QraftBatch(subject=('worksheet', 4117), hook_context=True)
batch = QraftBatch(graph=graph_id, node='rules')   # subject inherited from the graph
```

`subject`, `graph` and `node` are copied onto every member the workflow creates, so a
member filters and logs under the same entity; membership itself still rides on the
workflow foreign keys. Graph node tasks are dispatched by qraft, not `async_task`; members
correlated onto a graph node never settle it.
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
| `WorkflowHookDispatch` | Idempotent workflow hook and graph `on_settled` tracking |
| `QraftGraph` | Graph metadata, status, `settled_at`, `summary` |
| `QraftGraphNode` | One declared node, its task binding, and outcome |

All three workflow models carry `subject_type`, `subject_id`, `graph`, `node` and
`settled_at`; `QraftTask` carries the same four correlation columns. `graph` is `SET_NULL`
on both: deleting a graph must never delete work.

## Status Lifecycle

```
PENDING → RUNNING → SUCCEEDED
                  → FAILED → RUNNING (resume, chain only)
                  → WAITING_APPROVAL → RUNNING   (approve, chain only)
                                     → CANCELLED (reject)
         → CANCELLED (from PENDING or RUNNING)
```
