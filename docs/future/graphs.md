# Execution graphs

## 1. Why

A pipeline that fails halfway has one question worth answering: what has to run again? Qraft cannot answer it.

Take the revenue tool, the consumer this design is drawn from. One worksheet goes through four rule engines. They are independent — the module that declares them says so — but they run inside one task, in a loop, and the loop re-raises on the first failure. So an entity-tagging bug stops the NSF and lender passes that had nothing to do with it, and the retry runs all four again. Nothing is lost, because the passes are cheap and rewriting their output is harmless. That is luck, not design.

The luck runs out at the next engine. An engine reimplemented as a provider call costs real money per run, wants its own cluster, and must not be re-paid for because a sibling failed. The consumer's answer today would be to read its own `EngineRun` rows, work out which engines have no result, and start a fresh run covering those. That is a graph: a plan, a frontier, generations, and a crash-safe dispatcher. It would be private to one application, untested, and invisible to the dashboard. Every other consumer would write it again.

Qraft has four workflow primitives and none of them can express this. `QraftChain` freezes its steps up front and makes every step wait for the one before it. `QraftBatch` runs everything at once and cannot resume at all. `QraftIter` maps one function over many inputs. `QraftRun`, unreleased, tracks named stages that the application enqueues itself, fails permanently on the first failed stage, and is never mutated once settled. Four partial answers to one question.

The question is: re-run this unit and whatever depended on it, keep everything else, and be right about what "keep" means.

## 2. Principles

**The plan belongs to qraft.** Whoever holds the plan owns resume. If the application persists it, qraft can offer nothing better than "resume this batch" and the application still works out which batch and which layers. Graph and nodes are qraft rows.

**Edges, not layers.** Re-running one rule engine must invalidate the AI node that read it and not the one that did not. A layer index cannot say that. An `after` list can, and it is no harder to declare.

**A kept node's completion must be provable, or declared unprovable.** Resume is worth nothing if "this node already succeeded" is a guess. For work whose writes go through the task's database connection, qraft can make the proof exact. For a provider call it cannot, and the design says so rather than implying otherwise.

**The topology is sealed at start.** A node may not add nodes to its running graph. This excludes dynamic expansion, which drags in expansion identity, membership sealing, and joins whose expected membership changes. No consumer needs it yet.

**Qraft owns execution identity; the application owns what a valid output is.** Qraft supplies stable ids, ordering, retry, and the commit boundary. It never decides whether an engine's results are correct or complete enough.

## 3. Design

A graph is one execution of a plan over one subject. It holds nodes, each of which is one unit of work with a key, a function, frozen arguments, and a list of node keys it runs `after`. Building a graph is ordinary Python: a loop over whatever the application knows, then `start()`, which writes the graph, every node, and the first jobs in one transaction.

Qraft dispatches every node whose `after` nodes have all succeeded or been skipped. When a node finishes, the dispatcher records its outcome under the graph's row lock and looks for newly ready nodes. When nothing is running and nothing can start, the graph settles: succeeded if every node succeeded or was skipped, failed if any node failed. `resume()` takes a set of node keys, adds everything reachable downstream, moves those nodes back to pending at the next generation, and dispatches the frontier again. Every other node keeps its result.

Nodes declare how their completion may be trusted. A `transactional` node publishes its writes and qraft's completion receipt in the same database transaction, so the record cannot say succeeded unless the effect committed, and the effect cannot have committed without the record being recoverable. An `idempotent` node promises that running twice is safe. A `manual` node stops as uncertain after an ambiguous crash.

```
  ExtendedTxnEventLog
          |
          v
  graph  worksheet:4117   generation 1   RUNNING
  ┌──────────────────────────────────────────────────┐
  │  ingest ─┬─> rules.revenue  SUCCEEDED  gen 1     │
  │          ├─> rules.entity   FAILED     gen 1  ✗  │
  │          ├─> rules.nsf      SUCCEEDED  gen 1     │
  │          └─> rules.lender   SUCCEEDED  gen 1     │
  └──────────────────────────────────────────────────┘
        settles FAILED when nothing is left running

  resume(graph_id)         rerun = {rules.entity}
                           kept  = {ingest, rules.revenue, rules.nsf, rules.lender}

  ┌──────────────────────────────────────────────────┐
  │  ingest ─┬─> rules.revenue  SUCCEEDED  gen 1     │
  │          ├─> rules.entity   RUNNING    gen 2     │
  │          ├─> rules.nsf      SUCCEEDED  gen 1     │
  │          └─> rules.lender   SUCCEEDED  gen 1     │
  └──────────────────────────────────────────────────┘
```

## 4. Detail

### 4.1 Data model

The graph is the unreleased `QraftRun` and `QraftRunStage`, evolved. Nothing about runs has shipped: they are listed under "Shipped after 1.3.0 (unreleased)" in the roadmap, migrations 0011 to 0015 are unreleased, and the one consumer sits behind a default-off flag. The shape can change without a deprecation, and it should, because a second primitive covering the same ground would be worse than one that covers it properly.

`QraftGraph`, from `QraftRun`, keeps subject, `kind`, `revision`, `metadata`, `previous_graph`, `status`, `date_started`, `settled_at`, `overdue_flagged_at`, `on_settled`, `on_settled_kwargs`, `budgets`, and `summary`. It gains `generation` (default 1), `cluster` (the routing default for nodes that name none), and `resumed_at`. `ABANDONED` leaves the status set: its reason was a stage nobody ever enqueued, which cannot happen once qraft dispatches.

`QraftGraphNode`, from `QraftRunStage`, keeps `graph`, `position`, `status`, `skip_reason`, and `settled_at`. It renames `name` to `key` and `bound_at` to `dispatched_at`, and drops `unit_type` and `unit_id` — a node is a task, never an arbitrary unit the application binds. It gains:

| Field | Purpose |
|---|---|
| `after` | JSON list of node keys. Validated at `start()`: known keys, no self-edge, no cycle |
| `func`, `task_args`, `task_kwargs`, `options` | The frozen call, following `QraftChainStep`'s shape |
| `task` | FK, `SET_NULL`. The current generation's task, and the generation marker |
| `generation` | Bumped on resume |
| `depth` | Longest path from a root, derived at `start()`. Display order, and the answer to "which layer" |
| `recovery` | `transactional`, `idempotent`, or `manual`. No default |

`QraftTask` replaces its `run` and `stage` columns with `graph` (FK, `SET_NULL`) and `node` (char) for correlation, and gains `graph_node` (FK, `CASCADE`), set only on a node's own task. The two are distinct on purpose: correlation says "this task belongs to that graph", membership says "this task is that node's current execution". A stale-generation completion is then a task whose `graph_node` no longer points back at it.

`QraftTaskAttempt` gains `output_committed_at`, described in section 4.5.

One generated migration, `0016_graphs`, carries the renames, the added columns, and the choice changes. Every added column is nullable or carries a `db_default`. There is nothing to backfill.

### 4.2 Building and starting

`Graph` is an in-memory builder. No row exists for a plan that is never started, unlike `QraftChain`, which writes its model in `__init__`.

```python
from qraft import graphs

g = graphs.Graph(
    subject=None,
    kind="shadow",
    revision="rules@v7",
    cluster="revenue",
    started_at=event_log.completion_timestamp,
    on_settled="revenue.pipeline.on_graph_settled",
    budgets={"openai_requests": 400},
)
g.node("ingest", "revenue.pipeline.ingest_node", event_log.pk,
       recovery="transactional", qraft_options={"stall_after": 300})
for category in active_categories:
    g.node(f"rules.{category}", "revenue.pipeline.rules_node", event_log.pk, category,
           after=["ingest"], recovery="transactional",
           qraft_options={"max_attempts": 2, "stall_after": 300})
graph_id = g.start()
```

`node(key, func, *args, after=(), recovery, qraft_options=None, **kwargs)` follows `QraftChain.append`, including its guard against option keys colliding with the function's own keyword arguments. Accepted option keys are every `RetryPolicy.from_options` key plus `cluster`, `priority`, `stall_after`, `success_hook`, `failure_hook`, and `hook_context`. An unrecognised key raises at `node()` rather than being dropped silently, which is what `from_options` does today and is worth not repeating.

`start()` validates the topology, computes each node's `depth`, and writes the graph, all node rows, and the root nodes' tasks and scheduled attempts in one transaction. A crash during `start()` leaves nothing.

`start(request_key=...)` gives submission idempotency: the same key with the same plan returns the existing graph, the same key with a different plan raises. Its retention horizon is the graph retention horizon, so an application needing permanent request deduplication keeps its own request row.

### 4.3 Dispatch

Dispatching a node creates its `QraftTask` and a `SCHEDULED` attempt through `scheduler.schedule_attempt()` with `not_before=now`, under the graph's row lock. No broker call happens under that lock.

The scheduler's existing dispatch loop claims the attempt by compare-and-swap and enqueues it. `dispatch_interval` bounds how long that loop sleeps when idle, at 0.5 seconds by default, and it wakes earlier when an attempt comes due sooner. This reuses the crash safety that path already has: on the ORM broker the claim and the enqueue commit together, and on any other broker `rearm_stuck_claims()` recovers a claim that never reached the broker.

Enqueueing directly under the lock, the way `_queue_chain_step` does, was rejected. That function's own docstring records the failure it accepts: a broker error leaves the chain running at step *i* until somebody intervenes. With a scheduled row a broker outage delays the graph and loses nothing.

The cost is a scheduler hop per edge traversal, bounded by the idle sleep and usually shorter. For nodes measured in seconds or minutes that is invisible. A graph whose nodes are individually sub-second should be one node.

### 4.4 Settlement and resume

`route_workflow_completion` gains a branch: a task whose `graph_node` is set goes to the graph dispatcher, which runs in one transaction under the graph's row lock.

It re-reads the node's `task_id` first. If the completing task is not the one the node currently points at, the completion belongs to a superseded generation and is dropped. Then the node settles by compare-and-swap on `settled_at`, so a replayed completion matches zero rows and returns. Then the dispatcher looks for every pending node whose `after` keys are all succeeded or skipped, and dispatches them.

A graph settles when it is quiescent: nothing running, and nothing newly dispatched. Succeeded if every node succeeded or was skipped, failed if any failed, cancelled if any was cancelled and none failed.

Settling on quiescence rather than on the first failure is deliberate. It buys two things. `resume()` can refuse to run while any node is in flight, which removes every race between a resume and a straggler. And `on_settled` fires once with the whole picture rather than while three siblings are still working. What it gives up is immediacy: a failed root with a long-running sibling reports `FAILED` only when the sibling ends. Nothing is hidden in the meantime, because `node_settled` fires per node and the dashboard shows the failure when it lands.

`resume(graph_id, nodes=None)` is allowed on a failed graph, or on a succeeded one when `nodes` is explicit — that second form is how a finished graph is partially re-run against the same plan. With `nodes=None` the named set is every failed node.

The rerun set is the named nodes plus every node reachable from them along edges that is not still pending. Each gets its status reset to pending, its `task` cleared, its `settled_at` cleared, and its generation incremented; the old task keeps its `graph_node` link as history. The graph then clears `settled_at`, returns to running, increments its own generation, stamps `resumed_at`, deletes its `WorkflowHookDispatch` rows so the next settlement dispatches its hook again, and dispatches the new frontier. One transaction.

`preview_resume()` returns the rerun and kept sets without writing. The dashboard shows it, with the kept set's recorded usage and cost, before the button commits anything.

A failed node outside an explicit rerun set stays failed, and the graph settles failed again when the named nodes finish. A wrong plan is not resumable: the plan is frozen at `start()`, and the answer is a new graph with `previous_graph` set.

### 4.5 Completion proof

This is the section the rest of the design rests on, because resume is only as good as the claim that a kept node is genuinely done.

The gap is narrow and real. A node commits its results. Before django-q2's monitor records the task as successful, the worker dies. The work is durable; qraft's record says the attempt failed. Resume re-runs a node that had already finished, and the application writes a second set of results.

A `transactional` node closes that gap by committing its writes and qraft's receipt together. The application publishes inside a boundary qraft supplies:

```python
def rules_node(event_log_id, category):
    ctx = qraft.context.current_node()
    result = evaluate_pass(event_log_id, category)     # slow, no transaction held

    with ctx.commit() as completion:                   # short transaction
        engine_run = publish_results(result)
        completion.succeed({"engine_run_id": engine_run.pk})
```

`ctx.commit()` opens a transaction on the task's connection, locks the node's publication authority, checks that this attempt still holds it, checks that no receipt exists, yields to the application's writes, then writes the receipt and stamps `output_committed_at` on the attempt in the same transaction.

Computation stays outside. Wrapping the whole node in a transaction is simpler and was rejected: it is fine for a rule pass measured in tenths of a second and wrong for anything longer, which is exactly the work whose recovery matters most.

The crash cases resolve cleanly. A crash before the commit rolls back both the writes and the receipt, so nothing happened and the node is safe to re-run. A crash after the commit leaves both, and the reaper resolves the attempt as succeeded on the strength of the receipt rather than reaping it as lost. A connection dropped during `COMMIT` is not guessed at: recovery reads the receipt on a new connection. If the Q2 row later reports failure while a receipt exists, the receipt wins and the anomaly is recorded.

The contract has two requirements the application must honour. Every completion-critical write goes through that transaction and that connection; pointing two aliases at the same Postgres server is not enough. And nothing completion-critical happens after `succeed()` — that belongs before the receipt, or in another node.

Publication authority is a fencing token, not just a late-message check. Dropping a late attempt's result, which the hook handler does today, stops the accounting from being corrupted but does nothing to stop the original function from continuing to write. Revocation, reaping, and replacement all serialise against the same node lock that `ctx.commit()` takes, so if an old attempt reaches the lock first its receipt wins and no replacement is scheduled, and if revocation wins first the old attempt's publication fails before writing. Several attempts may physically execute during a partition or a false orphan diagnosis. At most one can publish. Writes made outside the boundary are not fenced, and the documentation must say so plainly.

`idempotent` nodes get no such guarantee and do not pretend to. A provider can accept a request and bill for it a moment before the worker dies. Qraft supplies the stable identity — graph id, node key, generation, task id, attempt id — and the application keys its own deduplication on it. Two patterns carry the weight: a unique key that includes the identity, with readers preferring the newest; or a conditional update filtered on the owning attempt. Qraft cannot enforce either.

`manual` nodes stop as uncertain after an ambiguous crash and wait for a person.

Requiring the declaration, with no default, is the point. A silent default would decide the most consequential property of a node on the author's behalf.

### 4.6 Observability

Everything is rows in the consumer's own database. `QraftGraph.objects.for_subject(("worksheet", 4117))` finds the graph; `graph.nodes` orders by `depth` then `position`; the tasks behind a node, including superseded generations, are `QraftTask.objects.filter(graph_node=...)`.

Inside a node, `qraft.context.current_node()` returns the identity: graph id, node key, generation, task and attempt ids, attempt number, subject, and metadata. The application stores those as plain columns on its own tables and never as foreign keys, because qraft prunes its rows and the application's output outlives them.

`graphs.snapshot(graph_id)` returns one JSON-safe dict carrying the graph's status, generation, subject, revision, timings, budgets, usage and cost; per node its key, depth, `after`, status, generation, cluster, recovery mode, timings, attempts with exception classes and durations, progress with its advanced-age and stall flag, usage, and `blocked_by`; plus the current frontier and the resume preview. The dashboard renders that dict. A consumer building its own view renders the same dict. Qraft publishes data, not a widget, which is why there is no view mixin to bind the design to one presentation framework.

`blocked_by` earns its place by answering the first question of any incident: why is nothing running.

Nodes get their own durable `success_hook` and `failure_hook` through `HookDispatch`. Workflow members have none today because the workflow is the event; in a graph the nodes are the events. The revenue consumer writes its per-engine failure record from a best-effort signal today and should be writing it from a durable hook.

New signals are `node_settled` and `graph_settled`; every attempt payload gains graph id, node key, and generation. Metrics rename to `qraft.graph.*` and add `qraft.node.settled{kind, key, outcome}` and `qraft.node.duration{kind, key}`. The node key is a metric label because it is a bounded application vocabulary, like `kind` and `func`. Graph id, task id, and generation never are.

### 4.7 Migrating the revenue consumer

`start_pipeline_run` builds a graph with `ingest` and one `rules.<category>` node per active category, all `transactional`. `run_passes` splits so that one pass is independently callable; the `EngineRun` row it currently creates outside its own transaction moves inside, because the scaffolding existed only to leave evidence of a failure, and a receipt is better evidence. `EngineRun` gains a plain `qraft_task_id` column, not a foreign key.

`_rerun_if_versions_moved` becomes `resume(nodes=["rules.*"])` when the revision is unchanged and a new graph with `previous_graph` when it is not.

`start_manual_scoring_run` becomes a one-node graph, `idempotent`, carrying the provider budget and the AI retry policy. Its `_should_exclude_scored` currently keys on `attempt_number > 1`; a resume creates a fresh task and resets that counter, so it must key on the node's generation instead. This is a live hazard, not a hypothetical: the same function was changed this cycle to make every retry skip already-scored rows, and a graph resume would silently undo that.

The pipeline's version pinning has to be fixed in the same work. `active_revision()` records the versions active when the run opened, but `run_shadow` calls `run_passes` without pinning them, so execution re-resolves whatever is active at the time. The recorded revision is a label, not an input snapshot, and a resumed node would run against different rules than its siblings. Freeze the versions at `start()` and pass them into every node.

### 4.8 Delivery

Four phases, each independently useful, recovery before visualisation.

**Phase 1 replaces the Run before 1.4.0 ships.** Models and migration `0016_graphs`, `qraft/graphs.py`, the graph dispatcher, dispatch through scheduled attempts, quiescent settlement, `on_settled` and its replay, node-level hooks, `current_node()`, the signal and metric renames, a dashboard panel with cancel and skip, retention, and the docs. The revenue consumer moves to one node per engine. There is no resume yet and the phase still pays for itself: four engines that fail independently, route independently, and retry independently.

**Phase 2 adds resume.** Generations, the rerun closure, `preview_resume()`, and the dashboard's resume-with-preview. The revenue promotion rerun moves onto it.

**Phase 3 adds the completion contract.** `output_committed_at` and the node receipt, the `recovery` declaration, `ctx.commit()`, publication fencing, and the reaper and reconciler rules that make a receipt authoritative. The revenue engines become `transactional` and their evidence scaffolding is deleted. This is the phase that makes phase 2's "kept" claim exact, and it is worth building in that order: resume is useful under an at-least-once contract, provided the docs say so.

**Phase 4 is approval gates, then the older primitives.** Gates are additive and land with the graph. Folding `QraftChain` and `QraftBatch` into facades is the breaking half, gated on somebody needing it rather than on the revenue consumer. A chain is nodes with a single `after` edge each; a batch is nodes with none. Both keep their current classes as facades over the graph, and both gain resume by construction — `QraftBatch` has none today. The old models stay readable for one release so work in flight can finish. `QraftIter` is not re-based: ten thousand rows of one function with a counter is the right shape for an iter and the wrong shape for a graph, which is also why a graph enforces a node ceiling.

### 4.9 Experiments before phase 3

Three questions cannot be settled by reading.

`ctx.commit()` has to be exercised against a target that calls `connections.close_all()`, which the revenue AI task does. It must fail loudly rather than commit half its work.

Dispatch throughput needs a number: two hundred graphs of five roots each, submitted in one second on the ORM broker, measured to the last root queued. The result decides whether the 0.5 second dispatch ceiling needs lowering for graph work.

The lock behaviour under wide fan-in needs the same treatment: five hundred nodes with two hundred and fifty completing within a second, measuring lock wait on the graph row.

## 5. Decisions

| # | Decision | Section |
|---|---|---|
| 1 | Evolve the unreleased Run into the graph rather than add a primitive beside it | 4.1 |
| 2 | Qraft owns the plan and the dispatch; the application does not enqueue nodes | 2, 4.3 |
| 3 | Explicit `after` edges, not layer indices | 2 |
| 4 | Topology sealed at `start()`; no node adds nodes | 2 |
| 5 | No result passing between nodes; qraft does not own application outputs | 4.2 |
| 6 | Dispatch by scheduled attempt, not by direct enqueue under the lock | 4.3 |
| 7 | Settle on quiescence, not on first failure | 4.4 |
| 8 | Resume in place with generations, not resume as a new graph | 4.4 |
| 9 | Rerun set is the named nodes plus their downstream closure | 4.4 |
| 10 | `recovery` is declared per node with no default | 4.5 |
| 11 | Publication is fenced by node authority, not by dropping late messages | 4.5 |
| 12 | Computation stays outside the publication transaction | 4.5 |
| 13 | Snapshot JSON is the visibility contract; no view mixin | 4.6 |
| 14 | Node key is a metric label; graph id, task id and generation are not | 4.6 |
| 15 | Node-level hooks are durable, unlike workflow member hooks | 4.6 |
| 16 | `graph_node` cascades, `graph` sets null: membership against correlation | 4.1 |
| 17 | Chain and Batch become facades in a later major; Iter is not re-based | 4.8 |
| 18 | `ABANDONED` is dropped; overdue observation is kept | 4.1 |

## 6. Scope edges

Out of scope, and belonging in the roadmap's "Deliberately not planned": dynamic node expansion; result passing; a node that is itself a workflow; automatic resume, since the retry policy owns attempts and a resume is a decision made with a preview in front of it; graph timeouts that fail a graph, for the same reason run timeouts were refused; cross-graph edges; and a renderer beyond a layered list, since a graph picture is not what an incident needs first.

Three questions are open, each with a recommendation.

**How many nodes is too many?** A ceiling exists to keep the frontier scan and the graph lock honest, and five hundred is a guess. Recommendation: ship five hundred as a setting, and revisit against the phase 3 fan-in experiment rather than against intuition.

**Should a node carry a timeout?** Scheduled attempts run under the cluster's timeout, so today the answer is a cluster per timeout class. Recommendation: leave it there for phase 1 and reconsider only if a consumer has nodes whose durations differ by more than an order of magnitude within one cluster.

**Does the graph need a concurrency cap?** Backpressure today is worker count, throttles and budgets. Recommendation: no cap in phase 1. Dispatch through scheduled rows makes a later cap a small change with no new state, so deferring costs nothing.
