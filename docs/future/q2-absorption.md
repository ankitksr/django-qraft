# Django-Q2 absorption plan

Qraft owns task state, retries, hooks, and orchestration. Django-Q2 still owns
one thing: the worker loop. This document describes the phases that remove the
Q2 dependencies. Phase 1 (the execution lease and heartbeat, shipped in 1.2.1)
moved crash detection onto Qraft-owned state. Phase 2 (owned scheduling,
shipped in 1.3.0) moved delayed execution onto Qraft-owned rows.

Both external reviews (codex, 2026-08) reached the same conclusion: durability
that is inferred from another library's persistence is the weakest part of the
architecture. Each phase below replaces one inference with owned state.

## Phase 2 — own scheduling (shipped in 1.3.0)

Implemented as designed: `qraft/scheduler.py`, migration `0006_scheduler_owned`,
all three delay paths converted. The marker fallback in the hook handler stays
for one release as the bridge for pre-1.3 schedules still in flight.

### The problem

Three paths delay a task today: retries (`RetryPolicy.schedule_retry`), DLQ
requeues (`dlq.requeue`), and deferred backend tasks
(`QraftTaskBackend._enqueue_deferred`). All three write a Django-Q2 `Schedule`
row with `repr()`-encoded args, and Django-Q2's scheduler later `eval()`s that
text and enqueues the task.

This round trip has five costs:

1. The `repr()`/`eval()` encoding is fragile. Values that do not survive a
   text round trip (datetimes, Decimals, nested structures) corrupt silently.
2. The scheduler runs on a ~30 second cycle, so a 2-second backoff really
   fires after up to 32 seconds.
3. Priority lanes are lost — a scheduled task always re-enters the default
   lane.
4. The attempt row cannot exist before execution, so the hook handler needs
   the marker-parsing fallback (`qraft:{id}:{attempt}` in the task name) and
   the dual-lookup complexity that comes with it.
5. The mechanism lives in a process Qraft does not control.

### The replacement

Scheduling becomes a state on Qraft's own tables. A `QraftTaskAttempt` gains a
`not_before` timestamp and a `SCHEDULED` state. To delay a task, Qraft creates
the attempt row up front — with its final attempt number, its priority lane,
and `not_before` set to the due time.

A dispatcher loop (a daemon thread beside the monitor, exactly like the
reaper) polls for due attempts and enqueues each one directly to the broker.
Because the attempt row exists before enqueue, the q2 task id is recorded at
enqueue time, and the hook handler resolves every completion through the one
fast-path lookup.

### What changes

| Code | Change |
|---|---|
| `qraft/models/tasks.py` | `not_before` field and a `SCHEDULED` attempt state (one migration) |
| `qraft/scheduler.py` (new) | The dispatcher loop: poll due attempts, enqueue, stamp the q2 task id |
| `qraft/retry.py` | `schedule_retry()` creates a due attempt instead of a `Schedule` row |
| `qraft/dlq.py` | `requeue()` does the same |
| `qraft/backend.py` | `_enqueue_deferred()` does the same |
| `qraft/cluster.py` | Start the dispatcher thread beside the monitor |
| `qraft/hooks.py` | Keep the marker fallback for one release as a migration path, then delete it and the dual-lookup documentation |

### What this buys

Retry delays become exact. Priority survives retries. The marker format, the
`repr()` encoding, and the reactive attempt creation all disappear. Django-Q2's
`Schedule` model and scheduler are no longer imported.

## Phase 3 — own the worker loop

### The problem

Django-Q2's process tree still executes everything: the pusher moves packs
from the broker to an internal queue, workers execute functions, the monitor
saves results and calls the Qraft hook. Qraft's task state is updated at one
step removed — the hook handler translates Q2's result into Qraft's state.
Every remaining review finding traces to this indirection: the Q2 result row
as a completion signal, the rejected `sync`/`save`/`cached` modes, the
at-least-once redelivery semantics Qraft cannot tune.

### The replacement

A Qraft-native worker loop on the pattern PostgreSQL queues use
(`SELECT ... FOR UPDATE SKIP LOCKED`):

1. One queue table replaces the broker, the pusher, and the pack encoding.
   A worker claims a due attempt directly with `SKIP LOCKED`, updates the
   lease in a short transaction, then commits the claim before executing user
   code. Completion is another short transaction, fenced by the claim identity.
   Arbitrary network calls must not hold the claim transaction open. Applications
   can use a separate publication transaction for database effects and receipts;
   external effects still require application idempotency.
2. The monitor process disappears — completion dispatch (hooks, workflow
   routing) runs in the worker immediately after the result is written, or in
   a small dispatch thread. The reaper and heartbeat stay as they are; they
   already operate on Qraft-owned state.
3. The supervisor (sentinel role) stays: spawn workers, watch them, recycle
   them. It becomes Qraft code with no `Sentinel` subclassing.

### What changes

| Code | Change |
|---|---|
| `qraft/models/` | Queue claim fields on the attempt (or a small claim table) |
| `qraft/worker.py` | Becomes the real execution loop: claim, lease, execute, record, dispatch |
| `qraft/cluster.py` | The supervisor stops subclassing `Cluster`/`Sentinel` |
| `qraft/hooks.py` | `qraft_hook_handler` becomes an internal call, not a Q2 callback |
| `qraft/brokers.py` | Deleted — priority is an `ORDER BY` on the claim query |
| `pyproject.toml` | `django-q2` becomes optional (compat extra) and later leaves |

### What this buys

Exactly-once state transitions Qraft defines itself, `sync`/`save`/`cached`
semantics Qraft can support instead of reject, sub-second pickup without a
polling scheduler, and one process model to document. The cost is real: the
loop must reproduce what Q2 already solved (signal handling, recycling,
timeouts, backpressure), and it is the step that makes Qraft a queue rather
than an extension.

## Latency: DB as truth, wakeups as transport

The queue table (phase 3) does not have to be slow. The latency of a polled
Postgres queue comes from the poll interval, not from Postgres. Two additions
close the gap to Redis:

1. **`LISTEN`/`NOTIFY` wakeups.** The enqueue transaction sends `NOTIFY` on
   commit; idle workers block on the notification instead of polling. Pickup
   can improve without shortening the polling interval; measure the actual
   latency before committing to a target. Keep fallback polling for missed
   notifications and scheduled deadlines. On startup and reconnect, commit
   `LISTEN`, inspect the queue, then wait: PostgreSQL documents a registration
   race ([LISTEN](https://www.postgresql.org/docs/current/sql-listen.html)).
   Notifications are wakeup hints, never the durable delivery record.
2. **The claim query replaces the transport.** With `SELECT ... FOR UPDATE
   SKIP LOCKED`, the worker claims work directly - no pusher, no pack
   encoding, no broker hop.

This inverts the broker question. The durable truth is always the database
row; a message transport is only a wakeup hint, and losing a hint costs one
poll interval, not a task. Under that model Redis becomes an optional
accelerator (publish the task id as a wakeup), never a store - and the
reliable-Redis problem (Streams, consumer groups, pending-entry reclaim)
does not need to be solved at all. For Qraft's workloads - AI jobs that run
for seconds to minutes - even the current sub-second poll is rarely the
bottleneck; the win from `LISTEN`/`NOTIFY` is as much the removal of idle
poll load as the latency itself.

## Sequencing and decision gates

Phase 2 shipped in 1.3.0 with the marker fallback as the compatibility
bridge. Phase 3 is a rewrite of the
execution core. Do not start it until: Phase 2 has been stable in a release,
real users report friction the wrapper cannot fix, and the Django-Q2
compatibility story (migration path for existing `Q_CLUSTER` users) is
written. If those gates never trigger, Phase 3 does not happen — that is an
acceptable outcome.
