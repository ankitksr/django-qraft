# Asyncio worker

`async def` tasks already run today: `qraft.runner.call_target` runs one coroutine to
completion per worker slot with `asyncio.run()`. This document is the parked design for
the bigger thing — a worker that keeps many coroutines in flight in one process. It is
gated on demonstrated demand; see "Deliberately not planned" in
[the roadmap](../roadmap.md).

## What it would add

Threading already covers blocking I/O, so an asyncio worker earns its place only where
native async libraries are the workload: hundreds of concurrent provider calls in one
process, where a thread per call is the wrong shape. It would be a third worker mode
beside the two Qraft has, selected per cluster:

```python
QRAFT_CLUSTER["worker_mode"] = "standard" | "threaded" | "asyncio"
```

`ALT_CLUSTERS` then routes work by shape — coroutine tasks to the asyncio cluster,
blocking sync tasks to the threaded one, CPU-bound tasks to a standard cluster. Nothing
about the existing modes changes, and `threads>1` keeps implying the threaded worker
unless `worker_mode` says otherwise.

The worker should accept coroutine functions only. Accepting sync callables and running
them through `asyncio.to_thread()` would smooth migration, but it hides blocking code
inside a loop that cannot afford it and rebuilds a thread pool inside the async worker.
Route sync work to the threaded cluster instead.

## The shape of the process

Each worker process runs one event loop, a bridge from Django-Q2's `task_queue`, a
concurrency limiter, and a publisher back to `result_queue`. The two multiprocessing
queues are the constraint that decides the design: `get()` and `put()` block and pickle,
so neither may be called on the loop thread.

One dedicated thread should block on `task_queue.get()` and hand each task to the loop
with `loop.call_soon_threadsafe()`. Publishing takes the same treatment in reverse — a
publisher thread, or `asyncio.to_thread(result_queue.put, task)`. A result that cannot be
pickled fails loudly rather than reaching the queue. Backpressure is an
`asyncio.Semaphore(max_inflight)`, mirroring `threaded_worker`, and at capacity the
intake thread stops reading rather than buffering.

Execution mirrors `threaded_worker`: resolve the dotted path, send `pre_execute`, await
the coroutine, capture the exception the same way, populate `result`, `success` and
`stopped`, publish.

## Where the semantics get thin

Per-task timeouts would use `asyncio.wait_for()`, and that is a weaker promise than the
sentinel's. Cancellation is cooperative: a coroutine blocked in sync code or a native
extension ignores it. The sentinel's process-level timer stays the real timeout, and the
loop's is a courtesy on top of it — the timer states (`-1` idle, `-2` recycle) must keep
meaning what they mean today.

Django's ORM is the other thin edge. A sync ORM call inside a coroutine raises
`SynchronousOnlyOperation` under a running loop, and any blocking call freezes every other
coroutine in the process. Connection hygiene has to happen in whichever thread actually
runs the code, which hybrid mode would make ambiguous — a second reason to leave it out.

## Wiring and observability

`QraftSentinel.spawn_worker()` picks the target by mode, and `spawn_process()` must
recognise `asyncio_worker` the way it recognises `worker` and `threaded_worker`, so
daemonisation and timer plumbing stay identical. `qraftcluster` should print the mode and
its concurrency knobs at startup.

Worth counting: in-flight coroutines, execution duration, timeouts and cancellations, and
intake queue depth. Worth logging: the mode at startup, and every timeout with a
searchable message.

## What would have to be proved

Unit tests for a coroutine's success and its exception formatting, a timeout marking the
task failed after attempting cancellation, the STOP pill draining cleanly, `max_inflight`
holding, and a non-picklable result failing predictably. A demo-app run enqueuing async
tasks through `qraft.tasks.async_task`, proving hooks and the Django-Q2 `Task` row still
work. Then the number that decides the whole thing: many concurrent async HTTP calls
against the threaded worker at the same concurrency, with the multiprocessing-queue
bridging overhead visible in the result.

## Milestones

1. **Asyncio worker MVP** — strict async-only, timeouts, backpressure, graceful shutdown.
2. **Polish** — metrics and logging, shutdown semantics, integration coverage.
