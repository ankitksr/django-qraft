# Asyncio worker

`async def` tasks already run today — `qraft.runner.call_target` runs one coroutine to
completion per worker slot with `asyncio.run()`. This document is the parked design for the
bigger thing: a worker that keeps many coroutines in flight per process. Gated on
demonstrated demand; see `docs/roadmap.md`, "Deliberately not planned".

## Goals

- **First-class support for `async def` tasks** in Django-Qraft, aligned with Django 5.x async direction.
- Preserve Qraft's core promise: **drop-in compatibility with Django-Q2** plus opt-in enhancements.
- Keep existing worker modes:
  - **Standard (Django-Q2 worker)**: best for CPU-bound / strict per-task timeout model (signals)
  - **Threaded worker (Qraft)**: best for blocking I/O using sync libraries
  - **New: Asyncio worker (Qraft)**: best for native async I/O

## Worker modes and routing

### Design

- Add an explicit worker mode setting so behavior is intentional and debuggable:
  - `QRAFT_CLUSTER["worker_mode"] = "standard" | "threaded" | "asyncio"`
- Continue to support **multi-cluster pools** via `ALT_CLUSTERS`:
  - `default`: standard
  - `io-threaded`: threaded
  - `io-async`: asyncio
- Document clear guidance:
  - `async def` coroutine functions → route to the asyncio cluster.
  - Sync-but-blocking tasks → route to threaded cluster.
  - CPU-bound tasks → route to standard cluster (or dedicated CPU cluster).

### Task compatibility policy

- **Option A (strict, recommended for MVP)**: asyncio worker only accepts coroutine functions (`inspect.iscoroutinefunction(f)`).
  - Pros: predictable concurrency; avoids accidental loop blocking.
  - Cons: sync tasks must be routed elsewhere.
- **Option B (hybrid, optional later)**: accept sync callables; run them via `asyncio.to_thread()`.
  - Pros: smoother migration.
  - Cons: hides blocking code; adds threadpool-in-worker complexity; can erode async benefits.

## Architecture: asyncio worker process

### High-level structure

Each worker process runs:
- One asyncio event loop.
- A task intake bridge from Django-Q2's `task_queue` (multiprocessing queue).
- A concurrency limiter (`max_inflight`).
- A result publisher to `result_queue` (multiprocessing queue).

Key constraints:
- `multiprocessing.Queue.get/put` are **blocking** and **pickle-based**.
- Interaction with mp queues must not freeze the event loop thread.
- Results must be **picklable**.

### Task intake (mp queue to asyncio loop)

Implementation options:
- **Preferred**: one dedicated thread blocks on `task_queue.get()` and forwards tasks into the loop via
  `loop.call_soon_threadsafe(asyncio_queue.put_nowait, task)`.
- Alternative: `await asyncio.to_thread(task_queue.get)` in a loop (simpler but adds scheduling overhead).

Stop signal handling:
- Detect `"STOP"` poison pill and initiate graceful shutdown.

### Result publishing (async to mp result_queue)

- `result_queue.put(task)` should not block the event loop:
  - Use a dedicated publisher thread, or `await asyncio.to_thread(result_queue.put, task)`.
- Enforce "picklable results":
  - If non-picklable, convert to a safe representation (e.g., `repr()` / string) or fail-fast with clear error.

### Concurrency and backpressure

- Use `asyncio.Semaphore(max_inflight)` (mirrors `threaded_worker`'s inflight control).
- When at capacity:
  - Prefer pausing intake (do not unboundedly buffer), to avoid memory growth.

## Execution semantics

### Task execution flow

For each incoming task dict:
- Resolve callable (string path via `pydoc.locate`, like `threaded_worker`).
- Emit Django-Q signal `pre_execute` (parity with Q2/Qraft).
- Execute:
  - Strict mode: `await f(*args, **kwargs)`.
  - Hybrid mode: if sync callable, `await asyncio.to_thread(f, *args, **kwargs)`.
- Capture exceptions with consistent formatting (parity with Q2/Qraft worker patterns).
- Populate:
  - `task["result"]`, `task["success"]`, `task["stopped"]`
- Publish result to `result_queue`.

### Timeouts and cancellation

- Implement per-task timeout with `asyncio.wait_for()`.
- On timeout:
  - Mark the task failed with timeout metadata.
  - Attempt cancellation (`task.cancel()`); note cancellation is cooperative.
- Sentinel timer integration:
  - Maintain consistent timer states (idle `-1`, recycle `-2`, etc.).
  - Update timer to reflect "any coroutine in-flight / busy".

Important note:
- Asyncio cancellation/timeout is "soft". A task that blocks in sync code or native extensions may ignore it.

## Django integration (Django 5.x async)

### ORM and DB connections

- Many deployments still use sync DB drivers / ORM access patterns.
- Guidance for users:
  - Avoid sync ORM calls inside async tasks unless properly bridged.
  - Prefer `sync_to_async`/`to_thread` for sync-only APIs, or route the task to sync workers.
- Connection hygiene:
  - Ensure `close_old_django_connections()` (or equivalent) is applied appropriately before/after execution.
  - If hybrid mode runs sync code in threads, connection close/open should happen in that same thread context.

### Signals and thread affinity

- Decide whether to emit `pre_execute` (and other signals) on the event loop thread only.
- Document thread-safety expectations for signal handlers.

## Config and API changes

### Settings

Extend `QraftSettings` to include:
- `worker_mode`: `"standard"` (default), `"threaded"`, `"asyncio"`.
- `max_inflight`: reuse existing setting; allow `asyncio_max_inflight` override if needed.
- Shutdown settings:
  - `grace_period` reuse, plus optional `asyncio_shutdown_timeout`.
- Policy toggles:
  - `asyncio_strict: bool` (or inverse `asyncio_hybrid: bool`).

Backward compatibility:
- Preserve current "threads>1 implies threaded worker" behavior unless `worker_mode` explicitly overrides it.
- Keep `ALT_CLUSTERS` semantics identical.

### Management command UX

- Update `qraftcluster` output to print worker mode and relevant concurrency knobs.

## Cluster and sentinel wiring

### Worker spawning

In `QraftSentinel.spawn_worker()`:
- If mode is threaded → spawn `threaded_worker`.
- If mode is asyncio → spawn new `asyncio_worker`.
- Else → spawn Django-Q2 `worker`.

Ensure `spawn_process()` recognizes the new worker target the same way as `worker` and `threaded_worker`
so daemonization and timer plumbing remains consistent.

## Observability and debuggability

### Logging

- Log worker mode at startup.
- For each task, log:
  - task name, function repr, group (if any), and execution path (async vs hybrid-to-thread).
- On timeouts/cancellation, log with clear, searchable messages.

### Metrics to add

- In-flight count.
- Mean/percentile execution time.
- Timeout/cancellation counts.
- Intake rate / queue depth (if feasible).

## Compatibility and safety gotchas

Document these plainly:
- **Pickling**: results shipped via `multiprocessing.Queue` must be picklable (coroutines, sockets, DB conns are not).
- **Blocking calls inside async**: any sync blocking I/O inside an async task freezes the loop and destroys concurrency.
- **Cancellation**: cooperative; timeouts may not stop stubborn tasks.
- **Timeout semantics**: ensure consistent story with sentinel timeouts and process recycling.
- **Django ORM**: easy to accidentally block; provide guidance and/or strict-mode enforcement.
- **Shutdown**: coordinate draining in-flight coroutines, intake thread, and result publishing without deadlocks.

## Testing plan

### Unit tests

- Coroutine task success with picklable result.
- Coroutine task exception formatting.
- Timeout triggers, task marked failed, cancellation attempted.
- STOP pill triggers clean shutdown.
- Backpressure: `max_inflight` prevents unbounded buffering.
- Non-picklable result handling is deterministic and user-friendly.

### Integration tests (demo app)

- Start cluster in asyncio mode and enqueue async tasks via `qraft.tasks.async_task`.
- Ensure results persist into Django-Q task model and Qraft hook dispatch continues to work.

### Performance sanity checks

- Many concurrent async HTTP calls should show better throughput than threaded for high concurrency.
- Confirm mp-queue bridging overhead is acceptable.

## Suggested milestones

- **M0 (Docs + safeguards)**: detect coroutine tasks in current workers and fail fast with actionable error message.
- **M1 (Asyncio worker MVP)**: strict async-only worker, basic timeouts, backpressure, graceful shutdown.
- **M2 (Hybrid mode + ORM guidance)**: optional `to_thread` execution for sync callables; stronger docs and warnings.
- **M3 (Polish)**: richer metrics/logging, improved shutdown semantics, broader integration coverage.
