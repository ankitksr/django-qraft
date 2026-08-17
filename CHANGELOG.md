# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Planned
- Coroutine tasks on the `django.tasks` backend
- Nested workflow support

## [1.3.0] - 2026-08-18

Phase 2 of the Django-Q2 absorption plan
([q2-absorption.md](docs/future/q2-absorption.md)): Qraft owns scheduling.

### Added
- **Qraft-owned scheduling** (`qraft/scheduler.py`): a delayed attempt is a `SCHEDULED`
  `QraftTaskAttempt` row — created up front with its final attempt number, target
  cluster, and `not_before` due time — not a Django-Q2 `Schedule`. A dispatcher loop
  (daemon thread beside the monitor) claims due rows by compare-and-swap
  (`SELECT ... FOR UPDATE SKIP LOCKED` where the database offers it) and hands them to
  the broker, stamping the q2 task id at enqueue. Delay is exact instead of quantized
  to Django-Q2's 30-second scheduler cycle, and priority lanes survive the delay. All
  three delay paths use it: retries (`RetryPolicy.schedule_retry()`), DLQ requeues
  (`dlq.requeue()`), and deferred `django.tasks` (`run_after`). The marker fallback in
  the hook handler stays for one release as the bridge for pre-1.3 schedules still in
  flight. New settings `dispatch_interval`, `dispatch_batch`
- **Bundled monitoring dashboard** (`qraft.dashboard`): staff-only Django app with
  task and workflow views, approve/reject/cancel/requeue actions, and JSON state and
  latency-percentile metrics endpoints. Add `qraft.dashboard` to `INSTALLED_APPS` and
  mount `qraft.dashboard.urls` — see [docs/dashboard.md](docs/dashboard.md)
- **Retention sweep** (`qraft/retention.py`): bounded, batched pruning of settled
  `QraftTask`/`QraftTaskAttempt`/`HookDispatch`/workflow rows by age
  (`retention_days`), count (`retention_max_tasks`), or the stricter of the two. Live
  rows never go; a workflow is pruned as a unit. An explicit `Q_CLUSTER["save_limit"]`
  is inherited as the count bound unless overridden
  (`retention_inherited_from_save_limit`). Settings `retention_interval`,
  `retention_batch_size`
- **Worker identity on the attempt row**: the lease stamps worker pid and thread name
  at `pre_execute`, so a stuck or dead attempt names its worker
- **`cluster` is a real `async_task()` parameter**: routes the task to a named
  `ALT_CLUSTERS` pool, replacing the `q_options`-only path. Workflow members validate
  their cluster at `append()`. Retries and DLQ requeues inherit the owning cluster
  instead of landing on whichever cluster dispatches them

### Fixed
- **Hook-handler retry-vs-completion race**: the retry decision commits in the same
  transaction as the completion, and a result arriving for an attempt already resolved
  (reaped, superseded) is dropped instead of double-dispatching
- **Reaper replays saved completions the hook handler missed**: a completion Django-Q2
  saved but never delivered (monitor died mid-dispatch) is resolved from the saved
  result instead of being retried as a false orphan
- **Chain advance queues the next step under the row lock**, and parallel workflow
  fan-out enqueues members atomically, closing double-queue windows under duplicate
  hook delivery
- **Workflow cancel is a guarded update**: cancel only lands from a cancellable state,
  and a committed completion racing the cancel wins instead of being overwritten

### Changed
- **`async_task()` validates harder at enqueue**: callables must be importable (bound
  methods and `functools.partial` rejected), reserved `q_options` keys are rejected,
  django-q option names hiding in function kwargs are rejected, and `save=True` is
  forced so the hook handler always sees a result row
- **Threaded workers enforce per-task deadlines**: a task overrunning its timeout gets
  a grace period, then the worker process exits forcibly so the sentinel can recycle
  it — a stuck thread no longer wedges the pool silently
- **Grey-area semantics pinned**: `max_attempts` counts total executions (documented
  on the field), priority lanes are strict (no cross-lane stealing), and workflow
  cancel scope covers members not yet enqueued
- Dropped the dead scheduler notify seam and the redundant `q2_task_id` index

### Database
- Migration `0006_scheduler_owned`: `QraftTaskAttempt.state`, `not_before`, `cluster`,
  `dispatch_func`, `dispatch_args`; nullable with DB-level defaults for a safe live
  rollout
- Migration `0007_worker_identity`: `QraftTaskAttempt.worker_pid`, `worker_thread`
- Migration `0008_drop_redundant_db_index`

### Demo
- Demo rebuilt as a self-verifying scenario suite: 32 scenarios in 6 groups
  (`core`, `workflows`, `durability`, `ai`, `django-tasks`, `bench`), each proving its
  claims through recorded checks. `manage.py demo {all|run|list|serve}`; `serve` boots
  a scenario-runner dashboard with soak load and per-cluster controls

## [1.2.1] - 2026-08-05

### Added
- **Execution lease with heartbeat** (`qraft/lease.py`): a worker stamps `date_started`
  and `heartbeat_at` on the attempt at `pre_execute` and refreshes `heartbeat_at` from a
  daemon thread every `heartbeat_interval` seconds (new setting, 30s) until the task ends.
  New `QraftTaskAttempt.date_started` / `heartbeat_at` fields (migration
  `0005_execution_lease`)

### Fixed
- **Reaper no longer reaps live long-running tasks.** The old predicate treated "no
  Django-Q2 `Task` row" as worker death, but Django-Q2 writes that row only at completion,
  so any task running past `reap_stale_after` was killed and requeued. `reap_orphans()`
  now reaps on a stale lease heartbeat (older than `max(3 * heartbeat_interval, 90s)`), or
  on a never-started attempt older than `reap_stale_after` whose pack is no longer in the
  ORM broker queue. `reap_stale_after` stays as the never-started fallback knob

- **Dead letter queue** (`qraft/dlq.py`): `dead_letters()` finds `FAILED`/`EXHAUSTED`
  tasks; `requeue()` re-enqueues one as the next attempt on the same `QraftTask`,
  preserving history and idempotency key. Admin gains a "Requeue selected dead tasks"
  bulk action
- **`django.tasks` deferred execution and `TaskContext`** (`qraft/backend.py`):
  `supports_defer` schedules `run_after` tasks through a marker-carrying Django-Q2
  `Schedule`, the same linkage used for retries. `takes_context=True` tasks get a real
  `TaskContext` injected worker-side via `run_task_with_context`, without disturbing the
  `QraftTask` record of the real target function/args/kwargs
- **Status-aware idempotency keys** (`qraft/tasks.py`): `idempotency_retry_dead=True`
  lets a `FAILED`/`EXHAUSTED` task's key be reclaimed by a new enqueue instead of
  permanently blocking it. Default `False` keeps the existing permanent-dedupe behavior

### Fixed
- **Parallel workflow cancel race**: `ParallelDispatcher` re-checks workflow status under
  the row lock inside `_atomic_increment()`, so a cancel racing the final task completion
  is no longer overwritten back to `SUCCEEDED`/`FAILED`
- **Chain double-queue on duplicate hook delivery**: `ChainDispatcher` now advances
  `current_step_index` only while it still matches the completing step's index, checked
  under a row lock, so a duplicate `qraft_hook_handler` delivery for the same step no
  longer queues the next step twice
- **Retries/DLQ requeue of `@task`-decorated functions**: `RetryPolicy.schedule_retry()`
  and `dlq.requeue()` now schedule the new `qraft.runner.run_task` instead of the stored
  dotted path directly, so a retried or requeued `django.tasks`-decorated function is
  unwrapped and called correctly instead of resolving to the non-callable `@task` wrapper.
  Requeued `takes_context=True` tasks still don't get `TaskContext` re-injected - out of
  scope, see `docs/django-tasks-backend.md`
- **Rate-limit delay cap now applied after jitter**: a `retry_after` hint near
  `rate_limit_max_delay` could slip past the cap once jitter was added (e.g. 300s cap ->
  330s). `parse_retry_after()` also floors sub-second hints to 1.0s, so a provider hint of
  `0` can no longer produce a zero/negative retry delay

### Changed
- **`async_task()` rejects `sync=True`, `save=False`, and `cached=...`**: completion
  tracking depends on the hook handler seeing a saved Django-Q2 result after the
  `QraftTaskAttempt` row exists, and these modes all break that: `sync=True` finishes (and
  fires the hook) before the row is committed, and `save=False`/`cached=...` never produce
  the result row at all. All three now raise `ValueError` instead of silently corrupting
  tracking

## [1.2.0] - 2026-08-05

Positioning shift: durable background jobs and workflows for Django — Postgres only, no
extra infra, built for AI workloads.

### Added

#### Durability
- **Orphan reaper** (`qraft/reaper.py`): `reap_orphans()` finds attempts whose worker died
  mid-run (task still `RUNNING`, attempt unresolved, no Django-Q2 `Task` row) and resolves
  them through the normal retry path, marking them `exception_class="OrphanedTask"`. Runs
  as a daemon thread beside the monitor process; settings `reap_interval` (60s) and
  `reap_stale_after` (3600s)
- **Broker receipt warning**: `QraftCluster.start()` warns when the configured broker
  overrides neither `acknowledge()` nor `fail()` — in-flight tasks are lost on a worker
  crash

#### AI-workload primitives
- **Rate-limit-aware retries**: `RATE_LIMIT_EXCEPTIONS` (RateLimited, RateLimitError,
  TooManyRequests, ThrottlingException, ResourceExhausted, OverloadedError, …) retry even
  under a restrictive `retry_exceptions` allowlist, back off exponentially regardless of
  the configured strategy, and cap at `rate_limit_max_delay` (300s). `parse_retry_after()`
  honors provider `Retry-After` hints as a floor
- **Cross-worker throttle** (`qraft/throttle.py`): `throttled()` decorator and `acquire()`
  over a DB token bucket (`RateBucket`), so N workers share one provider rate limit.
  Raises `RateLimited` when empty, which reschedules with rate-limit backoff
- **Idempotency keys**: unique `idempotency_key` on `QraftTask`; a repeat `async_task()`
  returns the original call's Q2 task id instead of enqueueing again. Permanent dedupe —
  a `FAILED`/`EXHAUSTED` task still blocks re-enqueue
- **Usage and progress** (`qraft/context.py`): `record_usage()` accumulates numeric fields
  into `QraftTaskAttempt.usage`, `report_progress()` merges into `QraftTask.progress`.
  Both find the executing attempt through a `pre_execute` receiver, no argument threading.
  `aggregate_usage()` and `aggregate_workflow_usage()` roll up across attempts and
  workflows
- **Priority lanes** (`qraft/brokers.py`): `QraftOrmBroker` drains `{list_key}--high`,
  `{list_key}`, `{list_key}--low` in order on one ORM queue. Enqueue with
  `qraft_options={'priority': 'high'|'low'}`; consume by setting
  `Q_CLUSTER["broker_class"] = "qraft.brokers.QraftOrmBroker"`

#### Workflows
- **Approval-gated chain steps**: `chain.append(..., requires_approval=True)` parks the
  chain in `WAITING_APPROVAL` at zero compute before that step. `QraftChain.approve()` and
  `reject(reason)` resume or cancel it under a row lock with transition validation
- **`result()` returns on parked chains**: `WAITING_APPROVAL` is a polling stop state, so
  `result(wait=...)` hands back completed steps instead of blocking to the timeout

#### django.tasks (DEP 14)
- **`qraft.backend.QraftTaskBackend`**: engines Django 6.0's official Tasks API on Qraft's
  pipeline. `supports_get_result` and `supports_priority`; deferred, coroutine, and
  context-taking tasks are rejected by `validate_task()`. Status mapping
  PENDING/RUNNING/SUCCEEDED/FAILED-EXHAUSTED → READY/RUNNING/SUCCESSFUL/FAILED. Import is
  guarded, so the package stays import-safe on Django < 6.0

### Changed
- `qraft.models` now re-exports `RateBucket` and `TaskPriority` alongside the other models
- `RetryPolicy` accepts `rate_limit_exceptions` and `rate_limit_max_delay`;
  `calculate_delay()`/`next_eta()`/`schedule_retry()` take `retry_after` and
  `is_rate_limit`
- `qraft_hook_handler()` passes the raw task result to `handle_task_retry()` so
  `Retry-After` hints can be parsed
- `QraftSentinel.spawn_monitor()` spawns the monitor with the reaper thread attached

### Database
- Migration `0004_ai_workload_fields`: `QraftTask.idempotency_key` (unique),
  `QraftTask.priority`, `QraftTask.progress`, `QraftTaskAttempt.usage`,
  `QraftChainStep.requires_approval`, `WAITING_APPROVAL` workflow status choice, and the
  new `RateBucket` table

### Demo
- New scenarios: `demo approval`, `demo ratelimit`, `demo usage`, `demo idempotent`,
  `demo reaper`
- New tasks: `throttled_task`, `llm_task`, `charge_task`, `review_task`, `publish_task`

### Documentation
- New `docs/ai-workloads.md` and `docs/django-tasks-backend.md`
- `docs/workflows.md`: approval steps and the updated status lifecycle
- `docs/roadmap.md`: shipped items collected at the top
- README: new feature sections, repositioned tagline, Postgres requirement

### Testing
- New test modules: `test_approval.py`, `test_backend.py`, `test_brokers.py`,
  `test_context.py`, `test_reaper.py`, `test_throttle.py`. Suite is 275 passing;
  `test_backend.py` skips unless Django 6.0+ is installed

## [1.1.1] - 2026-02-17

### Fixed

#### Concurrency Bugs
- **Fix `attempt_count` race in retry logic**: Use concrete `attempt.attempt_number` instead of COUNT query to prevent race conditions in concurrent retry scheduling
- **Add `select_for_update()` in hook handler**: Prevent concurrent status updates on the same QraftTask
- **Fix TOCTOU race in `_dispatch_workflow_hook()`**: Replace filter-then-create pattern with atomic `get_or_create()` for workflow hook dispatch
- **Fix HookDispatch placeholder cleanup**: Delete placeholder dispatch record on `q2_async_task()` failure instead of silently swallowing the error
- **Wrap `async_task()` in transaction**: QraftTask creation + Q2 queueing + QraftTaskAttempt creation are now atomic
- **Add ParallelDispatcher idempotency**: New `counted` field on QraftTaskAttempt prevents double-counting in parallel workflows

#### Configuration & Error Handling
- **LRU-cached `get_conf()`**: Settings instances are now cached per cluster name, avoiding re-creation on every call
- **Use `get_conf()` in RetryPolicy**: Retry defaults now respect ALT_CLUSTERS at runtime instead of using module-level singleton
- **Unified retry marker constants**: Centralized marker format strings in `qraft/retry.py` for consistency
- **UUID validation in marker parsing**: `_parse_qraft_marker()` now validates UUID format before returning

### Added

#### Workflow Improvements
- **Cancellation support**: All workflow types (Chain, Iter, Batch) now support `cancel()` method with `CANCELLED` status
- **Progress hooks**: Parallel workflows (Iter, Batch) support `progress_hook` called on each task completion
- **Rich result objects**: `WorkflowResult` and `TaskResult` dataclasses with `succeeded`, `failed_results`, `errors()`, and backward-compatible iteration
- **BaseWorkflow class**: Shared base class for Chain/Iter/Batch with common patterns (polling, cancellation, result building)
- **Hook path validation**: Hook dotted paths are validated at workflow creation time using `import_string()`
- **Exponential backoff polling**: `result(wait=...)` now uses exponential backoff (50ms → 2s) instead of fixed 100ms sleep
- **`on_cancelled` hook field**: Workflows can specify a hook to call on cancellation
- **`QraftBatch.append()`**: Consistent API across all workflow types; `add()` is deprecated

#### Admin & Models
- **Workflow admin registration**: `QraftChainModel`, `QraftIterModel`, `QraftBatchModel`, and `WorkflowHookDispatch` are now registered in Django admin with colored status, counters, and hook indicators
- **Fix N+1 in QraftTaskAdmin**: `get_queryset()` now annotates `attempt_count` to avoid per-row queries
- **State machine validation**: `WorkflowStatusMixin.transition_to()` validates status transitions with clear error messages
- **`counted` field on QraftTaskAttempt**: Supports dispatcher idempotency

#### Packaging & CI
- **Fixed `requires-python`**: Changed from `>=3.9` to `>=3.10` (match syntax used in codebase)
- **Fixed CI workflow**: Updated Python matrix, fixed `uv sync --group test`, added Codecov token, added `--cov-fail-under=80`
- **Added packaging metadata**: classifiers, project URLs, keywords
- **Version metadata**: package version is exposed via `pyproject.toml` / `importlib.metadata.version("django-qraft")`
- **Fixed README/CHANGELOG placeholders**: Replaced `yourusername` with actual GitHub username

### Changed
- `QraftTask.qraft_iter` and `qraft_batch` FK cascade changed from `SET_NULL` to `CASCADE`
- `QraftBatch.add()` deprecated in favor of `append()` for API consistency
- `RetryPolicy.__init__()` now uses `get_conf()` instead of module-level `conf`
- `schedule_retry()` now takes explicit `current_attempt` parameter
- Ruff target version changed from `py312` to `py310`

### Database
- `QraftTaskAttempt.counted`, `WorkflowHookMixin.on_cancelled`, `WorkflowHookMixin.progress_hook`, `CANCELLED` status choice, and FK cascade changes folded into migration `0003_workflow_primitives` (squashed pre-release)

## [1.1.0] - 2026-01-24

### Added

#### Workflow Primitives
- **QraftChain**: Sequential task execution with resume capability
  - Each step can have its own retry policy
  - Chain-level success/failure hooks
  - Resume from failed step after fixing issues
  - Automatic continuation after each step succeeds
  - API: `QraftChain.append()`, `run()`, `resume()`, `result()`, `current()`

- **QraftIter**: Parallel execution of same function with different inputs
  - Atomic counter tracking for completion
  - Default retry policy applies to all items
  - Workflow-level hooks fire when all tasks complete
  - API: `QraftIter.append()`, `run()`, `result()`, `length()`

- **QraftBatch**: Parallel execution of different functions (fork-join)
  - Each task can have individual retry policy
  - Heterogeneous task support
  - Workflow-level hooks fire when all tasks complete
  - API: `QraftBatch.add()`, `run()`, `result()`

#### New Models
- `QraftChainModel`: Chain workflow state tracking
- `QraftChainStep`: Individual chain step with OneToOne link to QraftTask
- `QraftIterModel`: Iter workflow with atomic counters
- `QraftBatchModel`: Batch workflow with atomic counters
- `WorkflowHookDispatch`: Idempotent workflow hook tracking

#### Module Reorganization
- Refactored `qraft/models.py` into modular structure:
  - `qraft/models/tasks.py`: QraftTask and QraftTaskAttempt
  - `qraft/models/workflows.py`: Workflow models
  - `qraft/models/hooks.py`: Hook dispatch models
  - `qraft/models/mixins.py`: Shared enums and mixins (WorkflowStatus, WorkflowHookMixin)

#### Workflow Infrastructure
- Workflow detection in hook handler with automatic routing
- `ChainDispatcher` for sequential workflow continuation
- `ParallelDispatcher` for atomic completion tracking with F() expressions
- `_create_workflow_task()` internal helper for workflow task creation
- Workflow-level dual-phase hooks (separate from task-level hooks)

### Changed
- Hook handler now detects workflow membership and routes to appropriate dispatcher
- Workflow tasks skip task-level hooks (workflow-level hooks only)
- Models module is now a package with organized submodules

### Database
- Migration `0003_workflow_primitives`: Adds all workflow models and QraftTask FKs
  - New tables: `qraft_qraftchainmodel`, `qraft_qraftchainstep`, `qraft_qraftitermodel`, `qraft_qraftbatchmodel`, `qraft_workflowhookdispatch`
  - Added fields: `qraft_task.qraft_iter`, `qraft_task.qraft_batch`

### Demo Updates
- Added `demo chain` command for sequential workflow demonstration
- Added `demo iter` command for parallel same-function demonstration
- Added `demo batch` command for parallel multi-function demonstration

### Testing
- Added comprehensive unit tests for Chain, Iter, and Batch primitives
- Added integration tests marked with `@pytest.mark.integration`
- Test files: `test_chain.py`, `test_iter.py`, `test_batch.py`, `test_workflow_integration.py`

### Documentation
- Updated README.md with workflow primitives documentation and examples
- Updated CLAUDE.md with workflow architecture details
- Added workflow primitives to feature list and use cases

## [1.0.0] - 2025-01-24

### Added
- Dual-phase hooks with separate success and failure handlers
- Rich retry policies with exponential, linear, and fixed backoff strategies
- Jitter support for retry delays to prevent thundering herd
- Optional multithreaded workers for I/O-bound workloads
- Async hook execution to prevent monitor bottlenecks
- `QraftTask` model for logical task tracking
- `QraftTaskAttempt` model for execution history
- `HookDispatch` model for idempotent hook execution
- `async_task()` function with Qraft-specific options
- `qraftcluster` management command
- Pydantic-based settings with Django integration
- ALT_CLUSTERS support for mixed workload pools
- Comprehensive unit test suite (108 tests, >90% coverage)
- Demo application with hooks, retry, and performance benchmarks

### Features

#### Dual-Phase Hooks
- Separate `success_hook` and `failure_hook` configuration
- Custom arguments and kwargs for each hook
- Async execution via worker pool (default) or sync in monitor
- Idempotent dispatch with `HookDispatch` tracking

#### Retry Policies
- Maximum attempts with exhaustion handling
- Backoff strategies: exponential (default), linear, fixed
- Jitter randomization (0-30% by default)
- Exception filtering with `retry_exceptions` and `skip_exceptions`
- Automatic scheduling via Django-Q2 Schedule

#### Multithreaded Workers
- Configurable threads per worker process
- Semaphore-based backpressure control (`max_inflight`)
- Graceful shutdown with configurable grace period
- Database connection management per thread
- Process-level timeout enforcement

#### Configuration
- `QRAFT_CLUSTER` settings (fallback to `Q_CLUSTER`)
- Pydantic validation with sensible defaults
- Environment-based cluster selection (`Q_CLUSTER_NAME`)
- Backward compatibility with Django-Q2

### Compatibility
- Python 3.10+
- Django 4.2+
- Django-Q2 1.8+
- Drop-in enhancement of Django-Q2

## [0.1.0] - 2025-01-10 (Initial Development)

### Added
- Initial project structure
- Basic task queue functionality
- Integration with Django-Q2

---

## Release Notes Guidelines

### Version Format
- **Major (X.0.0)**: Breaking changes
- **Minor (0.X.0)**: New features, backward compatible
- **Patch (0.0.X)**: Bug fixes, backward compatible

### Categories
- **Added**: New features
- **Changed**: Changes to existing functionality
- **Deprecated**: Soon-to-be removed features
- **Removed**: Removed features
- **Fixed**: Bug fixes
- **Security**: Security fixes

[Unreleased]: https://github.com/ankitksr/django-qraft/compare/v1.3.0...HEAD
[1.3.0]: https://github.com/ankitksr/django-qraft/compare/v1.2.1...v1.3.0
[1.2.1]: https://github.com/ankitksr/django-qraft/compare/v1.2.0...v1.2.1
[1.2.0]: https://github.com/ankitksr/django-qraft/compare/v1.1.1...v1.2.0
[1.1.1]: https://github.com/ankitksr/django-qraft/compare/v1.1.0...v1.1.1
[1.1.0]: https://github.com/ankitksr/django-qraft/compare/v1.0.0...v1.1.0
[1.0.0]: https://github.com/ankitksr/django-qraft/releases/tag/v1.0.0
[0.1.0]: https://github.com/ankitksr/django-qraft/releases/tag/v0.1.0
