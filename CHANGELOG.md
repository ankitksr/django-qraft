# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- **Dead letter queue** (`qraft/dlq.py`): `dead_letters()` finds `FAILED`/`EXHAUSTED`
  tasks; `requeue()` re-enqueues one as the next attempt on the same `QraftTask`,
  preserving history and idempotency key. Admin gains a "Requeue selected dead tasks"
  bulk action
- **`django.tasks` deferred execution and `TaskContext`** (`qraft/backend.py`):
  `supports_defer` schedules `run_after` tasks through a marker-carrying Django-Q2
  `Schedule`, the same linkage used for retries. `takes_context=True` tasks get a real
  `TaskContext` injected worker-side via `run_task_with_context`, without disturbing the
  `QraftTask` record of the real target function/args/kwargs

### Planned
- Coroutine tasks on the `django.tasks` backend
- Priority routing for scheduled retries
- Enhanced monitoring and metrics
- Nested workflow support

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

[Unreleased]: https://github.com/ankitksr/django-qraft/compare/v1.2.0...HEAD
[1.2.0]: https://github.com/ankitksr/django-qraft/compare/v1.1.1...v1.2.0
[1.1.1]: https://github.com/ankitksr/django-qraft/compare/v1.1.0...v1.1.1
[1.1.0]: https://github.com/ankitksr/django-qraft/compare/v1.0.0...v1.1.0
[1.0.0]: https://github.com/ankitksr/django-qraft/releases/tag/v1.0.0
[0.1.0]: https://github.com/ankitksr/django-qraft/releases/tag/v0.1.0
