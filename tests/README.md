# Django-Qraft Tests

Comprehensive unit test suite for django-qraft using modern Python testing practices.

## Quick Start

### Install Test Dependencies

```bash
uv sync --group test
```

### Run All Tests

```bash
# Using uv
uv run pytest

# Or directly with pytest
pytest
```

## Test Structure

```
tests/
├── __init__.py                   # Package marker
├── conftest.py                   # Shared fixtures and pytest configuration
├── settings.py                   # Django settings for tests
├── urls.py                       # URLconf for the dashboard tests
├── e2e_tasks.py                  # Real task/hook functions the e2e tests execute
│
│   # Core task pipeline
├── test_models.py                # QraftTask, QraftTaskAttempt, HookDispatch
├── test_tasks.py                 # async_task(): validation, options, enqueue
├── test_retry.py                 # RetryPolicy: backoff, jitter, exception filtering
├── test_hooks.py                 # Hook handler, dual-phase dispatch, idempotency
├── test_scheduler.py             # The dispatcher that owns delayed execution
├── test_lease.py                 # Execution lease and heartbeat
├── test_reaper.py                # Orphan detection, replay, stall flagging
├── test_dlq.py                   # Dead-letter listing and requeue
├── test_retention.py             # Bounded pruning of settled rows
│
│   # Workflows
├── test_chain.py                 # QraftChain sequential workflow
├── test_iter.py                  # QraftIter parallel fan-out
├── test_batch.py                 # QraftBatch fork-join
├── test_approval.py              # Human-in-the-loop chain steps
├── test_dispatchers.py           # Chain/parallel dispatcher races and idempotency
│
│   # Runtime and integration surface
├── test_worker.py                # threaded_worker loop and thread execution
├── test_cluster.py               # QraftSentinel/QraftCluster worker selection
├── test_commands.py              # qraftcluster management command
├── test_brokers.py               # Priority lanes, per-cluster brokers, RoutingBroker
├── test_throttle.py              # Shared token bucket
├── test_context.py               # Progress and usage reporting
├── test_backend.py               # django.tasks backend (skips below Django 6.0)
├── test_conf.py                  # Settings and ALT_CLUSTERS
├── test_dashboard.py             # Bundled monitoring dashboard
│
│   # Runs and observability
├── test_runs.py                  # Binding rules, derived settlement, replay paths
├── test_signals.py               # Send sites, id payloads, send_robust isolation
├── test_metrics.py               # Emission points, label sets, gauge ownership
├── test_logging.py               # QraftContextFilter inside and outside a task
├── test_pricing.py               # Cost resolver, subset formula, coverage flags
│
│   # Whole-path tests
├── test_e2e.py                   # Real broker + worker + monitor, nothing stubbed
├── test_integration.py           # Cross-cutting flows with the enqueue seam mocked
├── test_workflow_integration.py  # Workflow flows (integration marker)
└── test_postgres_concurrency.py  # Row-lock races; skipped unless run on Postgres
```

## Running Tests

### Basic Usage

```bash
# Run all tests
uv run pytest

# Run specific test file
uv run pytest tests/test_models.py

# Run specific test class
uv run pytest tests/test_models.py::TestQraftTask

# Run specific test function
uv run pytest tests/test_models.py::TestQraftTask::test_create_minimal_task

# Run tests matching a pattern
uv run pytest -k "retry"
```

### Parallel Execution

```bash
# Run tests in parallel using all CPU cores
uv run pytest -n auto

# Run tests using 4 workers
uv run pytest -n 4
```

### Coverage Reports

```bash
# Run tests with coverage
uv run pytest --cov=qraft

# Generate HTML coverage report
uv run pytest --cov=qraft --cov-report=html

# View coverage report
open htmlcov/index.html
```

### Verbose Output

```bash
# Show detailed test output
uv run pytest -v

# Show even more detail (print statements, etc.)
uv run pytest -vv -s
```

### Debugging

```bash
# Stop on first failure
uv run pytest -x

# Drop into debugger on failure
uv run pytest --pdb

# Show local variables in tracebacks
uv run pytest -l
```

## Test Organization

### Models Tests (`test_models.py`)

Tests for the database models:
- **TestQraftTask**: Task creation, status transitions, properties
- **TestQraftTaskAttempt**: Attempt tracking, uniqueness constraints
- **TestHookDispatch**: Hook idempotency, cascade deletion

### Retry Tests (`test_retry.py`)

Tests for retry logic:
- **TestRetryPolicy**: Policy initialization, validation, backoff calculations
- Delay calculations for fixed/linear/exponential strategies
- Exception filtering (retry_exceptions, skip_exceptions)
- Retry scheduling as a SCHEDULED attempt row (see `test_scheduler.py` for the dispatcher
  that claims it)

### Tasks Tests (`test_tasks.py`)

Tests for task creation:
- **TestAsyncTask**: Task queueing, hook configuration, retry setup
- Django-Q2 parameter passing
- Legacy hook migration
- Task name preservation

### Hooks Tests (`test_hooks.py`)

Tests for hook dispatching:
- **TestExtractExceptionClass**: Exception parsing from tracebacks
- **TestParseQraftMarker**: Task name parsing for retries
- **TestQraftHookHandler**: Hook handler logic, dual lookup
- **TestHookDispatcher**: Hook dispatching, retry handling, idempotency

### Workflow Tests (`test_chain.py`, `test_iter.py`, `test_batch.py`)

Tests for workflow primitives:
- **QraftChain**: sequential execution, resume from failed step, chain-level hooks
- **QraftIter**: parallel fan-out, atomic counters, completion detection
- **QraftBatch**: fork-join, per-task retry policies, `add()` deprecation shim
- `test_workflow_integration.py` covers end-to-end flows (marked `integration`)

### Configuration Tests (`test_conf.py`)

Tests for settings management:
- **TestRetrySettings**: Retry configuration validation
- **TestQraftSettings**: Main settings, threading configuration
- **TestDjangoSettingsIntegration**: Django settings loading, ALT_CLUSTERS
- **TestGetConf**: Dynamic configuration

### Observability Tests

`test_signals.py` asserts each send site fires once with an immutable id-only payload,
that a raising receiver is isolated by `send_robust`, and that a replayed transition sends
nothing. `test_metrics.py` drives a `RecordingSink` through every emission point and
asserts no label is ever a subject, run, task or attempt id. `test_logging.py` covers the
filter's attributes inside and outside a task.

`test_runs.py` covers the run surface: the nine edge rules, settlement on a success, on a
failed unit, with a skipped stage and with a workflow unit, `cancel` and `abandon`,
`on_settled` dispatched once, the summary's contents, the overdue sweep, the late subject
bind, and the three replay paths a settlement can arrive through twice
(`replay_unrouted`, a duplicate completion delivery, a replayed workflow settlement).

`test_brokers.py` covers both jobs of that module: the priority lanes, and the per-cluster
resolver — each broker key selecting its class in django_q's own order, the cache and its
invalidation, an ALT entry overriding the base, an unknown cluster falling back to it, the
pinned ORM database alias, and `RoutingBroker` delegating without recursing. The
mixed-broker deployment is asserted end to end with two ORM-backed clusters on distinct
list keys and a Redis-configured one proved through a stub, so no test needs a live server.

`test_lease.py` covers the redelivery guard: a first delivery claims and runs, a second is
refused without calling the function or refreshing the lease, the refusal resolves through
the retry policy, and a raised `max_executions_per_attempt` admits exactly one more.

Two fixtures in `conftest.py` carry this: `recording_sink` routes every emission into a
recorder, and `signal_log` connects a receiver to every signal and yields
`{signal_name: [payloads]}`. Sends ride `transaction.on_commit`, so pair `signal_log` with
`django_capture_on_commit_callbacks(execute=True)` around the action under test.

### End-to-End Tests (`test_e2e.py`)

The only tests that stub nothing between `async_task()` and the hook. They enqueue to the
real ORM broker and run Django-Q2's own `pusher`, `worker` and `monitor` loops in-process —
the same three loops `qcluster` runs as separate processes — so a real `Task` row fires
`qraft_hook_handler` through its normal `post_save` receiver.

- Success path: task executes, lease stamps `date_started`, hook is queued then executed
- Retry path: attempt 1 fails, attempt 2 exists as a SCHEDULED row, `dispatch_due()` queues
  it, attempt 2 executes and succeeds
- Exhaustion, DLQ requeue, chain ordering, iter counting
- One case drives `qraft.worker.threaded_worker` instead of the standard worker

Task functions live in `tests/e2e_tasks.py` rather than inside the tests, because a retry
re-imports them by dotted path. Out of scope: the sentinel (process spawning, recycling,
timeout kills) cannot run inside a test process, and `dispatch_due()` is called directly
rather than polled.

These were mutation-checked when written, so "they pass" is not the only evidence they
work. Deleting the task-level `HookDispatcher.dispatch()` call in `qraft/hooks.py` fails
four of them; making `lease.stamp_start()` write a null `date_started` fails two. Re-run
those two mutations if you ever suspect the suite has gone green for the wrong reason.

## Fixtures

Common fixtures available in all tests (defined in `conftest.py`):

- `qraft_task`: A test QraftTask instance
- `qraft_task_attempt`: A test QraftTaskAttempt instance
- `mock_q2_task_success`: Mock successful Django-Q2 task
- `mock_q2_task_failure`: Mock failed Django-Q2 task
- `retry_policy`: Test RetryPolicy instance

## Testing Philosophy

### Modern Practices

1. **pytest over unittest**: We use pytest for better readability and powerful fixtures
2. **Fixtures over setUp/tearDown**: Composable dependency injection
3. **Minimal mocking**: Test real behavior; mock only external dependencies
4. **Fast tests**: In-memory SQLite database, optimized for speed
5. **Isolated tests**: Each test is independent and can run in any order

### Database Testing

All database tests use `@pytest.mark.django_db`:

```python
@pytest.mark.django_db
class TestQraftTask:
    def test_create_task(self):
        task = QraftTask.objects.create(func="test.function")
        assert task.id is not None
```

The `--reuse-db` flag (enabled by default) reuses the test database between runs for speed.

`test_e2e.py` uses `@pytest.mark.django_db(transaction=True)` instead: the worker and
monitor loops close connections between tasks, which the default transaction-wrapped mode
cannot survive.

SQLite cannot express `SELECT ... FOR UPDATE SKIP LOCKED`, so the concurrency paths in
`scheduler.py`, `reaper.py` and `dispatchers.py` take their compare-and-swap fallback
branch here. That fallback is what the suite verifies; run against Postgres before trusting
the locking behaviour of a change to those files.

### Running against Postgres

Set `QRAFT_TEST_DATABASE_URL` and the whole suite runs against Postgres instead of
in-memory SQLite. The `postgres` marker selects the cases that need real row locks — a duplicate completion
delivery racing the live handler, concurrent progress reports, two run stages settling at
once, a stage bind racing a run cancel, a stall flag racing a progress advance, two
deliveries of one attempt, a member insert racing `bind_subject`, and two workers spending
the last of a budget — they are skipped automatically on SQLite, so `uv run pytest` never fails for want of a server.

```bash
QRAFT_TEST_DATABASE_URL=postgresql://user:pass@localhost:5432/qraft_test uv run pytest
QRAFT_TEST_DATABASE_URL=postgresql://user:pass@localhost:5432/qraft_test uv run pytest -m postgres
```

The role needs `CREATEDB`: the runner creates `test_<name>`. Two paths only exist on
Postgres and are otherwise untested — the single-statement `jsonb ||` merges in
`qraft/context.py`, and every `select_for_update()` that serialises a race rather than
falling back to a compare-and-swap.

### Mocking

We mock external dependencies but test real code paths:

```python
@patch("qraft.tasks.q2_async_task")
def test_async_task(mock_q2_async):
    mock_q2_async.return_value = "task-id"
    # Test real async_task logic
    result = async_task("test.function")
    assert result == "task-id"
```

That mock is what keeps most tests fast and precise about call arguments, but it also means
they never prove the seam itself works. `test_e2e.py` is where that is proven; a change to
enqueueing, the hook handler, or the dispatcher belongs in both.

## Coverage Goals

Currently 85%. We aim for:
- **85%+ code coverage** overall (CI fails below 72%)
- **100% coverage** for critical paths (retry logic, hook dispatching)
- **Clear test names** that document behavior
- **Minimal but effective** test cases

Two files read lower than they are: `qraft/admin.py` is never imported by the test settings
(no `django.contrib.admin`) and is verified manually via the demo app, and
`qraft/backend.py` reads 0% on Django 5.x, where its tests skip.

## Continuous Integration

These tests are designed to run in CI environments:

```bash
# CI-friendly command
uv run pytest --cov=qraft --cov-report=xml --cov-report=term -n auto
```

## Contributing

When adding new features:

1. Write tests first (TDD approach recommended)
2. Ensure all tests pass: `uv run pytest`
3. Check coverage: `uv run pytest --cov=qraft`
4. Run linter: `uv run ruff check qraft/`

## Troubleshooting

### Import Errors

If you get import errors, ensure the package is installed:

```bash
uv pip install -e .
```

### Database Errors

If you get database errors, try recreating the test database:

```bash
uv run pytest --create-db
```

### Fixture Not Found

Ensure `conftest.py` is in the tests directory and pytest can discover it.

## Resources

- [pytest documentation](https://docs.pytest.org/)
- [pytest-django documentation](https://pytest-django.readthedocs.io/)
- [pytest-cov documentation](https://pytest-cov.readthedocs.io/)
