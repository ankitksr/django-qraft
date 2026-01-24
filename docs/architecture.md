# Architecture

This document describes Django-Qraft's internal architecture, design patterns, and extension strategy.

## Design Philosophy

**Core principle**: Drop-in enhancement of Django-Q2 via selective inheritance, not a fork.

Django-Qraft extends Django-Q2 at specific points to add features while preserving compatibility:
- Uses Django-Q2's broker system unchanged
- Reuses Django-Q2's monitor and pusher unchanged
- Selectively overrides cluster and worker spawning
- Enhances hook handling via interception

## System Overview

```
┌─────────────────────────────────────────────────────────┐
│                    QraftCluster                         │
│  (extends django_q.cluster.Cluster)                     │
└──────────────────────┬──────────────────────────────────┘
                       │
                       │ spawns
                       ▼
┌─────────────────────────────────────────────────────────┐
│                   QraftSentinel                         │
│  (extends django_q.cluster.Sentinel)                    │
│                                                         │
│  ┌──────────┐  ┌─────────┐  ┌───────────────────────┐ │
│  │  Pusher  │  │ Monitor │  │  Worker Processes     │ │
│  │ (Django- │  │(Django- │  │  ┌─────────────────┐  │ │
│  │  Q2)     │  │  Q2)    │  │  │  Standard       │  │ │
│  └──────────┘  └─────────┘  │  │  (threads=1)    │  │ │
│                              │  └─────────────────┘  │ │
│                              │  ┌─────────────────┐  │ │
│                              │  │  Threaded       │  │ │
│                              │  │  (threads>1)    │  │ │
│                              │  │  ThreadPool     │  │ │
│                              │  └─────────────────┘  │ │
│                              └───────────────────────┘ │
└─────────────────────────────────────────────────────────┘
```

## Extension Pattern: Selective Override

### Inheritance Hierarchy

```python
# Django-Q2 base classes
django_q.cluster.Cluster
django_q.cluster.Sentinel

# Qraft extensions
qraft.cluster.QraftCluster(Cluster)
qraft.cluster.QraftSentinel(Sentinel)
```

### Key Override Points

**1. QraftCluster.start()**
```python
def start(self):
    # Override to spawn QraftSentinel instead of Sentinel
    self.sentinel = QraftSentinel(
        stop_event=self.stop_event,
        start_event=self.start_event,
        # ... other params
    )
    return self.sentinel.start()
```

**2. QraftSentinel.spawn_worker()**
```python
def spawn_worker(self):
    # Conditionally spawn threaded or standard worker
    if self.conf.threads > 1:
        worker = threaded_worker  # Qraft's threaded worker
    else:
        worker = django_q_worker  # Standard Django-Q2 worker

    return self.spawn_process(worker, ...)
```

**3. Hook Handler Interception**
```python
# Register global hook handler
QRAFT_CLUSTER = {
    "hook": "qraft.hooks.qraft_hook_handler",
}

# Handler intercepts all task completions
def qraft_hook_handler(task):
    # Update QraftTask status
    # Dispatch success/failure hooks
    # Handle retries
```

All other Django-Q2 components (pusher, monitor, broker) remain unchanged.

## Database Schema

### Model Relationships

```
┌──────────────────────────────────────────────────────────┐
│                      QraftTask                           │
│  ┌────────────────────────────────────────────────────┐  │
│  │ id (UUID, PK)                                      │  │
│  │ func (str)                                         │  │
│  │ task_args (JSON)                                   │  │
│  │ task_kwargs (JSON)                                 │  │
│  │ status (enum: PENDING/RUNNING/SUCCEEDED/...)       │  │
│  │ success_hook, failure_hook (str, nullable)         │  │
│  │ retry_policy (JSON, nullable)                      │  │
│  │ created_at, updated_at                             │  │
│  └────────────────────────────────────────────────────┘  │
└─────────────────────┬────────────────────────────────────┘
                      │
                      │ 1:N
                      ▼
┌──────────────────────────────────────────────────────────┐
│                  QraftTaskAttempt                        │
│  ┌────────────────────────────────────────────────────┐  │
│  │ id (AutoField, PK)                                 │  │
│  │ qraft_task (FK → QraftTask)                        │  │
│  │ attempt_number (int)                               │  │
│  │ q2_task_id (str, unique, indexed)                  │  │
│  │ success (bool)                                     │  │
│  │ exception_class (str, nullable)                    │  │
│  │ created_at                                         │  │
│  │                                                    │  │
│  │ unique_together: (qraft_task, attempt_number)      │  │
│  └────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────┘
                      ▲
                      │ links to
                      │
┌──────────────────────────────────────────────────────────┐
│              Django-Q2 Task (django_q.Task)              │
│  ┌────────────────────────────────────────────────────┐  │
│  │ id (str, PK)                                       │  │
│  │ name (str)                                         │  │
│  │ func (str)                                         │  │
│  │ args, kwargs (binary)                              │  │
│  │ success (bool)                                     │  │
│  │ result (binary)                                    │  │
│  └────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────┐
│                    HookDispatch                          │
│  ┌────────────────────────────────────────────────────┐  │
│  │ id (AutoField, PK)                                 │  │
│  │ qraft_task (FK → QraftTask, CASCADE)               │  │
│  │ hook_type (enum: SUCCESS/FAILURE)                  │  │
│  │ hook_path (str)                                    │  │
│  │ q2_task_id (str, unique)                           │  │
│  │ dispatched_at                                      │  │
│  │                                                    │  │
│  │ unique_together: (qraft_task, hook_type)           │  │
│  └────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────┘
```

### Key Design Decisions

**1. QraftTask as Logical Task**
- Stores function metadata and configuration
- Survives across retries (same UUID for all attempts)
- Tracks overall status (PENDING → RUNNING → SUCCEEDED/FAILED/EXHAUSTED)

**2. QraftTaskAttempt as Execution Record**
- One record per execution (initial + retries)
- Links to Django-Q2 Task via `q2_task_id` (unique, indexed)
- Unique constraint: `(qraft_task, attempt_number)` prevents duplicates

**3. No Foreign Key to Django-Q2**
- `q2_task_id` is a string field, not FK
- Allows Django-Q2 Task to be deleted without breaking Qraft records
- Supports Django-Q2's ORM broker task cleanup

**4. HookDispatch for Idempotency**
- Unique constraint: `(qraft_task, hook_type)` ensures one hook per type
- Prevents duplicate hook execution on monitor restarts
- CASCADE deletion cleans up when QraftTask is deleted

## Task Lifecycle

### Dual Lookup Strategy

Django-Qraft uses two paths to link Django-Q2 tasks to Qraft records:

**Path 1: Initial Task (via async_task)**
```
1. Create QraftTask (UUID generated)
2. Create QraftTaskAttempt with attempt_number=1
3. Call Django-Q2's async_task(), get q2_task_id
4. Update QraftTaskAttempt with q2_task_id
5. Hook handler: Fast query lookup by q2_task_id ✓
```

**Path 2: Retry Task (via schedule_retry)**
```
1. Create Django-Q2 Schedule with task_name="qraft:{id}:{attempt}"
2. Scheduler queues task (not through async_task)
3. Hook handler: Parse task_name, create QraftTaskAttempt ✓
```

This dual-path design preserves user `task_name` while handling retries that aren't pre-created.

### State Transitions

```
PENDING ──────┐
              │
              │ task starts
              ▼
          RUNNING ──────┐
              │         │
    success   │         │ failure
              │         │
              ▼         ▼
        SUCCEEDED   FAILED ─────┐
                        │       │
                        │       │ should retry?
                        │       │
                        │    yes│
                        │       ▼
                        │   (schedule retry,
                        │    stays FAILED)
                        │
                    no  │
                        ▼
                   EXHAUSTED
```

### Lifecycle Example

**Scenario**: Task fails twice, succeeds on attempt 3

```python
# User creates task
task_id = async_task('myapp.tasks.flaky', qraft_options={'max_attempts': 3})

# QraftTask: id=UUID-1, status=PENDING, retry_policy={max_attempts:3}
# QraftTaskAttempt: qraft_task=UUID-1, attempt_number=1, q2_task_id="abc123"
```

**Attempt 1**: Task fails
```
1. Worker executes, raises exception
2. Hook handler called by Django-Q2 monitor
3. Lookup QraftTaskAttempt by q2_task_id="abc123" → found
4. Update: QraftTaskAttempt.success=False, exception_class="ValueError"
5. Update: QraftTask.status=FAILED
6. Check retry policy: should_retry=True (1 < 3)
7. Schedule retry: create Schedule with task_name="qraft:UUID-1:2", next_run=+30s
8. Call failure_hook? No (still retrying)
```

**Attempt 2**: Task fails again
```
1. Scheduler queues task from Schedule
2. Worker executes, raises exception
3. Hook handler: q2_task_id lookup fails (task wasn't created via async_task)
4. Fallback: parse task_name="qraft:UUID-1:2" → extract UUID-1, attempt=2
5. get_or_create QraftTaskAttempt(qraft_task=UUID-1, attempt_number=2)
6. Update: success=False, exception_class="ConnectionError"
7. Check retry policy: should_retry=True (2 < 3)
8. Schedule retry: create Schedule for attempt 3, next_run=+60s
```

**Attempt 3**: Task succeeds
```
1. Worker executes, returns result
2. Hook handler: parse task_name="qraft:UUID-1:3"
3. get_or_create QraftTaskAttempt(qraft_task=UUID-1, attempt_number=3)
4. Update: success=True
5. Update: QraftTask.status=SUCCEEDED
6. Check retry policy: N/A (succeeded)
7. Call success_hook('myapp.hooks.on_success')
```

## Hook Dispatching Flow

### Async Hook Execution (Default)

```
Task completes
  │
  ▼
qraft_hook_handler() called by Django-Q2 monitor
  │
  ├─► Lookup QraftTaskAttempt (query or parse task_name)
  │
  ├─► Update attempt outcome (success/exception)
  │
  ├─► Update QraftTask status
  │
  ├─► HookDispatcher.dispatch()
  │     │
  │     ├─► If failed: _handle_retry()
  │     │     │
  │     │     ├─► should_retry? Yes → schedule_retry()
  │     │     │                 No  → set EXHAUSTED, call failure_hook
  │     │     │
  │     │     └─► _call_hook_async('failure_hook')
  │     │           │
  │     │           ├─► Create HookDispatch record (idempotency)
  │     │           │
  │     │           └─► Queue hook as task with hook=None
  │     │
  │     └─► If succeeded: _call_hook_async('success_hook')
  │           │
  │           ├─► Create HookDispatch record
  │           │
  │           └─► Queue hook as task with hook=None
  │
  └─► Return (monitor continues)
```

### Synchronous Hook Execution (sync_hooks=True)

```
Task completes
  │
  ▼
qraft_hook_handler()
  │
  ├─► Lookup attempt, update status (same as async)
  │
  ├─► HookDispatcher.dispatch()
  │     │
  │     └─► _call_hook_sync()
  │           │
  │           ├─► Resolve hook function
  │           │
  │           ├─► Call hook(*args, **kwargs) directly
  │           │
  │           └─► No HookDispatch record created
  │
  └─► Return
```

**Critical**: Retries use `get_or_create()` for QraftTaskAttempt, allowing hook handler to create records for retry executions.

## Configuration System

### Settings Hierarchy

```
1. Django settings.QRAFT_CLUSTER (primary)
2. Django settings.Q_CLUSTER (fallback, with warning)
3. Environment variable Q_CLUSTER_NAME (selects ALT_CLUSTERS entry)
4. Pydantic defaults
```

### Custom Django Settings Source

```python
class DjangoSettingsSource:
    """Load settings from Django's settings.QRAFT_CLUSTER or Q_CLUSTER"""

    def __call__(self):
        # 1. Try QRAFT_CLUSTER
        if hasattr(django_settings, "QRAFT_CLUSTER"):
            return django_settings.QRAFT_CLUSTER

        # 2. Fallback to Q_CLUSTER
        if hasattr(django_settings, "Q_CLUSTER"):
            logger.warning("Using Q_CLUSTER (deprecated). Use QRAFT_CLUSTER.")
            return django_settings.Q_CLUSTER

        return {}
```

### ALT_CLUSTERS Pattern

For mixed workloads (CPU + I/O), run multiple clusters with different configs:

```python
QRAFT_CLUSTER = {
    "name": "default",
    "workers": 4,
    "threads": 1,  # Standard workers

    "ALT_CLUSTERS": {
        "io-workers": {
            "workers": 2,
            "threads": 8,  # Threaded workers
            "max_inflight": 16,
        },
        "cpu-workers": {
            "workers": 8,
            "threads": 1,  # More processes for CPU
        }
    }
}
```

Start clusters:
```bash
python manage.py qraftcluster                          # default
Q_CLUSTER_NAME=io-workers python manage.py qraftcluster  # io-workers
Q_CLUSTER_NAME=cpu-workers python manage.py qraftcluster # cpu-workers
```

Route tasks:
```python
async_task('cpu.task')                     # → default
async_task('io.task', cluster='io-workers')   # → io-workers
async_task('cpu.task', cluster='cpu-workers') # → cpu-workers
```

## Worker Architecture

### Standard Worker (threads=1)

```
Worker Process
  │
  ├─► Listen on task_queue (blocking)
  │
  ├─► Receive task dict
  │
  ├─► Resolve function from dotted path
  │
  ├─► Execute: result = func(*args, **kwargs)
  │
  ├─► Publish result to result_queue
  │
  └─► Loop
```

**Characteristics:**
- One task at a time per process
- Process-level timeout via sentinel timer
- Signal-based termination (SIGTERM)
- Best for CPU-bound tasks

### Threaded Worker (threads>1)

```
Worker Process
  │
  ├─► Create ThreadPoolExecutor(max_workers=threads)
  │
  ├─► Create Semaphore(max_inflight)
  │
  ├─► Listen on task_queue (blocking, in main thread)
  │
  ├─► Receive task dict
  │
  ├─► Acquire semaphore (backpressure control)
  │
  ├─► Submit to thread pool:
  │     └─► Thread executes task
  │           │
  │           ├─► close_old_connections() before
  │           ├─► result = func(*args, **kwargs)
  │           ├─► close_old_connections() after
  │           │
  │           ├─► Publish result to result_queue
  │           │
  │           └─► Release semaphore
  │
  └─► Loop
```

**Characteristics:**
- Multiple tasks concurrently (up to `threads`)
- Backpressure via semaphore (`max_inflight`)
- Database connection per thread
- Process-level timeout (kills all threads)
- Best for I/O-bound tasks

## Performance Characteristics

### Threading Overhead

- ThreadPoolExecutor creation: one-time per worker process
- Semaphore acquire/release: ~microseconds per task
- Extra DB query for attempt lookup: ~1ms per task completion
- Connection management: ~1ms per task (open/close)

### Expected Speedup (I/O-bound)

| Configuration | Concurrent Tasks | Expected Throughput |
|---------------|------------------|---------------------|
| 2 workers, threads=1 | 2 | Baseline (1x) |
| 2 workers, threads=4 | 8 | 3-4x |
| 2 workers, threads=8 | 16 | 6-8x |
| 4 workers, threads=8 | 32 | 10-15x |

**Diminishing returns**: Beyond threads=16, context switching overhead increases.

### No Speedup For:

- CPU-bound tasks (Python GIL prevents true parallelism)
- Tasks with many DB writes (DB becomes bottleneck)
- Tasks that block on locks or shared resources

## Key Extension Points

For developers extending Qraft:

1. **Custom worker types**: Override `QraftSentinel.spawn_worker()`
2. **Custom hook dispatching**: Override `HookDispatcher._call_hook_async()`
3. **Custom retry logic**: Override `HookDispatcher._handle_retry()`
4. **Custom status tracking**: Extend `QraftTask` model
5. **Custom brokers**: Use Django-Q2's broker system (works unchanged)

## Common Pitfalls

1. **Retry tasks aren't pre-created**
   - Hook handler must handle `QraftTaskAttempt.DoesNotExist` for retries
   - Solution: Use `get_or_create()` with task_name parsing

2. **Threading != parallelism for CPU-bound**
   - Python GIL prevents true parallelism
   - Solution: Use standard workers (threads=1) for CPU tasks

3. **Process-level timeouts with threading**
   - One stuck thread terminates entire worker process
   - Solution: Keep tasks under timeout, or use standard workers

4. **Database connections per thread**
   - Each thread needs a connection
   - Solution: Size connection pool ≥ workers × threads

5. **Hook recursion**
   - Hooks must use `hook=None` to prevent infinite loops
   - Solution: Framework enforces this automatically

6. **task_name preservation**
   - User's task_name is preserved, not overwritten
   - Solution: Use `q2_task_id` for linkage, not task_name parsing

## Related Documentation

- [Configuration](configuration.md) - Settings reference
- [Threading](threading.md) - Multithreaded worker details
- [Hooks](hooks.md) - Hook system details
- [Retry](retry.md) - Retry policy details
- [Development](development.md) - Contributing guidelines
