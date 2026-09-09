# Hook System

Django-Qraft provides a dual-phase hook system with separate success and failure handlers, async execution for performance, and comprehensive tracking for reliability.

## Table of Contents

- [Overview](#overview)
- [Basic Usage](#basic-usage)
- [Hook Function Signatures](#hook-function-signatures)
- [Async vs Sync Hooks](#async-vs-sync-hooks)
- [Hook Tracking and Idempotency](#hook-tracking-and-idempotency)
- [Hook Arguments](#hook-arguments)
- [Retry Integration](#retry-integration)
- [Use Cases and Patterns](#use-cases-and-patterns)
- [Signals](#signals)
- [Advanced Topics](#advanced-topics)
- [Troubleshooting](#troubleshooting)

## Overview

Django-Qraft extends Django-Q2's single hook with a dual-phase system:

**Django-Q2 hooks:**
- Single hook called after task completion (success or failure)
- Runs synchronously in monitor process
- No built-in retry integration

**Django-Qraft hooks:**
- Separate success and failure hooks with custom arguments
- Async execution over workers (default) or sync in monitor
- Integrated with retry policy (failure hook called only after exhaustion)
- Tracked via `HookDispatch` model for idempotency

## Basic Usage

### Success Hook Only

```python
from qraft.tasks import async_task

# Define hook function
def on_success(**kwargs):
    print("Task completed")

# Queue task with success hook
task_id = async_task(
    'myapp.tasks.process_data',
    data={'key': 'value'},
    qraft_options={
        'success_hook': 'myapp.hooks.on_success',
    }
)
```

### Failure Hook Only

```python
def on_failure(context=None, **kwargs):
    print(f"Task {context['task_id']} failed with {context['exception_class']}")
    # Send alert, log to monitoring service, etc.

task_id = async_task(
    'myapp.tasks.risky_operation',
    qraft_options={
        'failure_hook': 'myapp.hooks.on_failure',
        'hook_context': True,
    }
)
```

### Both Hooks

```python
def on_success(**kwargs):
    print("Success")

def on_failure(context=None, **kwargs):
    print(f"Failure: {context['exception_class']}")

task_id = async_task(
    'myapp.tasks.important_task',
    arg1, arg2,
    qraft_options={
        'success_hook': 'myapp.hooks.on_success',
        'failure_hook': 'myapp.hooks.on_failure',
        'hook_context': True,
    }
)
```

## Hook Function Signatures

Qraft injects nothing into a hook. A hook receives exactly what you configured:
`success_args`/`failure_args` as positional arguments, `success_kwargs`/`failure_kwargs`
as keyword arguments, and — only when the task sets `hook_context=True` — one extra
`context` keyword. The task's return value is not passed; reach it through
`context['result_ref']`, described in [Hook context](#hook-context).

Write every hook with `**kwargs` so a later Qraft release that adds a keyword does not
break it.

### Success Hook

Called when a task completes successfully.

```python
def on_success(user_id, notify=True, **kwargs):
    if notify:
        send_notification(user_id, "Task completed")

async_task(
    'myapp.tasks.process_data',
    qraft_options={
        'success_hook': 'myapp.hooks.on_success',
        'success_args': [42],            # -> user_id
        'success_kwargs': {'notify': True},
    },
)
```

### Failure Hook

Called when a task fails and retries are exhausted, or when the task has no retry policy.

The failing task's id and exception class are not injected either. Opt into `context` to
get them:

```python
def on_failure(alert_level='warning', context=None, **kwargs):
    if context and alert_level == 'critical':
        send_pager_alert(
            f"Task {context['task_id']} failed: {context['exception_class']}"
        )

async_task(
    'myapp.tasks.process_data',
    qraft_options={
        'failure_hook': 'myapp.hooks.on_failure',
        'failure_kwargs': {'alert_level': 'critical'},
        'hook_context': True,
    },
)
```

### Complete Example

```python
# myapp/hooks.py

def on_task_success(pipeline_id=None, context=None, **kwargs):
    """Called when task completes successfully.

    Args:
        pipeline_id: Custom argument
        context: task_id, result_ref and the rest of the attempt's outcome
    """
    if pipeline_id:
        update_pipeline_status(pipeline_id, 'completed')

    logger.info("Task %s completed successfully", context['task_id'])


def on_task_failure(notify_admin=False, context=None, **kwargs):
    """Called when task fails after all retries exhausted.

    Args:
        notify_admin: Whether to send admin alert
        context: task_id, exception_class and the rest of the attempt's outcome
    """
    logger.error(
        "Task %s failed permanently: %s", context['task_id'], context['exception_class']
    )

    if notify_admin:
        send_admin_alert(
            subject=f"Task {context['task_id']} Failed",
            body=f"Exception: {context['exception_class']}",
        )

# myapp/tasks.py
from qraft.tasks import async_task

task_id = async_task(
    'myapp.tasks.process_pipeline',
    data,
    qraft_options={
        'success_hook': 'myapp.hooks.on_task_success',
        'success_kwargs': {'pipeline_id': 'pipe-123'},
        'failure_hook': 'myapp.hooks.on_task_failure',
        'failure_kwargs': {'notify_admin': True},
        'hook_context': True,
    }
)
```

## Async vs Sync Hooks

Django-Qraft supports two hook execution modes: async (default) and sync (legacy).

### Async Hooks (Default)

Hooks are queued as separate tasks over workers instead of running in the monitor process.

**Configuration:**

```python
QRAFT_CLUSTER = {
    "sync_hooks": False,  # Default
}
```

**How it works:**

```
Task completes
  │
  ├─► Monitor detects completion
  │
  ├─► qraft_hook_handler() called
  │     │
  │     ├─► Update QraftTask status
  │     │
  │     └─► Queue hook as async task
  │           │
  │           ├─► Create HookDispatch record
  │           │
  │           └─► django_q.tasks.async_task(hook_path, ...)
  │
  └─► Monitor continues (non-blocking)

Worker picks up hook task
  │
  └─► Execute hook function
```

**Benefits:**

1. **Non-blocking**: Monitor doesn't wait for hook completion
2. **Scalable**: Hooks run over worker pool, not single monitor thread
3. **Tracked**: `HookDispatch` records provide execution history
4. **Idempotent**: Unique constraint prevents duplicate hook execution
5. **Retriable**: Hook tasks can fail and retry independently

**Trade-offs:**

- Hook execution is asynchronous (not immediate)
- Adds overhead (extra task, DB record)
- Hook failures require separate monitoring

### Sync Hooks (Legacy)

Hooks execute synchronously in the monitor process after task completion.

**Configuration:**

```python
QRAFT_CLUSTER = {
    "sync_hooks": True,
}
```

**How it works:**

```
Task completes
  │
  ├─► Monitor detects completion
  │
  ├─► qraft_hook_handler() called
  │     │
  │     ├─► Update QraftTask status
  │     │
  │     └─► Call hook function directly
  │           │
  │           └─► hook_func(*args, **kwargs)
  │
  └─► Monitor continues (after hook completes)
```

**When to use:**

1. **Debugging**: Easier to trace hook execution
2. **Immediate execution required**: Hook must complete before task acknowledgment
3. **Django-Q2 compatibility**: Exact Django-Q2 behavior
4. **Simple hooks**: Very fast hooks that don't benefit from async

**Trade-offs:**

- Monitor can become bottleneck with many completions
- Slow hooks delay task processing
- No automatic retry on hook failure
- No `HookDispatch` tracking

### Comparison

| Feature | Async (Default) | Sync (Legacy) |
|---------|----------------|---------------|
| Execution | In worker pool | In monitor process |
| Blocking | Non-blocking | Blocks monitor |
| Tracking | HookDispatch records | None |
| Idempotency | Guaranteed (DB constraint) | Manual |
| Retry | Hook can retry as task | No retry |
| Performance | High (scalable) | Limited (single thread) |
| Debugging | Harder (async) | Easier (sync) |

**Recommendation:** Use async (default) for production. Use sync for debugging or Django-Q2 compatibility.

## Hook Tracking and Idempotency

### HookDispatch Model

Async hooks create a `HookDispatch` record for tracking:

```python
from qraft.models import HookDispatch

class HookDispatch(models.Model):
    qraft_task = models.ForeignKey(QraftTask, on_delete=models.CASCADE)
    hook_type = models.CharField(max_length=10)  # 'success' or 'failure'
    hook_path = models.CharField(max_length=256)  # Dotted path
    q2_task_id = models.CharField(max_length=32, unique=True)  # Hook task ID
    date_created = models.DateTimeField(auto_now_add=True)

    class Meta:
        unique_together = [('qraft_task', 'hook_type')]
```

### Idempotency Guarantee

The `(qraft_task, hook_type)` unique constraint ensures hooks execute exactly once:

```python
# First attempt: Creates HookDispatch record
hook_dispatch, created = HookDispatch.objects.get_or_create(
    qraft_task=task,
    hook_type='success',
    defaults={'hook_path': 'myapp.hooks.on_success', ...}
)

if created:
    # Queue hook as task
    q2_task_id = async_task(hook_path, ...)
    hook_dispatch.q2_task_id = q2_task_id
    hook_dispatch.save()
else:
    # Hook already dispatched, skip
    return
```

**Prevents:**
- Duplicate hook execution on monitor restarts
- Race conditions in distributed setups
- Accidental re-queuing

### Querying Hook Status

**Find hooks for a task:**

```python
from qraft.models import QraftTask, HookDispatch
from django_q.models import Task

# Get task
qraft_task = QraftTask.objects.get(id='some-uuid')

# Find dispatched hooks
hooks = HookDispatch.objects.filter(qraft_task=qraft_task)

for hook in hooks:
    print(f"Hook: {hook.hook_type}")
    print(f"Path: {hook.hook_path}")
    print(f"Created: {hook.date_created}")

    # Check if hook completed
    q2_task = Task.objects.get(id=hook.q2_task_id)
    print(f"Success: {q2_task.success}")
    if q2_task.success:
        print(f"Result: {q2_task.result}")
```

**Check if success hook was called:**

```python
success_hook = HookDispatch.objects.filter(
    qraft_task=task,
    hook_type='success',
).first()

if success_hook:
    # Hook was dispatched
    q2_task = Task.objects.get(id=success_hook.q2_task_id)
    if q2_task.success:
        print("Hook completed successfully")
    else:
        print("Hook failed")
else:
    print("Hook not dispatched yet")
```

### Hook Task Naming

Hook tasks use a special naming convention:

```
hook:{hook_type}:{qraft_task_id}

Examples:
- hook:success:123e4567-e89b-12d3-a456-426614174000
- hook:failure:123e4567-e89b-12d3-a456-426614174000
```

**Query hook tasks:**

```python
from django_q.models import Task

# Find all success hook tasks
success_hooks = Task.objects.filter(name__startswith='hook:success:')

# Find hooks for specific task
task_id = '123e4567-e89b-12d3-a456-426614174000'
task_hooks = Task.objects.filter(name__startswith=f'hook:{hook_type}:{task_id}')
```

## Hook Arguments

Hooks can receive custom arguments via `success_kwargs` and `failure_kwargs`.

### Hook context

A hook receives the arguments the caller froze at enqueue. It does not know which
attempt produced it or how that attempt ended. Set `hook_context` and it does:

```python
async_task(
    'myapp.tasks.ingest', worksheet_id,
    qraft_options={
        'success_hook': 'myapp.hooks.on_ingested',
        'failure_hook': 'myapp.hooks.on_ingest_failed',
        'hook_context': True,
    },
)

def on_ingested(context=None, **kwargs):
    logger.info("attempt %s of %s ended %s",
                context['attempt_number'], context['task_id'], context['outcome'])
```

`context` is a plain dict with `task_id`, `attempt_id`, `attempt_number`, `outcome`
(`succeeded`, `failed` or `orphaned`), `exception_class`, `run_id`, `stage`,
`subject_type`, `subject_id`, `result_ref` (the Django-Q2 task id whose `result` holds
the return value), `traceparent`, `date_started` and `date_completed`. It is assembled in
the hook handler from rows it already holds and frozen into the hook task's arguments, so
it survives the same crashes the `HookDispatch` row does.

The flag is opt-in because `context` is a plausible name for a caller's own keyword
argument — an existing hook signature never changes underneath you. A task that opts in
and also passes `context` in `success_kwargs` loses the caller's value.

Workflow constructors take the same flag, and their hooks receive the workflow's id,
type, outcome and counters — `current_step_index` for a chain, `completed_count`,
`total_count`, `success_count` and `failure_count` for an iter or a batch:

```python
QraftBatch(on_success='myapp.hooks.batch_done', hook_context=True)
```

`on_cancelled` receives the same shape as `on_success` and `on_failure`, read at the
moment the cancel took effect. How much of a fan-out had already finished is the
question that hook exists to answer.

A run's `on_settled` hook always receives a `context` — it has no caller-frozen arguments
to protect — carrying the run's ids, outcome and per-stage outcomes. See
[Runs](workflows.md#the-durable-completion-event).

### Success Hook Arguments

```python
def on_success(user_id=None, notify=True, **kwargs):
    """
    Args:
        user_id: Custom argument, bound from success_kwargs
        notify: Custom argument, bound from success_kwargs
        **kwargs: Catch-all for future arguments
    """
    if notify and user_id:
        User.objects.get(id=user_id).notify("Task completed")

task_id = async_task(
    'myapp.tasks.process_data',
    data,
    qraft_options={
        'success_hook': 'myapp.hooks.on_success',
        'success_kwargs': {
            'user_id': 123,
            'notify': True,
        },
    }
)
```

### Failure Hook Arguments

```python
def on_failure(alert_level='warning', owner=None, context=None, **kwargs):
    """
    Args:
        alert_level: Custom argument, bound from failure_kwargs
        owner: Custom argument, bound from failure_kwargs
        context: task_id and exception_class, present because hook_context is set
        **kwargs: Catch-all
    """
    message = f"Task {context['task_id']} failed: {context['exception_class']}"

    if alert_level == 'critical':
        send_pagerduty_alert(message)
    elif alert_level == 'warning':
        logger.warning(message)

    if owner:
        notify_user(owner, message)

task_id = async_task(
    'myapp.tasks.critical_task',
    qraft_options={
        'failure_hook': 'myapp.hooks.on_failure',
        'failure_kwargs': {
            'alert_level': 'critical',
            'owner': 'admin@example.com',
        },
        'hook_context': True,
    }
)
```

### Dynamic Arguments

You can pass computed values:

```python
import uuid

pipeline_id = str(uuid.uuid4())
pipeline_meta = {
    'pipeline_id': pipeline_id,
    'started_at': datetime.now().isoformat(),
    'user': request.user.id,
}

task_id = async_task(
    'myapp.tasks.process_pipeline',
    data,
    qraft_options={
        'success_hook': 'myapp.hooks.on_pipeline_success',
        'success_kwargs': {'pipeline_meta': pipeline_meta},
        'failure_hook': 'myapp.hooks.on_pipeline_failure',
        'failure_kwargs': {'pipeline_meta': pipeline_meta},
    }
)
```

Note the kwarg name here is `pipeline_meta`, not `context` — `context` is reserved for the dict Qraft assembles when `hook_context` is set (see [Hook context](#hook-context)).

## Retry Integration

Hooks integrate seamlessly with retry policies:

- **Success hook**: Called when task succeeds (any attempt)
- **Failure hook**: Called only after all retries are exhausted

### Example: Task with Retries and Hooks

```python
# Task that may fail initially but retries
task_id = async_task(
    'myapp.tasks.flaky_api_call',
    url='https://api.example.com/data',
    qraft_options={
        # Retry configuration
        'max_attempts': 3,
        'base_delay': 30,
        'backoff_strategy': 'exponential',

        # Hook configuration
        'success_hook': 'myapp.hooks.on_api_success',
        'failure_hook': 'myapp.hooks.on_api_failure',
    }
)
```

**Execution flow:**

```
Attempt 1: Fails (ConnectionError)
  → Retry scheduled (delay: 30s)
  → Failure hook NOT called (still retrying)

Attempt 2: Fails (Timeout)
  → Retry scheduled (delay: 60s)
  → Failure hook NOT called (still retrying)

Attempt 3: Succeeds
  → Success hook called
  → Failure hook NOT called
```

**Alternative flow (all retries fail):**

```
Attempt 1: Fails (ConnectionError)
  → Retry scheduled

Attempt 2: Fails (Timeout)
  → Retry scheduled

Attempt 3: Fails (ConnectionError)
  → All retries exhausted
  → Status set to EXHAUSTED
  → Failure hook called
```

### Checking Retry Status in Hooks

```python
def on_failure(context=None, **kwargs):
    from qraft.models import QraftTask, TaskStatus

    task = QraftTask.objects.get(id=context['task_id'])

    if task.status == TaskStatus.EXHAUSTED:
        logger.error(
            "Task %s exhausted after %s attempts: %s",
            task.id, task.attempt_count, context['exception_class'],
        )
    else:
        logger.warning("Task %s failed: %s", task.id, context['exception_class'])
```

Pass `'hook_context': True` in `qraft_options` for this hook to receive `context`.

## Use Cases and Patterns

### Pattern 1: Notification on Completion

```python
def notify_user(user_id=None, **kwargs):
    if user_id:
        user = User.objects.get(id=user_id)
        user.email_user(
            subject='Task Completed',
            message='Your task completed successfully.',
        )

async_task(
    'myapp.tasks.export_data',
    user_id=request.user.id,
    qraft_options={
        'success_hook': 'myapp.hooks.notify_user',
        'success_kwargs': {'user_id': request.user.id},
    }
)
```

### Pattern 2: Task Chaining

```python
from django_q.models import Task

def on_extract_success(pipeline_id=None, context=None, **kwargs):
    extracted = Task.objects.get(id=context['result_ref']).result
    async_task(
        'myapp.tasks.transform_data',
        extracted,
        qraft_options={
            'success_hook': 'myapp.hooks.on_transform_success',
            'success_kwargs': {'pipeline_id': pipeline_id},
            'hook_context': True,
        }
    )

def on_transform_success(pipeline_id=None, context=None, **kwargs):
    transformed = Task.objects.get(id=context['result_ref']).result
    async_task(
        'myapp.tasks.load_data',
        transformed,
        qraft_options={
            'success_hook': 'myapp.hooks.on_load_success',
            'success_kwargs': {'pipeline_id': pipeline_id},
        }
    )

# Start pipeline
async_task(
    'myapp.tasks.extract_data',
    source='api',
    qraft_options={
        'success_hook': 'myapp.hooks.on_extract_success',
        'success_kwargs': {'pipeline_id': 'pipe-123'},
        'hook_context': True,
    }
)
```

For task orchestration like this, prefer [`QraftChain`](workflows.md) — it is built for exactly this case and does not need the hook to fetch its own predecessor's result.

### Pattern 3: Status Updates

```python
from django_q.models import Task

def update_job_status(job_id=None, context=None, **kwargs):
    if job_id:
        result = Task.objects.get(id=context['result_ref']).result
        Job.objects.filter(id=job_id).update(
            status='completed',
            completed_at=timezone.now(),
            result=result,
        )

def mark_job_failed(job_id=None, context=None, **kwargs):
    if job_id:
        Job.objects.filter(id=job_id).update(
            status='failed',
            error=context['exception_class'],
            failed_at=timezone.now(),
        )

job = Job.objects.create(status='pending')

async_task(
    'myapp.tasks.process_job',
    job.data,
    qraft_options={
        'success_hook': 'myapp.hooks.update_job_status',
        'success_kwargs': {'job_id': job.id},
        'failure_hook': 'myapp.hooks.mark_job_failed',
        'failure_kwargs': {'job_id': job.id},
        'hook_context': True,
    }
)
```

### Pattern 4: Cleanup on Failure

```python
def cleanup_temp_files(temp_dir=None, **kwargs):
    if temp_dir and os.path.exists(temp_dir):
        shutil.rmtree(temp_dir)
        logger.info(f"Cleaned up temporary directory: {temp_dir}")

temp_dir = tempfile.mkdtemp()

async_task(
    'myapp.tasks.process_files',
    temp_dir,
    qraft_options={
        'failure_hook': 'myapp.hooks.cleanup_temp_files',
        'failure_kwargs': {'temp_dir': temp_dir},
    }
)
```

### Pattern 5: Metrics Collection

```python
def record_success_metric(metric_name=None, **kwargs):
    if metric_name:
        statsd.increment(f'{metric_name}.success')

def record_failure_metric(metric_name=None, context=None, **kwargs):
    if metric_name:
        statsd.increment(f'{metric_name}.failure')
        statsd.increment(f'{metric_name}.failure.{context["exception_class"]}')

async_task(
    'myapp.tasks.api_call',
    url,
    qraft_options={
        'success_hook': 'myapp.hooks.record_success_metric',
        'success_kwargs': {'metric_name': 'api.external'},
        'failure_hook': 'myapp.hooks.record_failure_metric',
        'failure_kwargs': {'metric_name': 'api.external'},
        'hook_context': True,
    }
)
```

## Signals

A hook is one callback per task. When an application wants to react to every
transition from one place — write a domain event, invalidate a cache, log a line —
`qraft.signals` is the cheaper surface.

```python
from django.dispatch import receiver
from qraft import signals

@receiver(signals.task_settled)
def on_task_settled(sender, payload, **kwargs):
    Event.objects.create(
        subject_type=payload['subject_type'],
        subject_id=payload['subject_id'],
        kind=payload['outcome'],
    )
```

| Signal | Fires when | Process |
|---|---|---|
| `task_started` | the lease opens on an attempt for the first time | worker |
| `attempt_finished` | an attempt resolves, whatever the outcome | monitor, or the process that reaped |
| `task_settled` | a task reaches SUCCEEDED, FAILED or EXHAUSTED | monitor |
| `workflow_settled` | a chain, iter or batch settles | monitor, or the web process that cancelled |
| `attempt_stall_suspected` | the reaper flags an attempt | monitor |
| `run_settled`, `run_overdue` | a run settles or is flagged overdue | monitor |

There is no `attempt_failed`: a receiver that only wants failures reads `outcome` on
`attempt_finished`.

### Payloads are ids, not instances

Every signal sends `sender=` the model class and one `payload` keyword argument: an
immutable mapping holding `task_id`, `attempt_id`, `attempt_number`, `func`, `status`,
`outcome`, `exception_class`, `run_id`, `stage`, `subject_type`, `subject_id`, `cluster`,
and the attempt's timestamps as ISO strings. A model instance captured before commit and
handed to a receiver after it is a snapshot that may already be stale; ids are what a
receiver looks up when it needs more.

Sends are registered with `transaction.on_commit` from inside the transaction that
performs the transition, so a receiver that reads rows sees the committed resolution.
Outside a transaction `on_commit` runs the callback immediately.

### Best effort, by design

**Signals are best-effort observers for logs, caches and in-process reactions; anything
the application must not miss goes through a hook, whose dispatch row survives a crash.**

Every send uses `send_robust()`: a receiver that raises is logged at warning with its
name and never reaches the caller, which is the hook handler in the monitor. A broken
observer must not break completion routing.

Each signal fires at most once per transition, guarded by the same compare-and-swaps the
transitions use: the attempt resolution's `success__isnull=True` update, the workflow's
`settled_at` column, the lease's `date_started__isnull=True` update. What that does not
cover is a crash between commit and the `on_commit` callback — that window loses the
signal, and `on_commit` callbacks are not durable. The row still records the transition.

### Receivers must be cheap

Every signal except `task_started` fires on the thread that runs the hook handler, in the
monitor process, or in the reaper thread beside it. That is the thread Qraft moved hooks
off so the monitor would not bottleneck. A row insert, a cache write or a log line is
fine. A receiver that calls a provider or runs a report belongs in a hook.

`task_started` fires in the worker process, beside the task itself.

## Advanced Topics

### Hook Recursion Prevention

Hook tasks automatically use `hook=None` to prevent infinite recursion:

```python
# In qraft/hooks.py
q2_task_id = q2_async_task(
    hook_path,
    *args,
    **kwargs,
    task_name=f"hook:{hook_type}:{qraft_task.id}",
    hook=None,  # No hook on hook tasks!
)
```

This prevents:

```
Task → Success hook → Success hook → Success hook → ...
```

### Hook Errors

**With async hooks:**

Hook failures are tracked in Django-Q2 Task records:

```python
hook_dispatch = HookDispatch.objects.get(qraft_task=task, hook_type='success')
q2_task = Task.objects.get(id=hook_dispatch.q2_task_id)

if not q2_task.success:
    print(f"Hook failed: {q2_task.result}")
```

**With sync hooks:**

Hook errors are logged but don't prevent task completion:

```python
# In qraft/hooks.py (sync mode)
try:
    hook_func(*args, **kwargs)
except Exception as e:
    logger.error(f"Hook failed: {e}", exc_info=True)
    # Task is still marked as completed
```

### Accessing Task Metadata in Hooks

Set `hook_context` to get the task's id and the rest of its outcome, then look up the
`QraftTask` row for anything not already in `context`:

```python
def on_success(context=None, **kwargs):
    from qraft.models import QraftTask

    task = QraftTask.objects.get(id=context['task_id'])
    print(f"Task function: {task.func}")
    print(f"Attempts: {task.attempt_count}")
    print(f"Created: {task.created_at}")

async_task(
    'myapp.tasks.process_data',
    data,
    qraft_options={
        'success_hook': 'myapp.hooks.on_success',
        'hook_context': True,
    }
)
```

See [Hook context](#hook-context) for the full set of fields `context` carries.

## Troubleshooting

### Hook Not Called

**Check hook configuration:**

```python
task = QraftTask.objects.get(id=task_id)
print(f"Success hook: {task.success_hook}")
print(f"Failure hook: {task.failure_hook}")
```

**Check task status:**

```python
print(f"Status: {task.status}")
# Hooks only called when status is SUCCEEDED or EXHAUSTED
```

**Check HookDispatch records:**

```python
hooks = HookDispatch.objects.filter(qraft_task=task)
if not hooks.exists():
    print("Hook not dispatched yet")
```

### Hook Called Multiple Times

**With async hooks:** Should not happen due to unique constraint

**Check for duplicates:**

```python
duplicates = HookDispatch.objects.filter(
    qraft_task=task
).values('hook_type').annotate(count=Count('id')).filter(count__gt=1)

if duplicates.exists():
    print("Duplicate hook dispatches found!")
```

**With sync hooks:** May happen on monitor restart

**Solution:** Use async hooks (default) for idempotency guarantee

### Hook Import Errors

**Check hook path:**

```python
# Correct
'success_hook': 'myapp.hooks.on_success'

# Wrong - missing module
'success_hook': 'on_success'

# Wrong - invalid path
'success_hook': 'myapp.hooks.typo_on_success'
```

**Test import manually:**

```bash
python manage.py shell
>>> from myapp.hooks import on_success  # Should not raise ImportError
```

### Hook Arguments Not Received

**Check kwargs are being passed:**

```python
def on_success(custom_arg=None, **kwargs):
    print(f"Received custom_arg: {custom_arg}")
    print(f"All kwargs: {kwargs}")

async_task(
    'myapp.tasks.my_task',
    qraft_options={
        'success_hook': 'myapp.hooks.on_success',
        'success_kwargs': {'custom_arg': 'value'},
    }
)
```

**Check for typos in kwarg names:**

```python
# In async_task call
'success_kwargs': {'cusom_arg': 'value'}  # Typo!

# In hook function
def on_success(custom_arg=None, **kwargs):  # Won't match
```

## Related Documentation

- [Getting Started](getting-started.md) - Basic hook usage
- [Configuration](configuration.md) - Hook settings reference
- [Retry Policies](retry.md) - Retry integration with hooks
- [Architecture](architecture.md) - Hook dispatching internals
