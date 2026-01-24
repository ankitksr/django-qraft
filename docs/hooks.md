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
def on_success(result, **kwargs):
    print(f"Task completed with result: {result}")

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
def on_failure(task_id, exception_class, **kwargs):
    print(f"Task {task_id} failed with {exception_class}")
    # Send alert, log to monitoring service, etc.

task_id = async_task(
    'myapp.tasks.risky_operation',
    qraft_options={
        'failure_hook': 'myapp.hooks.on_failure',
    }
)
```

### Both Hooks

```python
def on_success(result, **kwargs):
    print(f"Success: {result}")

def on_failure(task_id, exception_class, **kwargs):
    print(f"Failure: {exception_class}")

task_id = async_task(
    'myapp.tasks.important_task',
    arg1, arg2,
    qraft_options={
        'success_hook': 'myapp.hooks.on_success',
        'failure_hook': 'myapp.hooks.on_failure',
    }
)
```

## Hook Function Signatures

### Success Hook

Called when a task completes successfully.

**Required parameters:**

```python
def on_success(result, **kwargs):
    """
    Args:
        result: The return value from the task function
        **kwargs: Additional arguments (from success_kwargs)
    """
    pass
```

**Example with custom arguments:**

```python
def on_success(result, user_id=None, notify=True, **kwargs):
    if notify and user_id:
        send_notification(user_id, f"Task completed: {result}")
```

### Failure Hook

Called when a task fails and retries are exhausted (or no retry policy configured).

**Required parameters:**

```python
def on_failure(task_id, exception_class, **kwargs):
    """
    Args:
        task_id: UUID of the QraftTask
        exception_class: Name of exception that caused failure (str)
        **kwargs: Additional arguments (from failure_kwargs)
    """
    pass
```

**Example with custom arguments:**

```python
def on_failure(task_id, exception_class, alert_level='warning', **kwargs):
    if alert_level == 'critical':
        send_pager_alert(f"Task {task_id} failed: {exception_class}")
    else:
        log_warning(f"Task {task_id} failed: {exception_class}")
```

### Complete Example

```python
# myapp/hooks.py

def on_task_success(result, pipeline_id=None, **kwargs):
    """
    Called when task completes successfully.

    Args:
        result: Task return value
        pipeline_id: Optional pipeline identifier
    """
    if pipeline_id:
        update_pipeline_status(pipeline_id, 'completed', result)

    # Log to monitoring
    logger.info(f"Task completed successfully: {result}")


def on_task_failure(task_id, exception_class, notify_admin=False, **kwargs):
    """
    Called when task fails after all retries exhausted.

    Args:
        task_id: QraftTask UUID
        exception_class: Exception class name (str)
        notify_admin: Whether to send admin alert
    """
    # Log failure
    logger.error(f"Task {task_id} failed permanently: {exception_class}")

    # Conditional admin notification
    if notify_admin:
        send_admin_alert(
            subject=f"Task {task_id} Failed",
            body=f"Exception: {exception_class}",
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
    hook_path = models.CharField(max_length=255)  # Dotted path
    q2_task_id = models.CharField(max_length=32, unique=True)  # Hook task ID
    dispatched_at = models.DateTimeField(auto_now_add=True)

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
    print(f"Dispatched: {hook.dispatched_at}")

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

### Success Hook Arguments

```python
def on_success(result, user_id=None, notify=True, context=None, **kwargs):
    """
    Args:
        result: Task return value (always provided)
        user_id: Custom argument
        notify: Custom argument
        context: Custom argument
        **kwargs: Catch-all for future arguments
    """
    if notify and user_id:
        User.objects.get(id=user_id).notify(f"Task completed: {result}")

    if context:
        context['status'] = 'completed'
        context['result'] = result

task_id = async_task(
    'myapp.tasks.process_data',
    data,
    qraft_options={
        'success_hook': 'myapp.hooks.on_success',
        'success_kwargs': {
            'user_id': 123,
            'notify': True,
            'context': {'pipeline': 'data-import'},
        },
    }
)
```

### Failure Hook Arguments

```python
def on_failure(task_id, exception_class, alert_level='warning', owner=None, **kwargs):
    """
    Args:
        task_id: QraftTask UUID (always provided)
        exception_class: Exception class name (always provided)
        alert_level: Custom argument
        owner: Custom argument
        **kwargs: Catch-all
    """
    message = f"Task {task_id} failed: {exception_class}"

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
    }
)
```

### Dynamic Arguments

You can pass computed values:

```python
import uuid

pipeline_id = str(uuid.uuid4())
context = {
    'pipeline_id': pipeline_id,
    'started_at': datetime.now().isoformat(),
    'user': request.user.id,
}

task_id = async_task(
    'myapp.tasks.process_pipeline',
    data,
    qraft_options={
        'success_hook': 'myapp.hooks.on_pipeline_success',
        'success_kwargs': {'context': context},
        'failure_hook': 'myapp.hooks.on_pipeline_failure',
        'failure_kwargs': {'context': context},
    }
)
```

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
def on_failure(task_id, exception_class, **kwargs):
    from qraft.models import QraftTask, TaskStatus

    task = QraftTask.objects.get(id=task_id)

    if task.status == TaskStatus.EXHAUSTED:
        # All retries exhausted
        logger.error(
            f"Task {task_id} exhausted after {task.attempt_count} attempts: {exception_class}"
        )
    else:
        # Single failure without retry
        logger.warning(f"Task {task_id} failed: {exception_class}")
```

## Use Cases and Patterns

### Pattern 1: Notification on Completion

```python
def notify_user(result, user_id=None, **kwargs):
    if user_id:
        user = User.objects.get(id=user_id)
        user.email_user(
            subject='Task Completed',
            message=f'Your task completed successfully: {result}',
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
def on_extract_success(result, pipeline_id=None, **kwargs):
    # Chain to next task
    async_task(
        'myapp.tasks.transform_data',
        result,
        qraft_options={
            'success_hook': 'myapp.hooks.on_transform_success',
            'success_kwargs': {'pipeline_id': pipeline_id},
        }
    )

def on_transform_success(result, pipeline_id=None, **kwargs):
    # Chain to final task
    async_task(
        'myapp.tasks.load_data',
        result,
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
    }
)
```

### Pattern 3: Status Updates

```python
def update_job_status(result, job_id=None, **kwargs):
    if job_id:
        Job.objects.filter(id=job_id).update(
            status='completed',
            completed_at=timezone.now(),
            result=result,
        )

def mark_job_failed(task_id, exception_class, job_id=None, **kwargs):
    if job_id:
        Job.objects.filter(id=job_id).update(
            status='failed',
            error=exception_class,
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
    }
)
```

### Pattern 4: Cleanup on Failure

```python
def cleanup_temp_files(task_id, exception_class, temp_dir=None, **kwargs):
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
def record_success_metric(result, metric_name=None, **kwargs):
    if metric_name:
        statsd.increment(f'{metric_name}.success')
        statsd.timing(f'{metric_name}.duration', result.get('duration', 0))

def record_failure_metric(task_id, exception_class, metric_name=None, **kwargs):
    if metric_name:
        statsd.increment(f'{metric_name}.failure')
        statsd.increment(f'{metric_name}.failure.{exception_class}')

async_task(
    'myapp.tasks.api_call',
    url,
    qraft_options={
        'success_hook': 'myapp.hooks.record_success_metric',
        'success_kwargs': {'metric_name': 'api.external'},
        'failure_hook': 'myapp.hooks.record_failure_metric',
        'failure_kwargs': {'metric_name': 'api.external'},
    }
)
```

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

```python
def on_success(result, **kwargs):
    from qraft.models import QraftTask

    # Get task ID from kwargs if passed
    task_id = kwargs.get('task_id')
    if task_id:
        task = QraftTask.objects.get(id=task_id)
        print(f"Task function: {task.func}")
        print(f"Attempts: {task.attempt_count}")
        print(f"Created: {task.created_at}")

async_task(
    'myapp.tasks.process_data',
    data,
    qraft_options={
        'success_hook': 'myapp.hooks.on_success',
        'success_kwargs': {'task_id': '{{TASK_ID}}'},  # Placeholder replaced
    }
)
```

**Note:** Direct task ID access is not currently supported. Use `task_id` parameter for failure hooks.

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
def on_success(result, custom_arg=None, **kwargs):
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
def on_success(result, custom_arg=None, **kwargs):  # Won't match
```

## Related Documentation

- [Getting Started](getting-started.md) - Basic hook usage
- [Configuration](configuration.md) - Hook settings reference
- [Retry Policies](retry.md) - Retry integration with hooks
- [Architecture](architecture.md) - Hook dispatching internals
