# Getting Started with Django-Qraft

This guide will help you install and start using Django-Qraft in your Django project.

## Installation

### Using pip

```bash
pip install django-qraft
```

### Using uv (recommended)

```bash
uv pip install django-qraft
```

## Django Configuration

### 1. Add to INSTALLED_APPS

Django-Qraft requires both `django_q` and `qraft` in your `INSTALLED_APPS`:

```python
# settings.py
INSTALLED_APPS = [
    # ... your other apps
    'django_q',
    'qraft',
]
```

### 2. Configure the Cluster

Django-Qraft uses `QRAFT_CLUSTER` settings (with fallback to Django-Q2's `Q_CLUSTER` for compatibility):

```python
# settings.py
QRAFT_CLUSTER = {
    # Standard Django-Q2 settings
    "name": "default",
    "workers": 4,
    "timeout": 60,
    "retry": 90,  # Django-Q2 broker-level retry timeout
    "orm": "default",  # Use Django ORM as broker

    # Qraft-specific settings
    "threads": 1,           # Threads per worker (1=disabled, >1 enables)
    "max_inflight": None,   # Max concurrent tasks (default: threads * 2)
    "grace_period": 30.0,   # Seconds to wait on shutdown
    "sync_hooks": False,    # Run hooks async (default) or sync
    "retry_defaults": {
        "max_attempts": 3,
        "delay": 30.0,
        "backoff": "exponential",
        "jitter": True,
    },
}
```

**Minimal configuration:**

```python
QRAFT_CLUSTER = {
    "workers": 4,
    "timeout": 60,
    "orm": "default",
}
```

### 3. Run Migrations

```bash
python manage.py migrate
```

This creates the Qraft tables: tasks and their attempts, hook dispatch tracking,
the workflow models (chain, iter, batch), graphs and their nodes, and rate buckets
for throttling. See [Architecture](architecture.md) for the full schema.

## Starting the Cluster

### Basic Usage

```bash
python manage.py qraftcluster
```

This starts the task queue cluster with your configured settings.

### With Alternative Configuration

For multi-queue setups (see [Configuration](configuration.md) for ALT_CLUSTERS):

```bash
# Using command-line flag
python manage.py qraftcluster --name io-workers

# Using environment variable
Q_CLUSTER_NAME=io-workers python manage.py qraftcluster
```

### Run Once (Testing)

For testing, you can process one task and exit:

```bash
python manage.py qraftcluster --run-once
```

## Creating Your First Task

### Basic Task

Create a simple task function:

```python
# myapp/tasks.py
import time

def hello_task(name):
    time.sleep(2)  # Simulate work
    return f"Hello, {name}!"
```

Queue it using `async_task`:

```python
from qraft.tasks import async_task

# Queue the task
task_id = async_task('myapp.tasks.hello_task', 'World')
print(f"Queued task: {task_id}")
```

### Task with Hooks

Add success and failure hooks:

```python
# myapp/hooks.py
def on_success(**kwargs):
    print("Task succeeded")

def on_failure(context=None, **kwargs):
    print(f"Task {context['task_id']} failed with {context['exception_class']}")

# Queue with hooks
task_id = async_task(
    'myapp.tasks.hello_task',
    'World',
    qraft_options={
        'success_hook': 'myapp.hooks.on_success',
        'failure_hook': 'myapp.hooks.on_failure',
        'hook_context': True,
    }
)
```

A hook receives only what you configure in `qraft_options` — Qraft injects nothing by
default. See [Hooks Guide](hooks.md) for the full contract, including `hook_context`.

See [Hooks Guide](hooks.md) for detailed hook documentation.

### Task with Retry Policy

Configure retry behavior:

```python
task_id = async_task(
    'myapp.tasks.flaky_api_call',
    'https://api.example.com/data',
    qraft_options={
        'max_attempts': 5,
        'base_delay': 10,
        'backoff_strategy': 'exponential',
        'jitter': True,
        'retry_exceptions': ['RequestException', 'Timeout'],
    }
)
```

See [Retry Policies Guide](retry.md) for detailed retry documentation.

### Task with Both Hooks and Retries

```python
task_id = async_task(
    'myapp.tasks.important_task',
    arg1, arg2,
    qraft_options={
        # Retry configuration
        'max_attempts': 3,
        'base_delay': 30,
        'backoff_strategy': 'exponential',

        # Hook configuration
        'success_hook': 'myapp.hooks.on_success',
        'success_kwargs': {'notify': True},
        'failure_hook': 'myapp.hooks.on_failure',
        'failure_kwargs': {'alert': 'critical'},
    }
)
```

## Checking Task Status

### Using Django ORM

```python
from qraft.models import QraftTask, QraftTaskAttempt

# Get the task
task = QraftTask.objects.get(id=task_id)

# Check status
print(task.status)  # PENDING, RUNNING, SUCCEEDED, FAILED, EXHAUSTED

# Check attempt count
print(f"Attempts: {task.attempt_count}")

# View attempt history
for attempt in task.attempts.all():
    print(f"Attempt {attempt.attempt_number}: {'Success' if attempt.success else 'Failed'}")
    if not attempt.success:
        print(f"  Exception: {attempt.exception_class}")
```

### Using Django-Q2 ORM Models

```python
from django_q.models import Task

# Get the Django-Q2 task record
q2_task = Task.objects.get(id=task_id)

print(q2_task.func)     # Function path
print(q2_task.success)  # True/False
print(q2_task.result)   # Return value
```

## Common Patterns

### Pattern 1: Simple Background Task

```python
from qraft.tasks import async_task

def send_email(to, subject, body):
    # ... email sending logic
    pass

# Queue it
async_task('myapp.tasks.send_email',
           'user@example.com',
           'Welcome!',
           'Thanks for signing up')
```

### Pattern 2: Scheduled Task

`async_task` has no `schedule_type` or `next_run` parameter — passing them falls through
to `**kwargs` and they land as ordinary keyword arguments on the target function, not on
a scheduler. For a one-off delayed run, use Django-Q2's own `schedule()` directly. It
creates a `Schedule` row outside Qraft, so the resulting task carries no `QraftTask`,
hooks, or retry policy:

```python
from datetime import datetime, timedelta
from django_q.tasks import schedule
from django_q.models import Schedule

run_at = datetime.now() + timedelta(hours=1)
schedule(
    'myapp.tasks.cleanup_old_data',
    schedule_type=Schedule.ONCE,
    next_run=run_at,
)
```

If the task needs Qraft hooks or retries, have the scheduled function call `async_task`
itself rather than running the work directly.

### Pattern 3: Task Chain with Hooks

```python
# Task 1: Extract data
async_task(
    'myapp.tasks.extract_data',
    source='api',
    qraft_options={
        'success_hook': 'myapp.tasks.transform_data',
        'success_kwargs': {'pipeline_id': 'pipe-123'},
        'hook_context': True,
    }
)

# Task 2 (transform_data) is called as a hook when extract_data succeeds. It does not
# receive extract_data's return value as an argument — it reads context['result_ref']
# to fetch it. See "Task Chaining" in the Hooks Guide for the full example, or use
# QraftChain (workflows.md) for multi-step pipelines like this one.
```

### Pattern 4: Retry on Specific Errors

```python
async_task(
    'myapp.tasks.external_api_call',
    url='https://api.example.com',
    qraft_options={
        'max_attempts': 5,
        'base_delay': 10,
        'backoff_strategy': 'exponential',
        # Only retry on network/timeout errors
        'retry_exceptions': ['ConnectionError', 'Timeout', 'RequestException'],
        # Don't retry on authentication errors
        'skip_exceptions': ['AuthenticationError', 'PermissionDenied'],
    }
)
```

## Running the Demo

Django-Qraft includes a demo application to help you understand features:

```bash
cd demo

# Run migrations
python manage.py migrate

# Terminal 1: Start the cluster
python manage.py qraftcluster

# Terminal 2: Run demo scenarios
python manage.py demo hooks -n 5        # Dual-phase hooks
python manage.py demo retry --fail-times 2  # Retry policies
python manage.py demo perf -n 20        # Performance comparison
```

See [demo/README.md](../demo/README.md) for detailed demo documentation.

## What's Next?

Now that you have Django-Qraft running:

1. **Learn about configuration options** → [Configuration Guide](configuration.md)
2. **Understand the hook system** → [Hooks Guide](hooks.md)
3. **Configure retry policies** → [Retry Policies Guide](retry.md)
4. **Optimize for I/O workloads** → [Multithreaded Workers Guide](threading.md)
5. **Understand the architecture** → [Architecture Guide](architecture.md)

## Troubleshooting

### Tasks Not Processing

**Check the cluster is running:**
```bash
ps aux | grep qraftcluster
```

**Check for errors in cluster logs:**
```bash
python manage.py qraftcluster  # Watch the output
```

### Import Errors

**Ensure the function path is correct:**
```python
# Correct
async_task('myapp.tasks.my_function')

# Wrong - missing module
async_task('my_function')
```

**Ensure the module is importable:**
```bash
python manage.py shell
>>> from myapp.tasks import my_function  # Should not raise ImportError
```

### Database Errors

**Run migrations:**
```bash
python manage.py migrate
```

**Check database configuration:**
```python
# settings.py
QRAFT_CLUSTER = {
    "orm": "default",  # Must match a key in DATABASES
}
```

### Tasks Stuck in RUNNING

**Check timeout configuration:**
```python
QRAFT_CLUSTER = {
    "timeout": 60,  # Increase if tasks legitimately take longer
}
```

**Check worker processes:**
```bash
ps aux | grep qraftcluster
# You should see worker processes
```

## Additional Resources

- [Django-Q2 Documentation](https://django-q2.readthedocs.io/) - Base library
- [Configuration Reference](configuration.md) - All settings explained
- [Architecture Guide](architecture.md) - How it works internally
