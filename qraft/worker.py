"""
Threaded worker implementation for Django-Qraft.

Provides optional multithreaded task execution within Django-Q2 worker processes,
enabling higher per-worker concurrency for I/O-bound workloads.

This module only contains the threading-specific logic. Task execution reuses
Django-Q2's existing utilities and patterns.
"""

import logging
import math
import os
import pydoc
import signal
import threading
import time
import traceback
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import Value
from multiprocessing.process import current_process
from multiprocessing.queues import Queue
from queue import Empty
from threading import Semaphore

# Ensure Django is set up before importing Django-dependent modules
# This is required for macOS "spawn" multiprocessing method
from django.apps.registry import apps
from django.core.exceptions import AppRegistryNotReady

try:
    apps.check_apps_ready()
except AppRegistryNotReady:
    import django

    django.setup()

from django.utils import timezone
from django_q.conf import Conf, error_reporter, setproctitle
from django_q.signals import post_spawn, pre_execute
from django_q.utils import close_old_django_connections, get_func_repr

_logger = logging.getLogger("django-q")

# Timer sentinel values for inter-process communication
TIMER_IDLE = -1  # Worker is idle, no task executing
TIMER_RECYCLE = -2  # Signal sentinel to recycle this worker

# Extra seconds added to task timeout for processing overhead
TIMER_BUFFER = 3

# How long (seconds) to block on task_queue.get() before re-checking
QUEUE_POLL_INTERVAL = 1.0


class _DeadlineRegistry:
    """
    Per-task deadline tracking behind the single shared timer.

    The sentinel guard treats timer.value as a countdown: it decrements
    positive values by GUARD_CYCLE each pass and reincarnates the worker
    when the value reaches exactly 0. With concurrent tasks the timer must
    always hold the countdown of the earliest in-flight deadline, not just
    whichever task happened to arm it first.
    """

    def __init__(self, timer: Value):
        self._timer = timer
        self._lock = threading.Lock()
        self._deadlines: dict[object, float] = {}

    def add(self, key, timeout_seconds: float) -> None:
        with self._lock:
            self._deadlines[key] = time.monotonic() + timeout_seconds + TIMER_BUFFER
            self._publish()

    def remove(self, key) -> None:
        with self._lock:
            self._deadlines.pop(key, None)
            self._publish()

    def _publish(self) -> None:
        with self._timer.get_lock():
            # A recycle signal outranks deadline tracking; the worker is
            # already shutting down and the sentinel must see TIMER_RECYCLE.
            if self._timer.value == TIMER_RECYCLE:
                return
            if not self._deadlines:
                self._timer.value = TIMER_IDLE
            else:
                remaining = min(self._deadlines.values()) - time.monotonic()
                # Integer countdowns land on exactly 0 under the guard's
                # GUARD_CYCLE decrements; 0 means "expired, terminate me".
                self._timer.value = max(math.ceil(remaining), 0)


def _execute_task_in_thread(
    task: dict,
    result_queue: Queue,
    deadlines: _DeadlineRegistry,
    inflight_semaphore: Semaphore,
    timeout: int,
) -> None:
    """
    Execute a single task within a thread.

    This is a thread-safe task execution wrapper that:
    - Manages database connections (close before/after)
    - Handles result queuing
    - Releases the inflight semaphore on completion

    Unlike Django-Q2's worker which uses signal-based timeouts, threaded
    execution relies on process-level timeout enforcement by the sentinel.

    Args:
        task: Task dictionary containing func, args, kwargs, etc.
        result_queue: Queue to put results for the monitor process.
        deadlines: Registry keeping the shared timer on the earliest deadline.
        inflight_semaphore: Semaphore to release when task completes.
        timeout: Task timeout in seconds.
    """
    task_key = object()
    try:
        # Close stale connections before task execution
        close_old_django_connections()

        f = task["func"]
        func_name = get_func_repr(f)
        task_name = task["name"]

        _logger.info(
            "Thread executing task %s '%s'%s",
            task_name,
            func_name,
            f" [{task['group']}]" if "group" in task else "",
        )

        # Resolve function if it's a string path
        if not callable(f):
            f = pydoc.locate(f)

        # Signal pre-execution (same as Django-Q2)
        pre_execute.send(sender="django_q", func=f, task=task)

        # Register this task's deadline so the shared timer always reflects
        # the earliest one among in-flight tasks
        task_timeout = task.pop("timeout", timeout) or timeout
        if task_timeout and task_timeout > 0:
            deadlines.add(task_key, task_timeout)

        # Execute the task (mirrors Django-Q2 worker execution)
        try:
            if f is None:
                raise ValueError(f"Function {task['func']} is not defined")
            res = f(*task["args"], **task["kwargs"])
            result = (res, True)
        except Exception as e:
            result = (f"{e} : {traceback.format_exc()}", False)
            if error_reporter:
                error_reporter.report()

        # Process result (same structure as Django-Q2)
        task["result"] = result[0]
        task["success"] = result[1]
        task["stopped"] = timezone.now()
        result_queue.put(task)

        _logger.debug("Task %s completed with success=%s", task_name, result[1])

    except Exception as e:
        # Catch-all for unexpected errors in the execution wrapper
        _logger.error(
            "Unexpected error in task execution wrapper: %s", e, exc_info=True
        )
        try:
            task["result"] = f"Execution wrapper error: {e}"
            task["success"] = False
            task["stopped"] = timezone.now()
            result_queue.put(task)
        except Exception:
            _logger.error("Failed to queue error result for task")

    finally:
        # Close the execution lease: post_execute fires in the monitor process,
        # so the worker has to stop its own heartbeat thread.
        from . import context, tracing
        from .lease import stop_heartbeat

        stop_heartbeat(task.get("id"))
        # A pool thread outlives the task it ran, so the attempt's ids and its
        # span are cleared here rather than left for the next task to inherit.
        context.clear_context()
        tracing.detach_current()
        # Always close connections after task execution
        close_old_django_connections()
        # Drop this task's deadline; the timer re-arms to the earliest
        # remaining one, or TIMER_IDLE when nothing is in flight
        deadlines.remove(task_key)
        # Release semaphore to allow next task
        inflight_semaphore.release()


def threaded_worker(
    task_queue: Queue,
    result_queue: Queue,
    timer: Value,
    timeout: int,
    threads: int,
    max_inflight: int,
    grace_period: float,
) -> None:
    """
    Threaded worker that pulls tasks from the queue and executes them
    in a thread pool.

    This function replaces Django-Q2's standard worker when threading is enabled.
    It maintains the same interface (task_queue, result_queue, timer, timeout)
    with additional threading parameters.

    The main loop structure mirrors Django-Q2's worker:
    - Pull tasks until "STOP" poison pill
    - Track task count for recycling
    - Use timer for sentinel communication

    Args:
        task_queue: Multiprocessing queue to pull tasks from.
        result_queue: Multiprocessing queue to push results to.
        timer: Shared Value for timeout tracking by the sentinel.
        timeout: Default task timeout in seconds.
        threads: Number of threads in the pool.
        max_inflight: Maximum concurrent tasks (controls backpressure).
        grace_period: Seconds to wait for in-flight tasks on shutdown.
    """
    # Ignore SIGINT in worker process - let the sentinel handle shutdown
    # by sending "STOP" poison pill. This prevents KeyboardInterrupt from
    # interrupting queue.get() which can leave the queue in a bad state.
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    signal.signal(signal.SIGTERM, signal.SIG_DFL)

    proc_name = current_process().name
    _logger.info(
        "%s ready for work at %s (threaded: %d threads, max_inflight: %d)",
        proc_name,
        current_process().pid,
        threads,
        max_inflight,
    )

    # Signal that worker has spawned (same as Django-Q2)
    post_spawn.send(sender="django_q", proc_name=proc_name)

    if setproctitle:
        setproctitle.setproctitle(f"qcluster {proc_name} idle (threaded)")

    # Create thread pool and inflight semaphore for backpressure
    executor = ThreadPoolExecutor(
        max_workers=threads, thread_name_prefix="qraft_worker"
    )
    inflight_semaphore = Semaphore(max_inflight)
    deadlines = _DeadlineRegistry(timer)

    task_count = 0
    if timeout is None:
        timeout = TIMER_IDLE

    # Main loop: pull tasks and submit to thread pool
    # Use timeout-based get to allow periodic checks and clean shutdown.
    # The sentinel sends "STOP" poison pill to signal shutdown.
    should_stop = False
    while not should_stop:
        try:
            # Use timeout to allow periodic wake-up for shutdown checks
            task = task_queue.get(timeout=QUEUE_POLL_INTERVAL)
        except Empty:
            # No task available, continue loop to check again
            continue

        # Check for poison pill
        if task == "STOP":
            break

        task_count += 1

        # Update process title (similar to Django-Q2)
        func_name = get_func_repr(task["func"])
        task_name = task["name"]

        if setproctitle:
            proc_title = f"qcluster {proc_name} processing {task_name} '{func_name}'"
            if "group" in task:
                proc_title += f" [{task['group']}]"
            proc_title += f" (threaded: {threads}t)"
            setproctitle.setproctitle(proc_title)

        # Acquire semaphore (blocks if max_inflight reached)
        # This provides backpressure when the thread pool is saturated
        inflight_semaphore.acquire()

        # Submit task to thread pool
        executor.submit(
            _execute_task_in_thread,
            task,
            result_queue,
            deadlines,
            inflight_semaphore,
            timeout,
        )

        # Check for recycle condition (same as Django-Q2)
        if task_count >= Conf.RECYCLE:
            _logger.info(
                "%s reached recycle limit (%d tasks submitted), initiating shutdown",
                proc_name,
                task_count,
            )
            with timer.get_lock():
                timer.value = TIMER_RECYCLE
            should_stop = True

    # Graceful shutdown: wait for in-flight tasks to complete
    _logger.info(
        "%s stopping, waiting up to %.1fs for in-flight tasks",
        proc_name,
        grace_period,
    )

    if setproctitle:
        setproctitle.setproctitle(f"qcluster {proc_name} stopping (threaded)")

    # ThreadPoolExecutor.shutdown() takes no timeout, so wait on it from a
    # side thread to keep the grace period enforceable.
    shutdown_complete = threading.Event()

    def _shutdown_executor():
        executor.shutdown(wait=True, cancel_futures=False)
        shutdown_complete.set()

    threading.Thread(target=_shutdown_executor, daemon=True).start()

    if not shutdown_complete.wait(timeout=grace_period):
        _logger.warning(
            "%s executor shutdown timed out after %.1fs, forcing exit",
            proc_name,
            grace_period,
        )
        # Executor threads are non-daemon: returning here would leave the
        # interpreter joining them forever in threading._shutdown, and the
        # sentinel's stop() spins until this process dies. Flush the result
        # queue's feeder thread so already-finished results reach the
        # monitor, then hard-exit past the stuck threads.
        if isinstance(result_queue, Queue):
            # In-process tests drive the loop with queue.Queue, which has no
            # feeder thread to flush.
            result_queue.close()
            result_queue.join_thread()
        os._exit(1)

    with timer.get_lock():
        if timer.value > 0:
            timer.value = TIMER_IDLE

    _logger.info("%s stopped doing work", proc_name)
