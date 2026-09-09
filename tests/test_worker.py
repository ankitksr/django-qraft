"""Tests for qraft.worker module (threaded_worker, _execute_task_in_thread)."""

import os
import threading
import time
from multiprocessing import Queue as MPQueue
from multiprocessing import Value
from queue import Queue as ThreadQueue
from threading import Semaphore
from unittest.mock import patch

import pytest

from qraft.worker import (
    TIMER_IDLE,
    TIMER_RECYCLE,
    _DeadlineRegistry,
    _execute_task_in_thread,
    threaded_worker,
)


def fast_task():
    # Module-level so the result dict pickles through a multiprocessing queue.
    return "fast"


def make_task(**overrides):
    task = {
        "name": "task-1",
        "func": lambda: "ok",
        "args": (),
        "kwargs": {},
    }
    task.update(overrides)
    return task


@pytest.mark.django_db
class TestExecuteTaskInThread:
    """Tests for _execute_task_in_thread.

    django_db is required because the function closes stale Django DB
    connections as part of its real execution path.
    """

    def test_success_puts_result_and_releases_semaphore(self):
        task = make_task(func=lambda x, y: x + y, args=(1, 2), kwargs={})
        result_queue = ThreadQueue()
        timer = Value("f", TIMER_IDLE)
        deadlines = _DeadlineRegistry(timer)
        semaphore = Semaphore(1)
        semaphore.acquire()

        _execute_task_in_thread(task, result_queue, deadlines, semaphore, timeout=30)

        result_task = result_queue.get_nowait()
        assert result_task["result"] == 3
        assert result_task["success"] is True
        # Semaphore released so it can be acquired again without blocking.
        assert semaphore.acquire(blocking=False) is True
        # Timer reset to idle once the task (executed synchronously here) finishes.
        assert timer.value == TIMER_IDLE

    def test_finished_task_leaves_no_context_for_the_next_one(self):
        """
        A pool thread runs task after task. Whatever the last one bound - the
        ids the logging filter stamps, the span the enqueue would parent to -
        must be gone before the next one starts.
        """
        from qraft import context, tracing

        def bind_something():
            context._bound_context.set({"task_id": "leaked"})
            tracing._thread_state.token = "attached"

        task = make_task(func=bind_something)
        semaphore = Semaphore(1)
        semaphore.acquire()

        with patch.object(tracing, "trace", object()), patch.object(
            tracing.otel_context, "detach"
        ) as detach:
            _execute_task_in_thread(
                task,
                ThreadQueue(),
                _DeadlineRegistry(Value("f", TIMER_IDLE)),
                semaphore,
                timeout=30,
            )

        assert context.current_context()["task_id"] is None
        assert context._current_q2_task_id.get() is None
        detach.assert_called_once_with("attached")
        assert tracing._thread_state.token is None

    def test_exception_reports_failure_without_raising(self):
        def boom():
            raise ValueError("kaboom")

        task = make_task(func=boom)
        result_queue = ThreadQueue()
        timer = Value("f", TIMER_IDLE)
        deadlines = _DeadlineRegistry(timer)
        semaphore = Semaphore(1)
        semaphore.acquire()

        _execute_task_in_thread(task, result_queue, deadlines, semaphore, timeout=30)

        result_task = result_queue.get_nowait()
        assert result_task["success"] is False
        assert "ValueError" in result_task["result"]
        assert semaphore.acquire(blocking=False) is True

    def test_unresolvable_string_func_reports_failure(self):
        task = make_task(func="not.a.real.module.func")
        result_queue = ThreadQueue()
        timer = Value("f", TIMER_IDLE)
        deadlines = _DeadlineRegistry(timer)
        semaphore = Semaphore(1)
        semaphore.acquire()

        with patch("qraft.worker.pydoc.locate", return_value=None):
            _execute_task_in_thread(
                task, result_queue, deadlines, semaphore, timeout=30
            )

        result_task = result_queue.get_nowait()
        assert result_task["success"] is False
        assert "is not defined" in result_task["result"]
        # Semaphore must still be released even though the function never ran.
        assert semaphore.acquire(blocking=False) is True


@pytest.mark.django_db
class TestTimerDeadlines:
    """The shared timer must track the earliest deadline among in-flight tasks."""

    def _spawn(self, task, deadlines, semaphore, timeout=30):
        semaphore.acquire()
        thread = threading.Thread(
            target=_execute_task_in_thread,
            args=(task, ThreadQueue(), deadlines, semaphore, timeout),
        )
        thread.start()
        return thread

    def _wait_for(self, condition, timeout=5.0):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if condition():
                return True
            time.sleep(0.01)
        return False

    def test_timer_stays_armed_for_hanging_sibling(self):
        timer = Value("f", TIMER_IDLE)
        deadlines = _DeadlineRegistry(timer)
        semaphore = Semaphore(2)
        hang_gate = threading.Event()
        short_gate = threading.Event()

        hang_task = make_task(name="hang", func=lambda: hang_gate.wait(5), timeout=50)
        short_task = make_task(
            name="short", func=lambda: short_gate.wait(5), timeout=10
        )

        hang_thread = self._spawn(hang_task, deadlines, semaphore)
        assert self._wait_for(lambda: timer.value > 0)
        # Armed with the hanging task's countdown (50 + TIMER_BUFFER).
        assert 40 < timer.value <= 53

        short_thread = self._spawn(short_task, deadlines, semaphore)
        # Earliest deadline wins: short task's countdown (10 + TIMER_BUFFER).
        assert self._wait_for(lambda: 0 < timer.value <= 13)

        short_gate.set()
        short_thread.join(timeout=5)
        # Short task finished; timer must re-arm for the hanging sibling,
        # not reset to idle.
        assert timer.value != TIMER_IDLE
        assert 13 < timer.value <= 53

        hang_gate.set()
        hang_thread.join(timeout=5)
        assert timer.value == TIMER_IDLE

    def test_no_timeout_task_leaves_timer_idle(self):
        timer = Value("f", TIMER_IDLE)
        deadlines = _DeadlineRegistry(timer)
        semaphore = Semaphore(1)
        gate = threading.Event()
        started = threading.Event()

        def func():
            started.set()
            gate.wait(5)

        # No per-task timeout and no worker default (None -> TIMER_IDLE).
        task = make_task(func=func)
        thread = self._spawn(task, deadlines, semaphore, timeout=TIMER_IDLE)
        # Arming happens before the function runs, so once it has started
        # the timer state is settled.
        assert started.wait(5)
        assert timer.value == TIMER_IDLE

        gate.set()
        thread.join(timeout=5)
        assert timer.value == TIMER_IDLE


@pytest.mark.django_db
class TestThreadedWorkerLoop:
    """Tests for the threaded_worker main loop using real Queue objects."""

    def _run(self, tasks, threads=2, max_inflight=2, grace_period=0.5, timeout=30):
        task_queue = ThreadQueue()
        result_queue = ThreadQueue()
        timer = Value("f", TIMER_IDLE)
        for t in tasks:
            task_queue.put(t)
        task_queue.put("STOP")

        with (
            patch("qraft.worker.signal.signal"),
            patch("qraft.worker.setproctitle", None),
        ):
            threaded_worker(
                task_queue,
                result_queue,
                timer,
                timeout,
                threads,
                max_inflight,
                grace_period,
            )
        return result_queue, timer

    def test_processes_task_and_stops_on_poison_pill(self):
        task = make_task(func=lambda: "done")
        result_queue, timer = self._run([task])

        result_task = result_queue.get(timeout=1)
        assert result_task["success"] is True
        assert result_task["result"] == "done"
        # Loop must exit and reset the timer to idle after graceful shutdown.
        assert timer.value == TIMER_IDLE

    def test_recycle_limit_triggers_shutdown(self):
        tasks = [make_task(name=f"task-{i}") for i in range(3)]
        with patch("qraft.worker.Conf") as mock_conf:
            mock_conf.RECYCLE = 2
            task_queue = ThreadQueue()
            result_queue = ThreadQueue()
            timer = Value("f", TIMER_IDLE)
            for t in tasks:
                task_queue.put(t)
            # No STOP needed: recycle limit should stop the loop after 2 tasks.

            with (
                patch("qraft.worker.signal.signal"),
                patch("qraft.worker.setproctitle", None),
            ):
                threaded_worker(
                    task_queue,
                    result_queue,
                    timer,
                    30,
                    threads=2,
                    max_inflight=2,
                    grace_period=0.5,
                )

        # Only the first two tasks were submitted before recycling; the third
        # task must remain unqueued, and the recycle signal must survive
        # in-flight tasks finishing during the graceful shutdown.
        assert timer.value == TIMER_RECYCLE
        assert task_queue.qsize() == 1
        results = []
        while not result_queue.empty():
            results.append(result_queue.get_nowait())
        assert len(results) == 2

    def test_semaphore_backpressure_limits_concurrency(self):
        """With max_inflight=1, a slow task must block submission of the next."""
        started = []
        release_event = ThreadQueue()

        def slow_task():
            started.append(time.monotonic())
            release_event.get(timeout=1)
            return "done"

        tasks = [
            make_task(name="slow", func=slow_task),
            make_task(name="fast", func=lambda: "fast"),
        ]
        task_queue = ThreadQueue()
        result_queue = ThreadQueue()
        timer = Value("f", TIMER_IDLE)
        task_queue.put(tasks[0])
        task_queue.put(tasks[1])
        task_queue.put("STOP")

        def unblock_after_delay():
            time.sleep(0.1)
            release_event.put(None)

        unblocker = threading.Thread(target=unblock_after_delay)
        unblocker.start()

        with (
            patch("qraft.worker.signal.signal"),
            patch("qraft.worker.setproctitle", None),
        ):
            threaded_worker(
                task_queue,
                result_queue,
                timer,
                30,
                threads=2,
                max_inflight=1,
                grace_period=1.0,
            )
        unblocker.join()

        results = {}
        while not result_queue.empty():
            r = result_queue.get_nowait()
            results[r["name"]] = r
        assert results["slow"]["success"] is True
        assert results["fast"]["success"] is True

    def test_graceful_shutdown_waits_for_inflight_task(self):
        """A running task must complete before threaded_worker returns."""
        completed = []

        def slow_task():
            time.sleep(0.15)
            completed.append(True)
            return "done"

        task_queue = ThreadQueue()
        result_queue = ThreadQueue()
        timer = Value("f", TIMER_IDLE)
        task_queue.put(make_task(func=slow_task))
        task_queue.put("STOP")

        with (
            patch("qraft.worker.signal.signal"),
            patch("qraft.worker.setproctitle", None),
            patch("qraft.worker.os._exit") as mock_exit,
        ):
            threaded_worker(
                task_queue,
                result_queue,
                timer,
                30,
                threads=1,
                max_inflight=1,
                grace_period=1.0,
            )

        assert completed == [True]
        assert result_queue.get_nowait()["success"] is True
        # Tasks finished inside the grace period: no forced exit.
        mock_exit.assert_not_called()

    def test_grace_period_timeout_forces_exit(self):
        """If a task outlives the grace period, the worker must hard-exit:
        executor threads are non-daemon, so a plain return would hang the
        process (and the sentinel's stop()) forever."""
        task_queue = ThreadQueue()
        result_queue = ThreadQueue()
        timer = Value("f", TIMER_IDLE)
        task_queue.put(make_task(func=lambda: time.sleep(0.5) or "done"))
        task_queue.put("STOP")

        start = time.monotonic()
        with (
            patch("qraft.worker.signal.signal"),
            patch("qraft.worker.setproctitle", None),
            patch("qraft.worker.os._exit") as mock_exit,
        ):
            threaded_worker(
                task_queue,
                result_queue,
                timer,
                30,
                threads=1,
                max_inflight=1,
                grace_period=0.1,
            )
        elapsed = time.monotonic() - start

        # Must exit close to the grace period, not wait for the full 0.5s task.
        assert elapsed < 0.4
        mock_exit.assert_called_once_with(1)

    def test_forced_exit_flushes_finished_results(self):
        """Results already produced must survive the forced exit: the
        multiprocessing queue's feeder thread is flushed before os._exit."""
        hang_gate = threading.Event()
        task_queue = ThreadQueue()
        result_queue = MPQueue()
        timer = Value("f", TIMER_IDLE)
        task_queue.put(make_task(name="fast", func=fast_task))
        task_queue.put(make_task(name="hang", func=lambda: hang_gate.wait(10)))
        task_queue.put("STOP")

        # close() also closes this process's reader handle, so keep a
        # duplicate to prove the flushed data reached the pipe.
        from multiprocessing.connection import Connection

        reader = Connection(os.dup(result_queue._reader.fileno()))
        exit_calls = []

        try:
            with (
                patch("qraft.worker.signal.signal"),
                patch("qraft.worker.setproctitle", None),
                patch.object(
                    result_queue, "close", wraps=result_queue.close
                ) as mock_close,
                patch.object(
                    result_queue, "join_thread", wraps=result_queue.join_thread
                ) as mock_join,
                patch(
                    "qraft.worker.os._exit",
                    side_effect=lambda code: exit_calls.append(
                        (code, mock_close.called, mock_join.called)
                    ),
                ),
            ):
                threaded_worker(
                    task_queue,
                    result_queue,
                    timer,
                    30,
                    threads=2,
                    max_inflight=2,
                    grace_period=0.5,
                )
        finally:
            hang_gate.set()

        # os._exit(1) fired with the queue already closed and flushed.
        assert exit_calls == [(1, True, True)]
        # The fast task's result reached the pipe before the exit.
        assert reader.poll(1)
        result = reader.recv()
        reader.close()
        assert result["name"] == "fast"
        assert result["success"] is True
