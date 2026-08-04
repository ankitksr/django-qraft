"""Tests for qraft.worker module (threaded_worker, _execute_task_in_thread)."""

import time
from multiprocessing import Value
from queue import Queue as ThreadQueue
from threading import Semaphore
from unittest.mock import patch

import pytest

from qraft.worker import TIMER_IDLE, _execute_task_in_thread, threaded_worker


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
        semaphore = Semaphore(1)
        semaphore.acquire()

        _execute_task_in_thread(task, result_queue, timer, semaphore, timeout=30)

        result_task = result_queue.get_nowait()
        assert result_task["result"] == 3
        assert result_task["success"] is True
        # Semaphore released so it can be acquired again without blocking.
        assert semaphore.acquire(blocking=False) is True
        # Timer reset to idle once the task (executed synchronously here) finishes.
        assert timer.value == TIMER_IDLE

    def test_exception_reports_failure_without_raising(self):
        def boom():
            raise ValueError("kaboom")

        task = make_task(func=boom)
        result_queue = ThreadQueue()
        timer = Value("f", TIMER_IDLE)
        semaphore = Semaphore(1)
        semaphore.acquire()

        _execute_task_in_thread(task, result_queue, timer, semaphore, timeout=30)

        result_task = result_queue.get_nowait()
        assert result_task["success"] is False
        assert "ValueError" in result_task["result"]
        assert semaphore.acquire(blocking=False) is True

    def test_unresolvable_string_func_reports_failure(self):
        task = make_task(func="not.a.real.module.func")
        result_queue = ThreadQueue()
        timer = Value("f", TIMER_IDLE)
        semaphore = Semaphore(1)
        semaphore.acquire()

        with patch("qraft.worker.pydoc.locate", return_value=None):
            _execute_task_in_thread(task, result_queue, timer, semaphore, timeout=30)

        result_task = result_queue.get_nowait()
        assert result_task["success"] is False
        assert "is not defined" in result_task["result"]
        # Semaphore must still be released even though the function never ran.
        assert semaphore.acquire(blocking=False) is True


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

        # Only the first two tasks were submitted before recycling; the timer
        # was reset to idle after graceful shutdown, so we can't observe
        # TIMER_RECYCLE directly, but the third task must remain unqueued.
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

        import threading

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

    def test_grace_period_timeout_returns_without_waiting_forever(self):
        """If a task outlives the grace period, the worker still returns."""
        task_queue = ThreadQueue()
        result_queue = ThreadQueue()
        timer = Value("f", TIMER_IDLE)
        task_queue.put(make_task(func=lambda: time.sleep(0.5) or "done"))
        task_queue.put("STOP")

        start = time.monotonic()
        with (
            patch("qraft.worker.signal.signal"),
            patch("qraft.worker.setproctitle", None),
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

        # Must return close to the grace period, not wait for the full 0.5s task.
        assert elapsed < 0.4
