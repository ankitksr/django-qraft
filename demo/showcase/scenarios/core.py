"""Core scenarios: hooks, retry policies, exception filtering, threading."""

import time

from qraft.models import TaskStatus
from qraft.retry import RetryPolicy
from qraft.tasks import async_task
from showcase import probe
from showcase.harness import scenario
from showcase.models import Event

# Qraft owns scheduling: a delayed attempt is a row on qraft's own table with
# an exact `not_before`, and qraft's dispatcher enqueues it when it comes due.
# Both the delay chosen and the delay served are therefore measurable, and
# they should agree - which is the point. Under django-q2's Schedule model
# they could not: its scheduler runs on a hardcoded 30-second cycle
# (`Sentinel.guard`, `counter >= 30`), so every sub-30s backoff was rounded up
# on delivery no matter what qraft wrote.
DISPATCH_NOTE = (
    "'chose' is the not_before qraft wrote on the scheduled attempt; "
    "'served' is the gap the worker actually measured, from one attempt "
    "completing to the next starting. Delivery overhead is the dispatcher "
    "poll plus broker pickup."
)

# Slack allowed between the delay qraft chose and the delay a worker served.
DELIVERY_SLACK = 3.0


def _hooks(run: str, label: str) -> dict:
    return {
        "success_hook": "showcase.hooks.on_success",
        "success_args": (run, label),
        "failure_hook": "showcase.hooks.on_failure",
        "failure_args": (run, label),
    }


@scenario(
    "core.hooks",
    group="core",
    title="Dual-phase hooks",
    proves="A success fires only the success hook and a failure only the "
    "failure hook, each with the arguments it was configured with.",
)
def hooks(ctx):
    run = ctx.run
    good = async_task(
        "showcase.tasks.ok_task", run, "good", qraft_options=_hooks(run, "good")
    )
    bad = async_task(
        "showcase.tasks.fail_task",
        run,
        "bad",
        error="PermanentError",
        qraft_options=_hooks(run, "bad"),
    )

    good_task = ctx.wait(
        "success path settles", lambda: probe.settled(good), timeout=60
    )
    bad_task = ctx.wait("failure path settles", lambda: probe.settled(bad), timeout=60)
    if not (good_task and bad_task):
        return

    ctx.equals("success path status", good_task.status, TaskStatus.SUCCEEDED)
    ctx.equals("failure path status", bad_task.status, TaskStatus.FAILED)

    ctx.wait(
        "success hook ran",
        lambda: ctx.count(kind=Event.HOOK, name="success:good"),
        timeout=45,
    )
    ctx.wait(
        "failure hook ran",
        lambda: ctx.count(kind=Event.HOOK, name="failure:bad"),
        timeout=45,
    )

    # The hook arguments carried the label, so an event under the wrong label
    # would mean the wrong hook fired or the arguments were lost.
    ctx.equals(
        "failure hook did not fire on the success path",
        ctx.count(kind=Event.HOOK, name="failure:good"),
        0,
    )
    ctx.equals(
        "success hook did not fire on the failure path",
        ctx.count(kind=Event.HOOK, name="success:bad"),
        0,
    )

    ctx.equals(
        "success dispatch recorded once", probe.hook_dispatches(good_task), ["success"]
    )
    ctx.equals(
        "failure dispatch recorded once", probe.hook_dispatches(bad_task), ["failure"]
    )


@scenario(
    "core.asyncdef",
    group="core",
    title="Coroutine tasks",
    proves="An `async def` task is actually awaited in its worker slot, "
    "retries through the same scheduled-attempt path as a sync task, and "
    "an `async def` success hook is awaited too.",
)
def asyncdef(ctx):
    run = ctx.run
    task_id = async_task(
        "showcase.tasks.async_flaky_task",
        run,
        "coro",
        fail_times=1,
        qraft_options={
            "max_attempts": 3,
            "base_delay": 1.0,
            "backoff_strategy": "fixed",
            "jitter": False,
            "success_hook": "showcase.hooks.on_success_async",
            "success_args": (run, "coro"),
        },
    )

    task = ctx.wait(
        "coroutine task settles", lambda: probe.settled(task_id), timeout=90
    )
    if not task:
        return

    ctx.equals("status", task.status, TaskStatus.SUCCEEDED)
    ctx.equals("attempts", len(probe.attempts(task)), 2)

    # The task only records its Event after a real await, so these rows
    # existing at all means the worker ran the coroutine to completion
    # instead of dropping it unawaited.
    events = ctx.events(kind=Event.TASK, name="coro")
    ctx.equals("both attempts awaited", len(events), 2)
    ctx.check(
        "events recorded post-await",
        all(event.payload.get("awaited") for event in events),
    )

    ctx.wait(
        "async success hook awaited",
        lambda: ctx.count(kind=Event.HOOK, name="success:coro"),
        timeout=45,
    )


@scenario(
    "core.backoff",
    group="core",
    title="Retry backoff: exponential, linear, fixed",
    proves="Each strategy honours max_attempts, and the delay a worker "
    "actually waits matches the strategy - including delays far below "
    "django-q2's 30-second scheduler tick.",
)
def backoff(ctx):
    run = ctx.run
    base, attempts = 2.0, 4
    expected = {
        "exponential": [base, base * 2, base * 4],
        "linear": [base, base * 2, base * 3],
        "fixed": [base, base, base],
    }

    queued = {}
    for strategy in expected:
        queued[strategy] = async_task(
            "showcase.tasks.fail_task",
            run,
            strategy,
            qraft_options={
                "max_attempts": attempts,
                "base_delay": base,
                "backoff_strategy": strategy,
                "jitter": False,
            },
        )

    ctx.note(DISPATCH_NOTE)

    scheduled: dict[str, list[float]] = {name: [] for name in expected}
    for strategy, task_id in queued.items():
        task = ctx.wait(
            f"{strategy}: exhausts",
            lambda tid=task_id: probe.in_status(tid, TaskStatus.EXHAUSTED),
            timeout=180,
        )
        if not task:
            continue

        ctx.equals(
            f"{strategy}: max_attempts honoured", task.attempts.count(), attempts
        )

        # The attempt row carries both numbers, so neither has to be caught in
        # flight: `not_before` is what qraft chose, `date_started` is when a
        # worker picked it up.
        chosen = probe.chosen_delays(task)
        served = probe.gaps(task)
        scheduled[strategy] = chosen

        ctx.equals(f"{strategy}: delays chosen", len(chosen), attempts - 1)
        ctx.equals(f"{strategy}: delays served", len(served), attempts - 1)

        for index, (seen, want) in enumerate(zip(chosen, expected[strategy]), start=1):
            ctx.between(
                f"{strategy}: delay {index} chosen", seen, want - 0.5, want + 0.5
            )

        # The assertion this scenario exists for. Before qraft owned
        # scheduling these all arrived ~30s apart regardless of what was
        # chosen, so only the ETA could be checked.
        for index, (seen, want) in enumerate(zip(served, expected[strategy]), start=1):
            ctx.between(
                f"{strategy}: delay {index} served",
                seen,
                want - 0.5,
                want + DELIVERY_SLACK,
            )

        if served:
            ctx.check(
                f"{strategy}: a 2s backoff arrives in seconds, not at a 30s tick",
                served[0] < 10.0,
                f"first retry served after {served[0]:.1f}s",
            )
            ctx.note(
                f"{strategy}: chose {[round(v, 1) for v in chosen]}s, "
                f"served {[round(v, 1) for v in served]}s"
            )

    # Shape, independent of the absolute numbers: the three strategies must
    # not be interchangeable.
    exponential, linear, fixed = (
        scheduled[name] for name in ("exponential", "linear", "fixed")
    )
    if all(len(values) == attempts - 1 for values in (exponential, linear, fixed)):
        ctx.check(
            "exponential doubles",
            all(
                abs(later / earlier - 2) < 0.35
                for earlier, later in zip(exponential, exponential[1:])
            ),
            f"{[round(v, 1) for v in exponential]}",
        )
        steps = [later - earlier for earlier, later in zip(linear, linear[1:])]
        ctx.check(
            "linear steps by a constant",
            max(steps) - min(steps) < 1.0,
            f"{[round(v, 1) for v in linear]} (steps {[round(v, 1) for v in steps]})",
        )
        ctx.check(
            "fixed does not grow",
            max(fixed) - min(fixed) < 1.0,
            f"{[round(v, 1) for v in fixed]}",
        )


@scenario(
    "core.jitter",
    group="core",
    title="Retry jitter",
    proves="Jitter spreads the delay inside the configured fraction, and "
    "turning it off makes the delay constant.",
)
def jitter(ctx):
    # An in-process property check on the policy itself. Jitter is randomness,
    # and a handful of end-to-end retries cannot show a distribution; this
    # reads the same code path the scheduler calls.
    ctx.note("In-process check of RetryPolicy.calculate_delay, not an end-to-end run.")

    jittered = RetryPolicy(
        max_attempts=3,
        base_delay=10.0,
        backoff_strategy="fixed",
        jitter=True,
        jitter_max=0.5,
    )
    samples = [jittered.calculate_delay(1) for _ in range(300)]
    ctx.check(
        "jittered delays vary",
        len(set(samples)) > 1,
        f"{len(set(samples))} distinct values across 300 samples",
    )
    ctx.check(
        "jittered delays stay inside +/-50%",
        all(5 <= value <= 15 for value in samples),
        f"observed {min(samples)}..{max(samples)}",
    )

    steady = RetryPolicy(
        max_attempts=3, base_delay=10.0, backoff_strategy="fixed", jitter=False
    )
    ctx.equals(
        "jitter off gives one delay",
        sorted({steady.calculate_delay(1) for _ in range(50)}),
        [10],
    )


@scenario(
    "core.exception-filter",
    group="core",
    title="retry_exceptions and skip_exceptions",
    proves="An exception outside the allow-list, or inside the deny-list, "
    "stops the retry series that an identical policy otherwise runs.",
)
def exception_filter(ctx):
    run = ctx.run
    policy = {"max_attempts": 3, "base_delay": 1.0, "jitter": False}

    not_allowed = async_task(
        "showcase.tasks.fail_task",
        run,
        "not-allowed",
        error="PermanentError",
        qraft_options={**policy, "retry_exceptions": ["TransientError"]},
    )
    denied = async_task(
        "showcase.tasks.fail_task",
        run,
        "denied",
        error="PermanentError",
        qraft_options={**policy, "skip_exceptions": ["PermanentError"]},
    )
    allowed = async_task(
        "showcase.tasks.fail_task",
        run,
        "allowed",
        error="TransientError",
        qraft_options={**policy, "retry_exceptions": ["TransientError"]},
    )

    cases = [
        ("outside retry_exceptions", not_allowed, 1),
        ("inside skip_exceptions", denied, 1),
        ("inside retry_exceptions", allowed, 3),
    ]
    for name, task_id, want_attempts in cases:
        task = ctx.wait(
            f"{name}: exhausts",
            lambda tid=task_id: probe.in_status(tid, TaskStatus.EXHAUSTED),
            timeout=90,
        )
        if task:
            ctx.equals(f"{name}: attempts", task.attempts.count(), want_attempts)

    ctx.note(
        "All three share one policy; only the exception lists differ, so the "
        "attempt counts isolate the filtering."
    )


@scenario(
    "core.rate-limit-retry",
    group="core",
    title="Rate-limit aware retry",
    proves="A provider Retry-After hint overrides the configured backoff, "
    "and rate_limit_max_delay caps a hint that is too large.",
)
def rate_limit_retry(ctx):
    run = ctx.run

    # base_delay 30s exponential: if the hint were ignored, attempt 2 would be
    # half a minute away instead of six seconds.
    honoured = async_task(
        "showcase.tasks.rate_limited_task",
        run,
        "hinted",
        retry_after=6,
        fail_times=1,
        qraft_options={
            "max_attempts": 3,
            "base_delay": 30.0,
            "backoff_strategy": "exponential",
            "jitter": False,
        },
    )
    capped = async_task(
        "showcase.tasks.rate_limited_task",
        run,
        "capped",
        retry_after=600,
        fail_times=1,
        qraft_options={
            "max_attempts": 3,
            "base_delay": 30.0,
            "jitter": False,
            "rate_limit_max_delay": 5.0,
        },
    )

    ctx.note(DISPATCH_NOTE)
    cases = {"hinted": honoured, "capped": capped}

    for name, task_id in cases.items():
        task = ctx.wait(
            f"{name}: retry succeeds",
            lambda tid=task_id: probe.in_status(tid, TaskStatus.SUCCEEDED),
            timeout=120,
        )
        if not task:
            continue
        ctx.equals(f"{name}: two attempts", task.attempts.count(), 2)

        chosen = probe.chosen_delays(task)
        served = probe.gaps(task)
        if not ctx.check(f"{name}: a retry delay was recorded", bool(chosen)):
            continue

        if name == "hinted":
            # 6s hint plus up to 10% jitter, truncated to whole seconds.
            # Without the hint this would be the configured 30s.
            ctx.between(
                "hinted: the 6s hint wins over the 30s backoff", chosen[0], 5.0, 9.0
            )
        else:
            ctx.between(
                "capped: a 600s hint is clamped to the 5s cap", chosen[0], 4.0, 8.0
            )

        if served:
            ctx.between(
                f"{name}: the chosen delay is the delay served",
                served[0],
                chosen[0] - 0.5,
                chosen[0] + DELIVERY_SLACK,
            )


@scenario(
    "core.threading",
    group="core",
    title="Threaded workers versus process workers",
    proves="On an I/O-bound load the threaded cluster clears the same queue "
    "measurably faster than an equally sized process cluster.",
    clusters=("baseline", "threaded"),
)
def threading(ctx):
    # 24 tasks x 3s of I/O-bound sleep: baseline (capacity 2) takes 12 rounds
    # (~36s), threaded (capacity 16 = 2 workers x 8 threads) takes 2 rounds
    # (~6s) - slow enough per task to watch the Workers panel hold baseline
    # at 2/2 in flight for half a minute while threaded briefly shows up to
    # 16 in flight, fast enough that the whole scenario stays well under 90s.
    run, count, hold = ctx.run, 24, 3.0

    def drain(cluster: str) -> float:
        started = time.monotonic()
        ids = [
            async_task(
                "showcase.tasks.sleep_task",
                run,
                f"{cluster}-{index}",
                seconds=hold,
                qraft_options={"cluster": cluster},
            )
            for index in range(count)
        ]
        done = ctx.poll(
            lambda: all(probe.settled(task_id) for task_id in ids), timeout=180
        )
        elapsed = time.monotonic() - started
        return elapsed if done else -1.0

    process_seconds = drain("baseline")
    ctx.check(
        "baseline cluster drained the queue",
        process_seconds > 0,
        f"{process_seconds:.1f}s for {count} tasks",
    )
    thread_seconds = drain("threaded")
    ctx.check(
        "threaded cluster drained the queue",
        thread_seconds > 0,
        f"{thread_seconds:.1f}s for {count} tasks",
    )

    if process_seconds > 0 and thread_seconds > 0:
        speedup = process_seconds / thread_seconds
        ctx.note(
            f"{count} x {hold}s sleeps: baseline 2 workers took "
            f"{process_seconds:.1f}s, threaded 2 workers x 8 threads took "
            f"{thread_seconds:.1f}s ({speedup:.1f}x)."
        )
        ctx.check(
            "threading is faster on I/O-bound work",
            speedup > 3.0,
            f"speedup {speedup:.2f}x",
        )


@scenario(
    "core.hook-modes",
    group="core",
    title="Async hooks versus sync_hooks",
    proves="Async hooks run as their own queued task with a dispatch row; "
    "sync_hooks=True runs the hook in the monitor with neither.",
    clusters=("default", "synchooks"),
)
def hook_modes(ctx):
    run = ctx.run
    async_id = async_task(
        "showcase.tasks.ok_task",
        run,
        "async-mode",
        qraft_options={**_hooks(run, "async-mode"), "cluster": "default"},
    )
    sync_id = async_task(
        "showcase.tasks.ok_task",
        run,
        "sync-mode",
        qraft_options={**_hooks(run, "sync-mode"), "cluster": "synchooks"},
    )

    async_task_row = ctx.wait(
        "async-hook task settles", lambda: probe.settled(async_id), timeout=60
    )
    sync_task_row = ctx.wait(
        "sync-hook task settles", lambda: probe.settled(sync_id), timeout=60
    )
    if not (async_task_row and sync_task_row):
        return

    ctx.wait(
        "async hook ran",
        lambda: ctx.count(kind=Event.HOOK, name="success:async-mode"),
        timeout=45,
    )
    ctx.wait(
        "sync hook ran",
        lambda: ctx.count(kind=Event.HOOK, name="success:sync-mode"),
        timeout=45,
    )

    ctx.equals(
        "async mode records a dispatch row",
        probe.hook_dispatches(async_task_row),
        ["success"],
    )
    ctx.equals(
        "sync mode records no dispatch row", probe.hook_dispatches(sync_task_row), []
    )

    ctx.check(
        "async hook was queued as its own task",
        probe.q2_task_named(f"hook:success:{async_task_row.id}"),
        "django_q task named hook:success:<id> exists",
    )
    ctx.check(
        "sync hook was not queued",
        not probe.q2_task_named(f"hook:success:{sync_task_row.id}"),
        "no django_q task for the sync hook",
    )
