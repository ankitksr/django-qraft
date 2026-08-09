"""Workflow scenarios: chain, iter, batch, approval, cancellation, progress."""

from qraft.batch import QraftBatch
from qraft.chain import QraftChain
from qraft.iter import QraftIter
from qraft.models import (
    QraftBatchModel,
    QraftChainModel,
    QraftIterModel,
    WorkflowStatus,
)
from showcase.harness import scenario
from showcase.models import Control, Event


def _status(model_class, workflow_id):
    return model_class.objects.get(id=workflow_id).status


def _reached(model_class, workflow_id, *statuses):
    """Poll helper: the workflow row once it is in one of `statuses`."""

    def check():
        row = model_class.objects.get(id=workflow_id)
        return row if row.status in statuses else None

    return check


def _first_index(names: list[str], needle: str) -> int:
    return names.index(needle) if needle in names else -1


def _cancelled_hooks() -> int:
    """
    How many times the on_cancelled hook has fired, across everything.

    It cannot be scoped to one scenario: qraft calls on_cancelled with no
    arguments, so the hook has no run id to record.
    """
    return Event.objects.filter(kind=Event.HOOK, name="cancelled").count()


@scenario(
    "wf.chain",
    group="workflows",
    title="Chain: ordering, per-step retry, completion hook",
    proves="Steps run strictly in order, a step's own retry policy applies "
    "inside the chain, and the chain hook fires once at the end.",
)
def chain(ctx):
    run = ctx.run
    workflow = QraftChain(
        on_success="showcase.hooks.on_workflow_success",
        success_args=(run, "chain"),
        on_failure="showcase.hooks.on_workflow_failure",
        failure_args=(run, "chain"),
    )
    workflow.append("showcase.tasks.step_task", run, "s0", 0)
    workflow.append(
        "showcase.tasks.flaky_task",
        run,
        "s1",
        fail_times=1,
        qraft_options={"max_attempts": 3, "base_delay": 1.0, "jitter": False},
    )
    workflow.append("showcase.tasks.step_task", run, "s2", 2)
    chain_id = workflow.run()

    row = ctx.wait(
        "chain reaches a terminal status",
        _reached(
            QraftChainModel,
            chain_id,
            WorkflowStatus.SUCCEEDED,
            WorkflowStatus.FAILED,
            WorkflowStatus.CANCELLED,
        ),
        timeout=120,
    )
    if not row:
        return
    ctx.equals("chain succeeded", row.status, WorkflowStatus.SUCCEEDED)

    names = ctx.names()
    order = [_first_index(names, step) for step in ("s0", "s1", "s2")]
    ctx.check(
        "steps ran in order",
        order == sorted(order) and -1 not in order,
        f"first-seen positions s0,s1,s2 = {order}",
    )

    middle = row.steps.get(step_index=1).qraft_task
    ctx.equals("middle step retried under its own policy", middle.attempts.count(), 2)

    ctx.wait(
        "chain success hook fired",
        lambda: ctx.count(kind=Event.HOOK, name="wf-success:chain"),
        timeout=45,
    )
    ctx.equals(
        "chain success hook fired exactly once",
        ctx.count(kind=Event.HOOK, name="wf-success:chain"),
        1,
    )
    ctx.equals(
        "chain failure hook did not fire",
        ctx.count(kind=Event.HOOK, name="wf-failure:chain"),
        0,
    )


@scenario(
    "wf.chain-failure",
    group="workflows",
    title="Chain: failure hook and short-circuit",
    proves="An exhausted step fails the chain, fires the failure hook, and "
    "the steps behind it never run.",
)
def chain_failure(ctx):
    run = ctx.run
    workflow = QraftChain(
        on_success="showcase.hooks.on_workflow_success",
        success_args=(run, "broken"),
        on_failure="showcase.hooks.on_workflow_failure",
        failure_args=(run, "broken"),
    )
    workflow.append("showcase.tasks.step_task", run, "b0", 0)
    workflow.append(
        "showcase.tasks.fail_task",
        run,
        "b1",
        qraft_options={"max_attempts": 2, "base_delay": 1.0, "jitter": False},
    )
    workflow.append("showcase.tasks.step_task", run, "b2", 2)
    chain_id = workflow.run()

    row = ctx.wait(
        "chain reaches a terminal status",
        _reached(
            QraftChainModel, chain_id, WorkflowStatus.SUCCEEDED, WorkflowStatus.FAILED
        ),
        timeout=120,
    )
    if not row:
        return

    ctx.equals("chain failed", row.status, WorkflowStatus.FAILED)
    ctx.equals(
        "failing step used its full retry budget",
        row.steps.get(step_index=1).qraft_task.attempts.count(),
        2,
    )
    ctx.wait(
        "chain failure hook fired",
        lambda: ctx.count(kind=Event.HOOK, name="wf-failure:broken"),
        timeout=45,
    )
    ctx.equals(
        "chain success hook did not fire",
        ctx.count(kind=Event.HOOK, name="wf-success:broken"),
        0,
    )

    ctx.settle(3)
    ctx.equals("step after the failure never ran", ctx.count(name="b2"), 0)
    ctx.check(
        "step 2 was never queued",
        row.steps.get(step_index=2).qraft_task_id is None,
        "no QraftTask linked to the unreached step",
    )


@scenario(
    "wf.chain-resume",
    group="workflows",
    title="Chain: resume from the failed step",
    proves="resume() restarts the failed step only. Steps that already "
    "succeeded are not run a second time.",
)
def chain_resume(ctx):
    run = ctx.run
    Control.objects.update_or_create(
        key=f"{run}:gate", defaults={"value": {"open": False}}
    )

    workflow = QraftChain(
        on_success="showcase.hooks.on_workflow_success", success_args=(run, "resume")
    )
    workflow.append("showcase.tasks.step_task", run, "r0", 0)
    workflow.append("showcase.tasks.gated_step_task", run, "r1", 1)
    workflow.append("showcase.tasks.step_task", run, "r2", 2)
    chain_id = workflow.run()

    failed = ctx.wait(
        "chain fails at the gated step",
        _reached(QraftChainModel, chain_id, WorkflowStatus.FAILED),
        timeout=90,
    )
    if not failed:
        return
    ctx.equals("chain stopped at step 1", failed.current_step_index, 1)
    ctx.equals("first step ran once", ctx.count(name="r0"), 1)
    ctx.equals("third step has not run", ctx.count(name="r2"), 0)

    Control.objects.filter(key=f"{run}:gate").update(value={"open": True})
    QraftChain(chain_id=chain_id).resume()

    resumed = ctx.wait(
        "chain succeeds after resume",
        _reached(QraftChainModel, chain_id, WorkflowStatus.SUCCEEDED),
        timeout=90,
    )
    if not resumed:
        return

    ctx.equals("first step was not re-run", ctx.count(name="r0"), 1)
    ctx.equals("gated step ran twice in total", ctx.count(name="r1"), 2)
    ctx.equals("third step ran after the resume", ctx.count(name="r2"), 1)
    ctx.wait(
        "chain success hook fired after resume",
        lambda: ctx.count(kind=Event.HOOK, name="wf-success:resume"),
        timeout=45,
    )


@scenario(
    "wf.iter",
    group="workflows",
    title="Iter: parallel fan-out",
    proves="Every member runs, the counters land exactly on the total, and "
    "the workflow hook fires exactly once.",
)
def iter_workflow(ctx):
    run, total = ctx.run, 5
    workflow = QraftIter(
        "showcase.tasks.ok_task",
        on_success="showcase.hooks.on_workflow_success",
        success_args=(run, "iter"),
        on_failure="showcase.hooks.on_workflow_failure",
        failure_args=(run, "iter"),
    )
    for index in range(total):
        workflow.append(run, f"it-{index}")
    iter_id = workflow.run()

    row = ctx.wait(
        "iter reaches a terminal status",
        _reached(
            QraftIterModel, iter_id, WorkflowStatus.SUCCEEDED, WorkflowStatus.FAILED
        ),
        timeout=120,
    )
    if not row:
        return

    ctx.equals("iter succeeded", row.status, WorkflowStatus.SUCCEEDED)
    ctx.equals("total", row.total_count, total)
    ctx.equals("completed", row.completed_count, total)
    ctx.equals("successes", row.success_count, total)
    ctx.equals("failures", row.failure_count, 0)
    ctx.equals("every member task ran", ctx.count(kind=Event.TASK), total)

    ctx.wait(
        "iter hook fired",
        lambda: ctx.count(kind=Event.HOOK, name="wf-success:iter"),
        timeout=45,
    )
    ctx.settle(2)
    ctx.equals(
        "iter hook fired exactly once",
        ctx.count(kind=Event.HOOK, name="wf-success:iter"),
        1,
    )


@scenario(
    "wf.batch",
    group="workflows",
    title="Batch: heterogeneous fan-out",
    proves="Different functions fan out together, each with its own retry "
    "policy, and the batch hook fires once when all of them settle.",
)
def batch_workflow(ctx):
    run = ctx.run
    workflow = QraftBatch(
        on_success="showcase.hooks.on_workflow_success",
        success_args=(run, "batch"),
        on_failure="showcase.hooks.on_workflow_failure",
        failure_args=(run, "batch"),
    )
    workflow.append("showcase.tasks.ok_task", run, "ba-ok")
    workflow.append("showcase.tasks.sleep_task", run, "ba-slow", seconds=1.0)
    workflow.append(
        "showcase.tasks.flaky_task",
        run,
        "ba-flaky",
        fail_times=1,
        qraft_options={"max_attempts": 3, "base_delay": 1.0, "jitter": False},
    )
    batch_id = workflow.run()

    row = ctx.wait(
        "batch reaches a terminal status",
        _reached(
            QraftBatchModel, batch_id, WorkflowStatus.SUCCEEDED, WorkflowStatus.FAILED
        ),
        timeout=120,
    )
    if not row:
        return

    ctx.equals("batch succeeded", row.status, WorkflowStatus.SUCCEEDED)
    ctx.equals("completed", row.completed_count, 3)
    ctx.equals("successes", row.success_count, 3)
    ctx.equals("failures", row.failure_count, 0)

    functions = sorted(row.tasks.values_list("func", flat=True))
    ctx.equals(
        "three different functions ran",
        functions,
        [
            "showcase.tasks.flaky_task",
            "showcase.tasks.ok_task",
            "showcase.tasks.sleep_task",
        ],
    )
    flaky = row.tasks.get(func="showcase.tasks.flaky_task")
    ctx.equals("the flaky member used its own retry policy", flaky.attempts.count(), 2)

    ctx.wait(
        "batch hook fired",
        lambda: ctx.count(kind=Event.HOOK, name="wf-success:batch"),
        timeout=45,
    )
    ctx.settle(2)
    ctx.equals(
        "batch hook fired exactly once",
        ctx.count(kind=Event.HOOK, name="wf-success:batch"),
        1,
    )


@scenario(
    "wf.progress",
    group="workflows",
    title="Workflow progress hook",
    proves="The progress hook fires on every partial completion and stops "
    "before the last one, where the completion hook takes over.",
)
def progress_hook(ctx):
    run, total = ctx.run, 4
    workflow = QraftIter(
        "showcase.tasks.ok_task",
        on_success="showcase.hooks.on_workflow_success",
        success_args=(run, "prog"),
        progress_hook="showcase.hooks.on_progress",
    )
    for index in range(total):
        workflow.append(run, f"pg-{index}")
    iter_id = workflow.run()

    row = ctx.wait(
        "iter completes",
        _reached(
            QraftIterModel, iter_id, WorkflowStatus.SUCCEEDED, WorkflowStatus.FAILED
        ),
        timeout=120,
    )
    if not row:
        return

    # The progress hook is addressed by workflow id, since qraft calls it with
    # a fixed keyword signature that carries no scenario id.
    def recorded() -> list:
        return list(
            Event.objects.filter(run=str(iter_id), name="progress").values_list(
                "payload", flat=True
            )
        )

    # Each hook is a queued task of its own, so the last one can land well
    # after the workflow itself is complete. Waiting for the expected count
    # rather than for the first arrival is what keeps this deterministic.
    ctx.poll(lambda: len(recorded()) >= total - 1 or None, timeout=45)
    progress = recorded()

    ctx.equals(
        "progress hook fired on each partial completion", len(progress), total - 1
    )
    # Compared as a set, not a sequence: the dispatcher serialises the counter
    # under a row lock, but each progress hook is itself a queued task, so two
    # of them can land on different workers and record out of order.
    ctx.equals(
        "each partial completion reported its own count",
        sorted(entry["completed_count"] for entry in progress),
        list(range(1, total)),
    )
    ctx.check(
        "progress reported the right total",
        all(entry["total_count"] == total for entry in progress),
        f"{[entry['total_count'] for entry in progress]}",
    )
    ctx.wait(
        "completion hook fired at the end",
        lambda: ctx.count(kind=Event.HOOK, name="wf-success:prog"),
        timeout=45,
    )


@scenario(
    "wf.approval",
    group="workflows",
    title="Human in the loop: approve and reject",
    proves="A gated chain parks at WAITING_APPROVAL and consumes nothing "
    "until approve() resumes it; reject() cancels it instead.",
)
def approval(ctx):
    run = ctx.run

    approved = QraftChain(
        on_success="showcase.hooks.on_workflow_success", success_args=(run, "approved")
    )
    approved.append("showcase.tasks.step_task", run, "ap-draft", 0)
    approved.append(
        "showcase.tasks.step_task", run, "ap-publish", 1, requires_approval=True
    )
    approved_id = approved.run()

    parked = ctx.wait(
        "chain parks for approval",
        _reached(QraftChainModel, approved_id, WorkflowStatus.WAITING_APPROVAL),
        timeout=90,
    )
    if not parked:
        return

    ctx.equals("the step before the gate ran", ctx.count(name="ap-draft"), 1)
    ctx.check(
        "the gated step was not queued",
        parked.steps.get(step_index=1).qraft_task_id is None,
        "no QraftTask linked while parked",
    )
    ctx.settle(4)
    ctx.equals(
        "the gated step consumed nothing while parked", ctx.count(name="ap-publish"), 0
    )
    ctx.equals(
        "still parked after waiting",
        _status(QraftChainModel, approved_id),
        WorkflowStatus.WAITING_APPROVAL,
    )

    QraftChain(chain_id=approved_id).approve()
    resumed = ctx.wait(
        "approve() resumes the chain",
        _reached(QraftChainModel, approved_id, WorkflowStatus.SUCCEEDED),
        timeout=90,
    )
    if resumed:
        ctx.equals("the gated step ran after approval", ctx.count(name="ap-publish"), 1)
        ctx.wait(
            "chain hook fired after approval",
            lambda: ctx.count(kind=Event.HOOK, name="wf-success:approved"),
            timeout=45,
        )

    rejected = QraftChain(
        on_success="showcase.hooks.on_workflow_success",
        success_args=(run, "rejected"),
        on_cancelled="showcase.hooks.on_cancelled",
    )
    rejected.append(
        "showcase.tasks.step_task", run, "rj-publish", 0, requires_approval=True
    )
    rejected_id = rejected.run()

    ctx.equals(
        "a first-step gate parks before running anything",
        _status(QraftChainModel, rejected_id),
        WorkflowStatus.WAITING_APPROVAL,
    )
    ctx.equals("nothing ran before the gate", ctx.count(name="rj-publish"), 0)

    before = _cancelled_hooks()
    QraftChain(chain_id=rejected_id).reject("not this time")
    ctx.equals(
        "reject() cancels the chain",
        _status(QraftChainModel, rejected_id),
        WorkflowStatus.CANCELLED,
    )
    ctx.wait(
        "reject() fired the on_cancelled hook",
        lambda: _cancelled_hooks() > before,
        timeout=45,
    )
    ctx.note(
        "qraft dispatches on_cancelled with no arguments, so the hook cannot "
        "be told which workflow was cancelled; this counts dispatches instead."
    )
    ctx.settle(3)
    ctx.equals("the rejected step never ran", ctx.count(name="rj-publish"), 0)
    ctx.equals(
        "the chain success hook did not fire",
        ctx.count(kind=Event.HOOK, name="wf-success:rejected"),
        0,
    )


@scenario(
    "wf.cancel",
    group="workflows",
    title="Cancellation, including a cancel racing a completion",
    proves="Cancelling stops further work, and a cancel issued while the "
    "last member is still running is not overwritten when it lands.",
)
def cancel(ctx):
    run = ctx.run

    ctx.note(
        "QraftIter has to be reconstructed with its func even when iter_id is "
        "given, unlike QraftBatch(batch_id=...); and cancel() dispatches no "
        "on_cancelled hook for any workflow type, only QraftChain.reject() does."
    )

    early = QraftIter(
        "showcase.tasks.sleep_task",
        on_success="showcase.hooks.on_workflow_success",
        success_args=(run, "cancel-early"),
        on_cancelled="showcase.hooks.on_cancelled",
    )
    for index in range(4):
        early.append(run, f"ce-{index}", seconds=3.0)
    early_id = early.run()
    QraftIter("showcase.tasks.sleep_task", iter_id=early_id).cancel()

    ctx.equals(
        "cancelled immediately",
        _status(QraftIterModel, early_id),
        WorkflowStatus.CANCELLED,
    )
    ctx.settle(10)
    row = QraftIterModel.objects.get(id=early_id)
    ctx.equals(
        "still cancelled once the in-flight members finish",
        row.status,
        WorkflowStatus.CANCELLED,
    )
    ctx.equals("cancelled workflow counted nothing", row.completed_count, 0)
    ctx.equals(
        "no completion hook after cancellation",
        ctx.count(kind=Event.HOOK, name="wf-success:cancel-early"),
        0,
    )

    # Race: three members finish fast, the fourth is still running when the
    # cancel lands. The dispatcher re-reads status under its row lock, so the
    # late completion must not flip the workflow back to SUCCEEDED.
    racing = QraftIter(
        "showcase.tasks.sleep_task",
        on_success="showcase.hooks.on_workflow_success",
        success_args=(run, "cancel-race"),
    )
    for index in range(3):
        racing.append(run, f"cr-{index}", seconds=0.2)
    racing.append(run, "cr-last", seconds=8.0)
    racing_id = racing.run()

    ready = ctx.wait(
        "three of four members complete",
        lambda: (QraftIterModel.objects.get(id=racing_id).completed_count == 3 or None),
        timeout=90,
    )
    if not ready:
        return

    before = QraftIterModel.objects.get(id=racing_id)
    if before.status != WorkflowStatus.RUNNING:
        ctx.check(
            "cancel window was still open",
            False,
            f"workflow already {before.status}; the race window closed early",
        )
        return

    QraftIter("showcase.tasks.sleep_task", iter_id=racing_id).cancel()
    ctx.equals(
        "cancelled while the last member was in flight",
        _status(QraftIterModel, racing_id),
        WorkflowStatus.CANCELLED,
    )

    ctx.wait(
        "the last member finishes",
        lambda: ctx.count(name="cr-last"),
        timeout=60,
    )
    ctx.settle(4)
    final = QraftIterModel.objects.get(id=racing_id)
    ctx.equals(
        "the late completion did not overwrite the cancel",
        final.status,
        WorkflowStatus.CANCELLED,
    )
    ctx.equals("the late completion was not counted", final.completed_count, 3)
    ctx.equals(
        "no completion hook after the racing cancel",
        ctx.count(kind=Event.HOOK, name="wf-success:cancel-race"),
        0,
    )
