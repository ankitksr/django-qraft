"""Graph lifecycle through real workers, with durable application evidence."""

from qraft import graphs
from qraft.models import QraftGraph, QraftTask, WorkflowHookDispatch
from showcase.harness import scenario
from showcase.models import Control, Event


def reached(graph_id, status):
    return lambda: QraftGraph.objects.filter(pk=graph_id, status=status).first()


@scenario(
    "wf.graph-resume",
    group="workflows",
    title="Graph: selective resume and settlement generations",
    proves="A failed branch resumes without repeating its successful sibling; "
    "each generation delivers its own settlement hook.",
)
def graph_resume(ctx):
    Control.objects.create(key=f"{ctx.run}:gate", value={"open": False})
    builder = graphs.Graph(
        subject=("demo", ctx.run),
        kind="selective-resume",
        on_settled="showcase.hooks.on_graph_settled",
        on_settled_kwargs={"run": ctx.run},
    )
    builder.node(
        "keep", "showcase.tasks.ok_task", ctx.run, "kept", recovery="idempotent"
    )
    builder.node(
        "retry",
        "showcase.tasks.gated_step_task",
        ctx.run,
        "retry",
        1,
        recovery="idempotent",
        qraft_options={"max_attempts": 1},
    )
    builder.node(
        "publish",
        "showcase.tasks.ok_task",
        ctx.run,
        "published",
        after=("keep", "retry"),
        recovery="idempotent",
    )
    graph_id = builder.start()
    if not ctx.wait("graph fails at the closed gate", reached(graph_id, "failed"), 45):
        return
    kept_task = graphs.get(graph_id).nodes.get(key="keep").task_id
    ctx.equals(
        "only failed branch needs rerunning",
        graphs.preview_resume(graph_id)["rerun"],
        ["retry"],
    )
    Control.objects.filter(key=f"{ctx.run}:gate").update(value={"open": True})
    graphs.resume(graph_id)
    if not ctx.wait("resumed graph succeeds", reached(graph_id, "succeeded"), 45):
        return
    ctx.equals(
        "successful sibling keeps its task",
        graphs.get(graph_id).nodes.get(key="keep").task_id,
        kept_task,
    )
    ctx.equals("successful sibling ran once", ctx.count(name="kept"), 1)
    ctx.equals("downstream node ran once", ctx.count(name="published"), 1)
    ctx.wait(
        "both generation hooks arrived",
        lambda: ctx.count(name="graph-settled") == 2,
        30,
    )
    ctx.equals(
        "hook outcomes are generation-specific",
        sorted(
            (event.payload["generation"], event.payload["outcome"])
            for event in ctx.events(name="graph-settled")
        ),
        [(1, "failed"), (2, "succeeded")],
    )
    ctx.equals(
        "dispatch history retained",
        WorkflowHookDispatch.objects.filter(
            workflow_type="graph", workflow_id=graph_id
        ).count(),
        2,
    )


@scenario(
    "wf.graph-approval",
    group="workflows",
    title="Graph: approve, reject and cancel gates",
    proves="Approval gates create no task until approved; cancelled and rejected "
    "graphs cannot dispatch gated work.",
)
def graph_approval(ctx):
    for decision in ("approve", "reject", "cancel"):
        builder = graphs.Graph(subject=("demo", ctx.run), kind=f"gate-{decision}")
        builder.node(
            "publish",
            "showcase.tasks.ok_task",
            ctx.run,
            decision,
            recovery="idempotent",
            requires_approval=True,
        )
        graph_id = builder.start()
        ctx.equals(
            f"{decision}: graph is parked",
            graphs.get(graph_id).status,
            "waiting_approval",
        )
        ctx.equals(
            f"{decision}: no task before decision",
            QraftTask.objects.filter(graph_id=graph_id).count(),
            0,
        )
        if decision == "approve":
            graphs.approve(graph_id, "publish")
            ctx.wait("approved graph succeeds", reached(graph_id, "succeeded"), 30)
        else:
            if decision == "reject":
                graphs.reject(graph_id, "publish", reason="demo")
            else:
                graphs.cancel(graph_id)
            try:
                graphs.approve(graph_id, "publish")
            except graphs.GraphError:
                ctx.check(f"{decision}: late approval refused", True)
            else:
                ctx.check(f"{decision}: late approval refused", False)
            ctx.equals(
                f"{decision}: no task dispatched",
                QraftTask.objects.filter(graph_id=graph_id).count(),
                0,
            )


@scenario(
    "wf.graph-receipt",
    group="workflows",
    title="Graph: application writes and completion receipt",
    proves="A published application event and its receipt commit together, "
    "and the receipt is visible in the graph snapshot.",
)
def graph_receipt(ctx):
    builder = graphs.Graph(subject=("demo", ctx.run), kind="publication")
    builder.node(
        "publish", "showcase.tasks.publish_event", ctx.run, recovery="transactional"
    )
    graph_id = builder.start()
    if not ctx.wait("publication graph succeeds", reached(graph_id, "succeeded"), 30):
        return
    receipt = graphs.snapshot(graph_id)["nodes"][0]["receipt"]
    ctx.require("receipt exists", receipt is not None)
    ctx.check(
        "receipt identifies committed application data",
        Event.objects.filter(
            pk=receipt["event_id"], run=ctx.run, name="published-output"
        ).exists(),
    )
