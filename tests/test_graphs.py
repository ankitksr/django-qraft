"""Tests for qraft.graphs: topology, frontier dispatch, quiescent settlement."""

from unittest.mock import patch

import pytest
from django.utils import timezone

from qraft import graphs
from qraft.models import QraftTask, QraftTaskAttempt, TaskStatus, WorkflowHookDispatch
from qraft.models.graphs import GraphStatus, NodeStatus, QraftGraph, QraftGraphNode
from qraft.models.tasks import AttemptState

pytestmark = pytest.mark.django_db

TASK = "tests.e2e_tasks.succeed"
RECOVERY = "transactional"


def build_graph(**kwargs):
    return graphs.Graph(subject=("worksheet", 4117), kind="shadow", **kwargs)


def add_node(builder, key, after=()):
    builder.node(key, TASK, after=after, recovery=RECOVERY)


def complete_node(graph_id, node_key, success=True):
    node = QraftGraphNode.objects.get(graph_id=graph_id, key=node_key)
    task = QraftTask.objects.get(pk=node.task_id)
    task.status = TaskStatus.SUCCEEDED if success else TaskStatus.EXHAUSTED
    task.save(update_fields=["status"])
    attempt = task.attempts.order_by("-attempt_number").first()
    QraftTaskAttempt.objects.filter(pk=attempt.pk).update(
        success=success, date_completed=timezone.now()
    )
    attempt.refresh_from_db()
    graphs.handle_node_completion(task, attempt)


class TestStartTopology:
    def test_start_records_the_subject_kind_and_nodes(self):
        builder = build_graph(revision="rules@v7", metadata={"trigger": "upload"})
        add_node(builder, "ingest")
        add_node(builder, "rules", after=("ingest",))
        graph_id = builder.start()

        graph = QraftGraph.objects.get(id=graph_id)
        assert (graph.subject_type, graph.subject_id) == ("worksheet", "4117")
        assert (graph.status, graph.kind, graph.revision) == (
            GraphStatus.RUNNING,
            "shadow",
            "rules@v7",
        )
        assert graph.metadata == {"trigger": "upload"}
        assert list(graph.nodes.values_list("key", "depth", "status")) == [
            ("ingest", 0, NodeStatus.RUNNING),
            ("rules", 1, NodeStatus.PENDING),
        ]

    def test_backdated_start_and_previous_graph_link(self):
        earlier = timezone.now() - timezone.timedelta(hours=2)
        first_builder = build_graph()
        add_node(first_builder, "ingest")
        first = first_builder.start()

        second_builder = build_graph(started_at=earlier, previous_graph=first)
        add_node(second_builder, "ingest")
        second = second_builder.start()

        graph = QraftGraph.objects.get(id=second)
        assert graph.date_started == earlier
        assert str(graph.previous_graph_id) == first

    def test_an_empty_graph_is_refused(self):
        with pytest.raises(graphs.GraphError, match="at least one node"):
            build_graph().start()

    def test_duplicate_node_keys_are_refused(self):
        builder = build_graph()
        add_node(builder, "ingest")
        add_node(builder, "ingest")
        with pytest.raises(graphs.GraphError, match="unique"):
            builder.start()

    def test_an_unknown_dependency_key_is_refused(self):
        builder = build_graph()
        add_node(builder, "rules", after=("missing",))
        with pytest.raises(graphs.GraphError, match="unknown key"):
            builder.start()

    def test_a_self_edge_is_refused(self):
        builder = build_graph()
        builder.node("ingest", TASK, after=("ingest",), recovery=RECOVERY)
        with pytest.raises(graphs.GraphError, match="cannot depend on itself"):
            builder.start()

    def test_a_cycle_is_refused(self):
        builder = build_graph()
        builder.node("a", TASK, after=("b",), recovery=RECOVERY)
        builder.node("b", TASK, after=("a",), recovery=RECOVERY)
        with pytest.raises(graphs.GraphError, match="cycle"):
            builder.start()


class TestFrontierDispatch:
    def test_root_nodes_receive_tasks_and_scheduled_attempts_on_start(self):
        builder = build_graph()
        add_node(builder, "ingest")
        add_node(builder, "rules", after=("ingest",))
        graph_id = builder.start()

        ingest = QraftGraphNode.objects.get(graph_id=graph_id, key="ingest")
        rules = QraftGraphNode.objects.get(graph_id=graph_id, key="rules")

        assert ingest.status == NodeStatus.RUNNING
        assert ingest.task_id is not None
        assert ingest.dispatched_at is not None
        attempt = ingest.task.attempts.get()
        assert attempt.state == AttemptState.SCHEDULED
        assert attempt.q2_task_id is None

        assert rules.status == NodeStatus.PENDING
        assert rules.task_id is None

    def test_parallel_roots_both_dispatch_on_start(self):
        builder = build_graph()
        add_node(builder, "a")
        add_node(builder, "b")
        graph_id = builder.start()

        for key in ("a", "b"):
            node = QraftGraphNode.objects.get(graph_id=graph_id, key=key)
            assert node.status == NodeStatus.RUNNING
            assert node.task.attempts.filter(state=AttemptState.SCHEDULED).exists()


class TestQuiescentSettlement:
    def test_all_nodes_succeeding_settles_the_graph_once(
        self, signal_log, django_capture_on_commit_callbacks
    ):
        builder = build_graph()
        add_node(builder, "ingest")
        add_node(builder, "rules", after=("ingest",))
        graph_id = builder.start()

        complete_node(graph_id, "ingest")
        assert QraftGraph.objects.get(id=graph_id).status == GraphStatus.RUNNING

        with django_capture_on_commit_callbacks(execute=True):
            complete_node(graph_id, "rules")
        graph = QraftGraph.objects.get(id=graph_id)
        assert graph.status == GraphStatus.SUCCEEDED
        assert graph.settled_at is not None
        assert len(signal_log["graph_settled"]) == 1

        with django_capture_on_commit_callbacks(execute=True):
            complete_node(graph_id, "rules")
        assert len(signal_log["graph_settled"]) == 1

    def test_a_failed_node_waits_for_siblings_before_the_graph_fails(
        self, signal_log, django_capture_on_commit_callbacks
    ):
        builder = build_graph()
        add_node(builder, "a")
        add_node(builder, "b")
        graph_id = builder.start()

        with django_capture_on_commit_callbacks(execute=True):
            complete_node(graph_id, "a", success=False)
        graph = QraftGraph.objects.get(id=graph_id)
        assert graph.status == GraphStatus.RUNNING
        assert (
            QraftGraphNode.objects.get(graph_id=graph_id, key="a").status
            == NodeStatus.FAILED
        )
        assert (
            QraftGraphNode.objects.get(graph_id=graph_id, key="b").status
            == NodeStatus.RUNNING
        )
        assert len(signal_log["graph_settled"]) == 0

        with django_capture_on_commit_callbacks(execute=True):
            complete_node(graph_id, "b", success=True)
        graph = QraftGraph.objects.get(id=graph_id)
        assert graph.status == GraphStatus.FAILED
        assert len(signal_log["graph_settled"]) == 1

    def test_a_skipped_node_still_lets_the_graph_succeed(self):
        builder = build_graph()
        add_node(builder, "ingest")
        add_node(builder, "rules", after=("ingest",))
        graph_id = builder.start()

        graphs.skip(graph_id, "rules", reason="nothing undecided")
        complete_node(graph_id, "ingest")
        assert QraftGraph.objects.get(id=graph_id).status == GraphStatus.SUCCEEDED


class TestSkipAndCancel:
    def test_skip_is_refused_on_a_running_node_and_on_a_settled_graph(self):
        builder = build_graph()
        add_node(builder, "ingest")
        add_node(builder, "rules", after=("ingest",))
        graph_id = builder.start()

        with pytest.raises(graphs.GraphError, match="only a pending node"):
            graphs.skip(graph_id, "ingest")

        graphs.skip(graph_id, "rules")
        complete_node(graph_id, "ingest")
        with pytest.raises(graphs.GraphError, match="final"):
            graphs.skip(graph_id, "rules")

    def test_cancel_settles_once_and_refuses_a_second_call(
        self, signal_log, django_capture_on_commit_callbacks
    ):
        builder = build_graph()
        add_node(builder, "ingest")
        graph_id = builder.start()

        with django_capture_on_commit_callbacks(execute=True):
            assert graphs.cancel(graph_id) is True
        assert QraftGraph.objects.get(id=graph_id).status == GraphStatus.CANCELLED
        assert len(signal_log["graph_settled"]) == 1

        with (
            django_capture_on_commit_callbacks(execute=True),
            pytest.raises(graphs.GraphError, match="already"),
        ):
            graphs.cancel(graph_id)
        assert len(signal_log["graph_settled"]) == 1

    def test_unknown_graph_raises_rather_than_returning_none(self):
        with pytest.raises(graphs.GraphError):
            graphs.cancel("00000000-0000-0000-0000-000000000000")


class TestSupersededGeneration:
    def test_an_old_task_completion_is_dropped_after_rebind(self):
        builder = build_graph()
        add_node(builder, "ingest")
        graph_id = builder.start()

        node = QraftGraphNode.objects.get(graph_id=graph_id, key="ingest")
        old_task = node.task
        new_task = QraftTask.objects.create(
            func=TASK,
            task_args=[],
            task_kwargs={},
            status=TaskStatus.SUCCEEDED,
            graph_id=graph_id,
            node="ingest",
            graph_node=node,
            subject_type="worksheet",
            subject_id="4117",
        )
        QraftGraphNode.objects.filter(pk=node.pk).update(task=new_task)

        old_task.status = TaskStatus.SUCCEEDED
        old_task.save(update_fields=["status"])
        attempt = old_task.attempts.order_by("-attempt_number").first()
        QraftTaskAttempt.objects.filter(pk=attempt.pk).update(
            success=True, date_completed=timezone.now()
        )
        attempt.refresh_from_db()

        assert graphs.handle_node_completion(old_task, attempt) is True
        node.refresh_from_db()
        assert node.status == NodeStatus.RUNNING
        assert node.task_id == new_task.id


class TestOnSettled:
    def test_on_settled_is_dispatched_once_with_a_context(self):
        builder = build_graph(on_settled="app.hooks.ready")
        add_node(builder, "ingest")
        graph_id = builder.start()

        with patch("qraft.dispatchers.q2_async_task", return_value="q2-hook") as queued:
            complete_node(graph_id, "ingest")
            complete_node(graph_id, "ingest")

        dispatches = WorkflowHookDispatch.objects.filter(
            workflow_type="graph", workflow_id=graph_id, hook_type="settled"
        )
        assert dispatches.count() == 1
        context = queued.call_args.kwargs["context"]
        assert context["graph_id"] == graph_id
        assert context["outcome"] == GraphStatus.SUCCEEDED
        assert context["kind"] == "shadow"
        assert context["nodes"] == {"ingest": NodeStatus.SUCCEEDED}
        assert context["duration_s"] >= 0


class TestReplaySettledHooks:
    def _settled_without_its_hook(self):
        builder = build_graph(on_settled="app.hooks.ready")
        add_node(builder, "ingest")
        graph_id = builder.start()
        with patch("qraft.graphs._dispatch_settled_hook"):
            complete_node(graph_id, "ingest")
        QraftGraph.objects.filter(id=graph_id).update(
            settled_at=timezone.now() - timezone.timedelta(minutes=10)
        )
        assert not WorkflowHookDispatch.objects.filter(workflow_id=graph_id).exists()
        return graph_id

    def test_a_settlement_whose_hook_never_dispatched_is_replayed_once(self):
        graph_id = self._settled_without_its_hook()

        with patch("qraft.dispatchers.q2_async_task", return_value="q2-hook"):
            assert graphs.replay_settled_hooks(grace=60) == 1
            assert graphs.replay_settled_hooks(grace=60) == 0

        assert (
            WorkflowHookDispatch.objects.filter(
                workflow_type="graph", workflow_id=graph_id, hook_type="settled"
            ).count()
            == 1
        )

    def test_it_leaves_running_graphs_fresh_settlements_and_hookless_graphs_alone(
        self,
    ):
        running = build_graph(on_settled="app.hooks.ready")
        add_node(running, "ingest")
        running.start()

        fresh_builder = build_graph(on_settled="app.hooks.ready")
        add_node(fresh_builder, "ingest")
        fresh = fresh_builder.start()
        with patch("qraft.graphs._dispatch_settled_hook"):
            graphs.cancel(fresh)

        hookless_builder = build_graph()
        add_node(hookless_builder, "ingest")
        hookless = hookless_builder.start()
        graphs.cancel(hookless)
        QraftGraph.objects.filter(id=hookless).update(
            settled_at=timezone.now() - timezone.timedelta(minutes=10)
        )

        with patch("qraft.dispatchers.q2_async_task", return_value="q2-hook"):
            assert graphs.replay_settled_hooks(grace=60) == 0


class TestBindSubject:
    def test_a_graph_may_start_without_a_subject(self):
        builder = graphs.Graph()
        add_node(builder, "ingest")
        add_node(builder, "score", after=("ingest",))
        graph_id = builder.start()

        graph = QraftGraph.objects.get(id=graph_id)
        assert graph.subject_type is None
        assert graph.subject_id is None
        assert graph.status == GraphStatus.RUNNING

    def test_the_bind_updates_the_graph_and_every_bound_member(self):
        from qraft.models import QraftBatchModel, QraftChainModel, QraftIterModel

        builder = graphs.Graph()
        add_node(builder, "ingest")
        add_node(builder, "score", after=("ingest",))
        graph_id = builder.start()

        node_task = QraftGraphNode.objects.get(graph_id=graph_id, key="ingest").task
        chain = QraftChainModel.objects.create(graph_id=graph_id, node="score")
        batch = QraftBatchModel.objects.create(graph_id=graph_id)
        iter_model = QraftIterModel.objects.create(
            func=TASK, total_count=1, graph_id=graph_id
        )

        graphs.bind_subject(graph_id, ("worksheet", 4117))

        graph = QraftGraph.objects.get(id=graph_id)
        assert (graph.subject_type, graph.subject_id) == ("worksheet", "4117")
        for row in (node_task, chain, batch, iter_model):
            row.refresh_from_db()
            assert (row.subject_type, row.subject_id) == ("worksheet", "4117")

    def test_a_second_bind_and_a_settled_graph_are_both_refused(self):
        builder = graphs.Graph()
        add_node(builder, "ingest")
        graph_id = builder.start()
        graphs.bind_subject(graph_id, ("worksheet", 1))

        with pytest.raises(graphs.GraphError, match="declared once"):
            graphs.bind_subject(graph_id, ("worksheet", 2))

        other_builder = build_graph()
        add_node(other_builder, "ingest")
        other = other_builder.start()
        with pytest.raises(graphs.GraphError, match="declared once"):
            graphs.bind_subject(other, ("worksheet", 10))

        settled_builder = graphs.Graph()
        add_node(settled_builder, "ingest")
        settled = settled_builder.start()
        graphs.cancel(settled)
        with pytest.raises(graphs.GraphError, match="subject is final"):
            graphs.bind_subject(settled, ("worksheet", 3))

    def test_the_model_method_delegates_and_refreshes(self):
        builder = graphs.Graph()
        add_node(builder, "ingest")
        graph = QraftGraph.objects.get(id=builder.start())

        graph.bind_subject("worksheet", 4117)

        assert (graph.subject_type, graph.subject_id) == ("worksheet", "4117")


class TestRequestKeyIdempotency:
    def test_the_same_request_key_and_plan_return_the_existing_graph(self):
        builder = build_graph()
        add_node(builder, "ingest")
        first = builder.start(request_key="upload-4117")
        second = builder.start(request_key="upload-4117")
        assert first == second
        assert QraftGraph.objects.filter(request_key="upload-4117").count() == 1

    def test_a_different_plan_under_the_same_key_is_refused(self):
        first_builder = build_graph()
        add_node(first_builder, "ingest")
        first_builder.start(request_key="upload-4117")

        second_builder = build_graph()
        add_node(second_builder, "rules")
        with pytest.raises(graphs.GraphError, match="different plan"):
            second_builder.start(request_key="upload-4117")


class TestCurrentNode:
    def test_current_node_returns_identity_for_a_graph_node_task(self):
        from qraft import context

        builder = build_graph(metadata={"pipeline": "shadow"})
        add_node(builder, "ingest")
        graph_id = builder.start()

        node = QraftGraphNode.objects.get(graph_id=graph_id, key="ingest")
        task = node.task
        attempt = task.attempts.order_by("-attempt_number").first()
        QraftTaskAttempt.objects.filter(pk=attempt.pk).update(q2_task_id="q2-node")
        attempt.refresh_from_db()
        context._current_q2_task_id.set("q2-node")

        info = context.current_node()
        assert info == {
            "graph_id": graph_id,
            "node_key": "ingest",
            "generation": 1,
            "task_id": str(task.id),
            "attempt_id": str(attempt.id),
            "attempt_number": 1,
            "subject_type": "worksheet",
            "subject_id": "4117",
            "metadata": {"pipeline": "shadow"},
        }

    def test_current_node_is_none_outside_a_graph_task(self):
        from qraft import context

        assert context.current_node() is None


class TestOverdue:
    """A graph running past its threshold is flagged, never failed."""

    def _running_graph(self, started_at=None):
        builder = build_graph(started_at=started_at)
        add_node(builder, "ingest")
        return builder.start()

    def test_flags_once_and_only_past_the_threshold(
        self, settings, signal_log, django_capture_on_commit_callbacks
    ):
        settings.QRAFT_GRAPH_OVERDUE_AFTER = 60
        fresh = self._running_graph()
        stale = self._running_graph(
            started_at=timezone.now() - timezone.timedelta(minutes=5)
        )

        with django_capture_on_commit_callbacks(execute=True):
            assert graphs.flag_overdue() == 1
            assert graphs.flag_overdue() == 0

        assert QraftGraph.objects.get(id=stale).overdue_flagged_at is not None
        assert QraftGraph.objects.get(id=fresh).overdue_flagged_at is None
        # Observation only: an overdue graph keeps running.
        assert QraftGraph.objects.get(id=stale).status == GraphStatus.RUNNING
        assert len(signal_log["graph_overdue"]) == 1

    def test_unset_threshold_sweeps_nothing(self, settings):
        settings.QRAFT_GRAPH_OVERDUE_AFTER = None
        settings.QRAFT_RUN_OVERDUE_AFTER = None
        self._running_graph(started_at=timezone.now() - timezone.timedelta(days=1))
        assert graphs.flag_overdue() == 0

    def test_the_reaper_sweep_runs_it(self, settings):
        from qraft.reaper import reap_orphans

        settings.QRAFT_GRAPH_OVERDUE_AFTER = 60
        graph_id = self._running_graph(
            started_at=timezone.now() - timezone.timedelta(minutes=5)
        )

        reap_orphans(stale_after=60)

        assert QraftGraph.objects.get(id=graph_id).overdue_flagged_at is not None


class TestSummary:
    def test_summary_snapshots_every_node_and_the_graph_duration(
        self, django_capture_on_commit_callbacks
    ):
        builder = build_graph()
        add_node(builder, "ingest")
        add_node(builder, "rules", after=("ingest",))
        graph_id = builder.start()

        # Skipped while still pending: the stage nothing can run.
        graphs.skip(graph_id, "rules", reason="no ACTIVE ruleset versions")
        with django_capture_on_commit_callbacks(execute=True):
            complete_node(graph_id, "ingest")

        summary = QraftGraph.objects.get(id=graph_id).summary
        assert summary["outcome"] == GraphStatus.SUCCEEDED
        assert summary["duration_s"] >= 0
        by_key = {node["key"]: node for node in summary["nodes"]}
        assert set(by_key) == {"ingest", "rules"}
        assert by_key["ingest"]["outcome"] == NodeStatus.SUCCEEDED
        assert by_key["ingest"]["duration_s"] >= 0
        assert by_key["rules"]["outcome"] == NodeStatus.SKIPPED
        assert by_key["rules"]["skip_reason"] == "no ACTIVE ruleset versions"
        # A skipped node never ran, so it has no task and no duration.
        assert by_key["rules"]["task_id"] is None
