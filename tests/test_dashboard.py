"""Tests for the bundled monitoring dashboard (qraft.dashboard)."""

import json
import uuid
from datetime import timedelta
from unittest.mock import patch

import pytest
from django.contrib.auth.models import User
from django.test import RequestFactory, override_settings
from django.utils import timezone

from qraft.chain import QraftChain
from qraft.dashboard import metrics, views
from qraft.models import (
    QraftBatchModel,
    QraftIterModel,
    QraftTask,
    QraftTaskAttempt,
    RateBucket,
    TaskStatus,
    WorkflowStatus,
)
from qraft.models.tasks import AttemptState

PUBLIC = {"public": True}


def _task(func="app.tasks.crunch", status=TaskStatus.PENDING, **kwargs):
    return QraftTask.objects.create(func=func, status=status, **kwargs)


def _attempt(task, number=1, **kwargs):
    kwargs.setdefault("q2_task_id", uuid.uuid4().hex)
    return QraftTaskAttempt.objects.create(
        qraft_task=task, attempt_number=number, **kwargs
    )


def _backdate(task, seconds):
    """Push date_created into the past; auto_now_add ignores create kwargs."""
    created = timezone.now() - timedelta(seconds=seconds)
    QraftTask.objects.filter(id=task.id).update(date_created=created)
    task.refresh_from_db()
    return created


class TestAuth:
    def test_page_redirects_anonymous_to_login(self, db, client):
        response = client.get("/qraft/")
        assert response.status_code == 302
        assert "login" in response["Location"]

    def test_state_returns_403_json_for_anonymous(self, db, client):
        response = client.get("/qraft/api/state/")
        assert response.status_code == 403
        assert response.json() == {"error": "staff required"}

    def test_metrics_returns_403_json_for_anonymous(self, db, client):
        assert client.get("/qraft/api/metrics/").status_code == 403

    def test_action_returns_403_json_for_anonymous(self, db, client):
        response = client.post(f"/qraft/dlq/{uuid.uuid4()}/requeue/")
        assert response.status_code == 403

    def test_staff_user_allowed(self, db):
        user = User.objects.create_user("op", is_staff=True)
        request = RequestFactory().get("/qraft/api/state/")
        request.user = user
        assert views.state(request).status_code == 200

    def test_staff_user_can_render_page(self, db):
        user = User.objects.create_user("op2", is_staff=True)
        request = RequestFactory().get("/qraft/")
        request.user = user
        response = views.dashboard(request)
        assert response.status_code == 200
        assert b"QRAFT" in response.content

    def test_inactive_staff_blocked(self, db):
        user = User.objects.create_user("gone", is_staff=True, is_active=False)
        request = RequestFactory().get("/qraft/api/state/")
        request.user = user
        assert views.state(request).status_code == 403

    def test_non_staff_user_blocked(self, db):
        user = User.objects.create_user("pleb")
        request = RequestFactory().get("/qraft/api/state/")
        request.user = user
        assert views.state(request).status_code == 403

    @override_settings(QRAFT_DASHBOARD=PUBLIC)
    def test_public_flag_disables_gate(self, db, client):
        assert client.get("/qraft/").status_code == 200
        assert client.get("/qraft/api/state/").status_code == 200
        assert client.get("/qraft/api/metrics/").status_code == 200


@pytest.fixture
def public(settings):
    settings.QRAFT_DASHBOARD = PUBLIC


@pytest.mark.usefixtures("public")
class TestStateEndpoint:
    def test_state_shape(self, db, client):
        now = timezone.now()

        running = _task(status=TaskStatus.RUNNING)
        running.progress = {"current": 3, "total": 10, "message": "chewing"}
        running.retry_policy = {"max_attempts": 4}
        running.save()
        _attempt(
            running,
            date_started=now - timedelta(seconds=10),
            heartbeat_at=now - timedelta(seconds=2),
        )

        done = _task(func="app.tasks.summarize", status=TaskStatus.SUCCEEDED)
        _attempt(
            done,
            success=True,
            date_started=now - timedelta(seconds=8),
            date_completed=now - timedelta(seconds=3),
            usage={"input_tokens": 100, "cost": 0.5, "model": "m1"},
        )

        pending = _task(status=TaskStatus.PENDING)
        _attempt(
            pending,
            q2_task_id=None,
            state=AttemptState.SCHEDULED,
            not_before=now + timedelta(seconds=30),
        )

        dead = _task(func="app.tasks.flaky", status=TaskStatus.EXHAUSTED)
        _attempt(dead, success=False, exception_class="ValueError")

        chain = QraftChain()
        chain.append("app.tasks.extract", requires_approval=True)
        chain.append("app.tasks.load")
        with patch("qraft.tasks.q2_async_task"):
            chain.run()

        RateBucket.objects.create(key="openai", tokens=3.5)

        state = client.get("/qraft/api/state/").json()

        assert state["counts"]["running"] == 1
        assert state["counts"]["succeeded"] == 1
        assert state["counts"]["exhausted"] == 1
        assert state["queued"] == 0
        assert state["scheduled"] == 1
        assert 25 <= state["next_due_in"] <= 30
        assert state["legacy_scheduled"] == 0

        by_func = {task["func"]: task for task in state["tasks"]}
        row = by_func["crunch"]
        assert row["status"] == "running"
        assert row["attempts"] == 1
        assert row["max_attempts"] == 4
        assert row["progress"] == "3/10"
        assert row["message"] == "chewing"
        assert 1 <= row["heartbeat"] <= 4
        assert by_func["flaky"]["error"] == "ValueError"

        chains = [wf for wf in state["workflows"] if wf["kind"] == "chain"]
        assert len(chains) == 1
        wf = chains[0]
        assert wf["status"] == WorkflowStatus.WAITING_APPROVAL
        assert wf["gated"] is True
        assert wf["can_approve"] is True
        assert wf["can_cancel"] is True
        assert wf["members"][0]["gated"] is True
        assert wf["members"][0]["status"] == "not queued"
        assert wf["counters"]["total"] == 2

        assert [entry["func"] for entry in state["dlq"]] == ["flaky"]
        assert state["buckets"] == [{"key": "openai", "tokens": 3.5}]
        # Tokens stay in the rollup; money moves to the cost summary, which is
        # what carries its coverage and whether it was estimated.
        assert state["usage"] == {"input_tokens": 100}
        assert state["cost"] == {
            "amount": "0.5",
            "currency": None,
            "coverage": "complete",
            "estimated": False,
        }

    def test_admin_links_degrade_without_admin(self, db, client):
        # django.contrib.admin is deliberately absent from test settings.
        _task()
        state = client.get("/qraft/api/state/").json()
        assert state["tasks"][0]["admin_url"] is None

    def test_chain_membership_tagged_on_task_row(self, db, client):
        chain = QraftChain()
        chain.append("app.tasks.extract")
        with patch("qraft.tasks.q2_async_task", return_value=uuid.uuid4().hex):
            chain.run()
        step = chain._model.steps.get(step_index=0)
        assert step.qraft_task_id is not None
        state = client.get("/qraft/api/state/").json()
        assert state["tasks"][0]["workflow"] == "chain"


@pytest.mark.usefixtures("public")
class TestMetrics:
    def test_pickup_latency_percentiles(self, db, client):
        for seconds in (5, 15):
            task = _task()
            created = _backdate(task, 100)
            _attempt(task, date_started=created + timedelta(seconds=seconds))

        data = client.get("/qraft/api/metrics/?window=1h").json()
        assert data["window"] == "1h"
        assert data["pickup"]["n"] == 2
        assert data["pickup"]["p50"] == pytest.approx(5, abs=0.01)
        assert data["pickup"]["p95"] == pytest.approx(15, abs=0.01)

    def test_dispatch_lag(self, db, client):
        now = timezone.now()
        task = _task()
        _attempt(
            task,
            not_before=now - timedelta(seconds=60),
            date_started=now - timedelta(seconds=58),
        )
        data = client.get("/qraft/api/metrics/").json()
        assert data["dispatch_lag"]["n"] == 1
        assert data["dispatch_lag"]["p50"] == pytest.approx(2, abs=0.01)

    def test_retry_rate(self, db, client):
        retried = _task(status=TaskStatus.SUCCEEDED)
        _attempt(retried, 1, success=False, exception_class="ValueError")
        _attempt(retried, 2, success=True)
        clean = _task(status=TaskStatus.SUCCEEDED)
        _attempt(clean, 1, success=True)
        _task(status=TaskStatus.RUNNING)  # not settled: excluded

        data = client.get("/qraft/api/metrics/").json()
        assert data["retry"] == {"settled": 2, "retried": 1, "rate": 0.5}

    def test_exception_leaderboard(self, db, client):
        now = timezone.now()
        for exc in ("ValueError", "ValueError", "KeyError"):
            task = _task(status=TaskStatus.FAILED)
            _attempt(
                task,
                success=False,
                exception_class=exc,
                date_completed=now - timedelta(seconds=1),
            )
        data = client.get("/qraft/api/metrics/").json()
        assert data["exceptions"][0] == {"exception_class": "ValueError", "n": 2}
        assert data["exceptions"][1] == {"exception_class": "KeyError", "n": 1}

    def test_per_function_stats(self, db, client):
        now = timezone.now()
        for success, duration in ((True, 1), (False, 3)):
            task = _task(func="app.tasks.alpha", status=TaskStatus.SUCCEEDED)
            _attempt(
                task,
                success=success,
                date_started=now - timedelta(seconds=duration + 10),
                date_completed=now - timedelta(seconds=10),
            )
        data = client.get("/qraft/api/metrics/").json()
        assert data["functions"] == [
            {
                "func": "app.tasks.alpha",
                "count": 2,
                "success_pct": 50.0,
                "p95": pytest.approx(3, abs=0.01),
            }
        ]

    def test_throughput_buckets_sum_to_completions(self, db, client):
        now = timezone.now()
        for seconds_ago in (30, 300, 3000):
            task = _task(status=TaskStatus.SUCCEEDED)
            _attempt(
                task,
                success=True,
                date_completed=now - timedelta(seconds=seconds_ago),
            )
        data = client.get("/qraft/api/metrics/?window=1h").json()
        assert sum(data["throughput"]["buckets"]) == 3
        assert data["throughput"]["per_minute"] == 0.05
        assert data["throughput"]["bucket_seconds"] == 3600 // metrics.SPARKLINE_BARS

    def test_unknown_window_falls_back_to_default(self, db, client):
        data = client.get("/qraft/api/metrics/?window=bogus").json()
        assert data["window"] == "1h"

    def test_empty_database(self, db, client):
        data = client.get("/qraft/api/metrics/?window=15m").json()
        assert data["pickup"] == {"n": 0, "p50": None, "p95": None}
        assert data["retry"]["rate"] is None
        assert data["functions"] == []


@pytest.mark.usefixtures("public")
class TestActions:
    def test_requeue_moves_dead_task_back_to_pending(self, db, client):
        task = _task(status=TaskStatus.EXHAUSTED)
        _attempt(task, success=False, exception_class="ValueError")

        response = client.post(f"/qraft/dlq/{task.id}/requeue/")
        assert response.status_code == 200
        assert response.json()["requeued"] == str(task.id)

        task.refresh_from_db()
        assert task.status == TaskStatus.PENDING
        new = task.attempts.get(attempt_number=2)
        assert new.state == AttemptState.SCHEDULED

    def test_requeue_rejects_live_task(self, db, client):
        task = _task(status=TaskStatus.RUNNING)
        response = client.post(f"/qraft/dlq/{task.id}/requeue/")
        assert response.status_code == 409

    def test_requeue_unknown_task_404(self, db, client):
        assert client.post(f"/qraft/dlq/{uuid.uuid4()}/requeue/").status_code == 404

    def _parked_chain(self):
        chain = QraftChain()
        chain.append("app.tasks.extract", requires_approval=True)
        chain.append("app.tasks.load")
        with patch("qraft.tasks.q2_async_task"):
            chain.run()
        return chain

    def test_approve_resumes_parked_chain(self, db, client):
        chain = self._parked_chain()
        with patch("qraft.tasks.q2_async_task", return_value=uuid.uuid4().hex):
            response = client.post(f"/qraft/chains/{chain.id}/approve/")
        assert response.status_code == 200
        assert chain.status == WorkflowStatus.RUNNING
        assert chain._model.steps.get(step_index=0).qraft_task_id is not None

    def test_approve_not_parked_chain_conflicts(self, db, client):
        chain = self._parked_chain()
        with patch("qraft.tasks.q2_async_task", return_value=uuid.uuid4().hex):
            client.post(f"/qraft/chains/{chain.id}/approve/")
        assert client.post(f"/qraft/chains/{chain.id}/approve/").status_code == 409

    def test_approve_unknown_chain_404(self, db, client):
        assert client.post(f"/qraft/chains/{uuid.uuid4()}/approve/").status_code == 404

    def test_reject_cancels_parked_chain(self, db, client):
        chain = self._parked_chain()
        response = client.post(f"/qraft/chains/{chain.id}/reject/")
        assert response.status_code == 200
        assert chain.status == WorkflowStatus.CANCELLED

    def test_reject_is_idempotent(self, db, client):
        chain = self._parked_chain()
        client.post(f"/qraft/chains/{chain.id}/reject/")
        response = client.post(f"/qraft/chains/{chain.id}/reject/")
        assert response.status_code == 200
        assert response.json()["already"] is True

    def test_cancel_running_iter(self, db, client):
        row = QraftIterModel.objects.create(
            func="app.tasks.alpha", status=WorkflowStatus.RUNNING, total_count=2
        )
        response = client.post(f"/qraft/workflows/iter/{row.id}/cancel/")
        assert response.status_code == 200
        row.refresh_from_db()
        assert row.status == WorkflowStatus.CANCELLED

    def test_cancel_is_idempotent(self, db, client):
        row = QraftBatchModel.objects.create(status=WorkflowStatus.RUNNING)
        client.post(f"/qraft/workflows/batch/{row.id}/cancel/")
        response = client.post(f"/qraft/workflows/batch/{row.id}/cancel/")
        assert response.status_code == 200
        assert response.json()["already"] is True

    def test_cancel_terminal_workflow_conflicts(self, db, client):
        row = QraftIterModel.objects.create(
            func="app.tasks.alpha", status=WorkflowStatus.SUCCEEDED
        )
        response = client.post(f"/qraft/workflows/iter/{row.id}/cancel/")
        assert response.status_code == 409

    def test_cancel_unknown_kind_404(self, db, client):
        url = f"/qraft/workflows/nope/{uuid.uuid4()}/cancel/"
        assert client.post(url).status_code == 404

    def test_cancel_unknown_id_404(self, db, client):
        url = f"/qraft/workflows/iter/{uuid.uuid4()}/cancel/"
        assert client.post(url).status_code == 404

    def test_actions_require_post(self, db, client):
        task = _task(status=TaskStatus.EXHAUSTED)
        assert client.get(f"/qraft/dlq/{task.id}/requeue/").status_code == 405


def test_state_payload_is_json_serializable_roundtrip(db, client, public):
    raw = client.get("/qraft/api/state/").content
    payload = json.loads(raw)
    assert set(payload) >= {
        "now",
        "counts",
        "queued",
        "scheduled",
        "next_due_in",
        "legacy_scheduled",
        "tasks",
        "workflows",
        "dlq",
        "buckets",
        "usage",
    }


@pytest.mark.usefixtures("public")
class TestSubjectFilterAndProgress:
    def test_filters_progress_age_and_metrics_health(self, db, client):
        now = timezone.now()
        mine = _task(
            status=TaskStatus.RUNNING, subject_type="worksheet", subject_id="1"
        )
        _attempt(
            mine,
            date_started=now - timedelta(seconds=30),
            progress={"current": 4, "total": 8, "message": "scoring"},
            progress_advanced_at=now - timedelta(seconds=20),
        )
        other = _task(subject_type="worksheet", subject_id="2")
        other.progress = {"current": 1, "total": 2}  # pre-1.4 snapshot only
        other.save()
        _attempt(other)
        _task()
        QraftIterModel.objects.create(
            func="app.tasks.x", subject_type="worksheet", subject_id="1"
        )
        QraftBatchModel.objects.create(subject_type="worksheet", subject_id="2")

        state = client.get("/qraft/api/state/").json()
        assert len(state["tasks"]) == 3
        assert state["filters"] == {}
        assert state["metrics_health"] == {"failures": 0, "last_error": None}
        by_id = {row["id"]: row for row in state["tasks"]}
        assert by_id[str(mine.id)]["progress"] == "4/8"
        assert 19 <= by_id[str(mine.id)]["advanced_age"] <= 22
        assert by_id[str(other.id)]["progress"] == "1/2"
        assert by_id[str(other.id)]["advanced_age"] is None

        filtered = client.get(
            "/qraft/api/state/?subject_type=worksheet&subject_id=1"
        ).json()
        assert filtered["filters"] == {"subject_type": "worksheet", "subject_id": "1"}
        assert [row["id"] for row in filtered["tasks"]] == [str(mine.id)]
        assert [wf["kind"] for wf in filtered["workflows"]] == ["iter"]
        assert filtered["workflows"][0]["subject_id"] == "1"


@pytest.mark.usefixtures("public")
class TestGraphsPanel:
    def test_graph_rows_carry_nodes_badges_and_the_previous_graph_link(
        self, db, client
    ):
        from qraft import graphs
        from qraft.models.graphs import GraphStatus, NodeStatus, QraftGraph

        def start_graph(subject, nodes, **kwargs):
            builder = graphs.Graph(subject=subject, **kwargs)
            for key in nodes:
                builder.node(
                    key,
                    "app.tasks.ingest",
                    after=() if key == nodes[0] else (nodes[0],),
                    recovery="transactional",
                )
            return builder.start()

        first = start_graph(("worksheet", "1"), ["ingest"])
        graphs.cancel(first)
        second = start_graph(
            ("worksheet", "1"),
            ["ingest", "rules"],
            kind="shadow",
            previous_graph=first,
        )
        graphs.skip(second, "rules", reason="nothing undecided")
        QraftGraph.objects.filter(id=second).update(overdue_flagged_at=timezone.now())
        start_graph(("worksheet", "2"), ["ingest"])

        state = client.get("/qraft/api/state/").json()
        assert len(state["graphs"]) == 3
        row = next(r for r in state["graphs"] if r["id"] == second)
        assert row["kind"] == "shadow"
        assert row["status"] == GraphStatus.RUNNING
        assert row["overdue"] is True
        assert row["can_cancel"] is True
        assert row["previous_graph_id"] == first
        assert row["elapsed"] >= 0
        assert [(n["key"], n["status"]) for n in row["nodes"]] == [
            ("ingest", NodeStatus.RUNNING),
            ("rules", NodeStatus.SKIPPED),
        ]

        filtered = client.get(
            "/qraft/api/state/?subject_type=worksheet&subject_id=1"
        ).json()
        assert {r["id"] for r in filtered["graphs"]} == {first, second}
        by_graph = client.get(f"/qraft/api/state/?graph={second}").json()
        assert [r["id"] for r in by_graph["graphs"]] == [second]

    def test_cancel_action(self, db, client):
        from qraft import graphs
        from qraft.models.graphs import GraphStatus, QraftGraph

        builder = graphs.Graph(subject=("worksheet", "1"))
        builder.node("ingest", "app.tasks.ingest", recovery="transactional")
        graph_id = builder.start()

        assert client.post(f"/qraft/graphs/{graph_id}/cancel/").status_code == 200
        assert QraftGraph.objects.get(id=graph_id).status == GraphStatus.CANCELLED
        # Repeating it is a conflict, not a silent second settlement.
        assert client.post(f"/qraft/graphs/{graph_id}/cancel/").status_code == 409
        assert client.get(f"/qraft/graphs/{graph_id}/cancel/").status_code == 405

        unknown = "00000000-0000-0000-0000-000000000000"
        assert client.post(f"/qraft/graphs/{unknown}/cancel/").status_code == 404
        assert client.post(f"/qraft/graphs/nope/{graph_id}/").status_code == 404


    def test_resume_action_carries_a_preview_and_reruns(self, db, client):
        from qraft import graphs
        from qraft.models import QraftTask, QraftTaskAttempt, TaskStatus
        from qraft.models.graphs import (
            GraphStatus,
            NodeStatus,
            QraftGraph,
            QraftGraphNode,
        )

        builder = graphs.Graph(subject=("worksheet", "9"))
        builder.node("ingest", "app.tasks.ingest", recovery="transactional")
        builder.node(
            "rules", "app.tasks.ingest", after=("ingest",), recovery="transactional"
        )
        graph_id = builder.start()

        node = QraftGraphNode.objects.get(graph_id=graph_id, key="ingest")
        task = QraftTask.objects.get(pk=node.task_id)
        task.status = TaskStatus.EXHAUSTED
        task.save(update_fields=["status"])
        attempt = task.attempts.order_by("-attempt_number").first()
        QraftTaskAttempt.objects.filter(pk=attempt.pk).update(
            success=False, date_completed=timezone.now()
        )
        attempt.refresh_from_db()
        graphs.handle_node_completion(task, attempt)
        assert QraftGraph.objects.get(id=graph_id).status == GraphStatus.FAILED

        state = client.get("/qraft/api/state/").json()
        row = next(r for r in state["graphs"] if r["id"] == str(graph_id))
        assert row["can_resume"] is True
        assert row["resume_preview"]["rerun"] == ["ingest"]

        assert client.post(f"/qraft/graphs/{graph_id}/resume/").status_code == 200
        graph = QraftGraph.objects.get(id=graph_id)
        assert graph.status == GraphStatus.RUNNING
        assert graph.generation == 2
        assert (
            QraftGraphNode.objects.get(graph_id=graph_id, key="ingest").status
            == NodeStatus.RUNNING
        )
        # A running graph has nothing to resume.
        assert client.post(f"/qraft/graphs/{graph_id}/resume/").status_code == 409


@pytest.mark.usefixtures("public")
class TestStallBadge:
    def test_a_flagged_attempt_shows_stalled_then_recovered(self, db, client):
        now = timezone.now()
        stalled = _task(status=TaskStatus.RUNNING, func="app.tasks.score")
        _attempt(
            stalled,
            date_started=now - timedelta(minutes=10),
            progress_advanced_at=now - timedelta(minutes=9),
            stall_suspected_at=now - timedelta(minutes=1),
        )
        recovered = _task(status=TaskStatus.RUNNING, func="app.tasks.score")
        _attempt(
            recovered,
            date_started=now - timedelta(minutes=10),
            stall_suspected_at=now - timedelta(minutes=5),
            progress_advanced_at=now - timedelta(seconds=10),
        )
        healthy = _task(status=TaskStatus.RUNNING, func="app.tasks.score")
        _attempt(healthy, date_started=now - timedelta(minutes=1))

        rows = {
            row["id"]: row for row in client.get("/qraft/api/state/").json()["tasks"]
        }
        assert (
            rows[str(stalled.id)]["stalled"],
            rows[str(stalled.id)]["stall_recovered"],
        ) == (
            True,
            False,
        )
        assert (
            rows[str(recovered.id)]["stalled"],
            rows[str(recovered.id)]["stall_recovered"],
        ) == (True, True)
        assert rows[str(healthy.id)]["stalled"] is False


@pytest.mark.usefixtures("public")
class TestProgressOwnership:
    def test_a_superseded_snapshot_is_not_shown_beside_a_newer_attempt(
        self, db, client
    ):
        """
        `QraftTask.progress` is whatever attempt wrote it last. Showing it
        beside a newer attempt reports a superseded attempt's numbers as the
        running one's.
        """
        task = _task(status=TaskStatus.RUNNING, func="app.tasks.score")
        first = _attempt(task, progress={"current": 9, "total": 10})
        task.progress = {"current": 9, "total": 10, "attempt_id": str(first.id)}
        task.save(update_fields=["progress"])
        second = QraftTaskAttempt.objects.create(
            qraft_task=task, attempt_number=2, q2_task_id="q2-second"
        )

        (row,) = client.get("/qraft/api/state/").json()["tasks"]
        assert row["progress"] == ""

        # Once the current attempt reports, its own numbers show.
        QraftTaskAttempt.objects.filter(id=second.id).update(
            progress={"current": 1, "total": 10}
        )
        (row,) = client.get("/qraft/api/state/").json()["tasks"]
        assert row["progress"] == "1/10"

    def test_a_pre_1_4_snapshot_with_no_attempt_id_still_shows(self, db, client):
        task = _task(status=TaskStatus.RUNNING, func="app.tasks.score")
        task.progress = {"current": 3, "total": 4}
        task.save(update_fields=["progress"])
        _attempt(task)

        (row,) = client.get("/qraft/api/state/").json()["tasks"]
        assert row["progress"] == "3/4"


@pytest.mark.usefixtures("public")
class TestGraphNodeLinks:
    """A node failure is one click from the graph row, and named on it."""

    def test_a_task_node_carries_its_task_link_and_exception(self, db, client):
        from qraft import graphs
        from qraft.models.graphs import NodeStatus, QraftGraphNode

        builder = graphs.Graph(subject=("worksheet", "7"))
        builder.node("ingest", "revenue.tasks.ingest", recovery="transactional")
        builder.node(
            "rules", "revenue.tasks.rules", after=("ingest",), recovery="transactional"
        )
        graph_id = builder.start()

        node = QraftGraphNode.objects.get(graph_id=graph_id, key="ingest")
        task = node.task
        from qraft.models import TaskStatus

        task.status = TaskStatus.EXHAUSTED
        task.save(update_fields=["status"])
        from qraft.models import QraftTaskAttempt

        QraftTaskAttempt.objects.filter(qraft_task=task).update(
            success=False, exception_class="IngestError"
        )
        QraftGraphNode.objects.filter(pk=node.pk).update(status=NodeStatus.FAILED)

        row = next(
            r
            for r in client.get("/qraft/api/state/").json()["graphs"]
            if r["id"] == graph_id
        )
        ingest = next(n for n in row["nodes"] if n["key"] == "ingest")
        assert ingest["task_id"] == str(task.id)
        assert ingest["error"] == "IngestError"
        # The admin is deliberately not installed in the test project, so the
        # link degrades to None rather than raising.
        assert ingest["task_url"] is None

        pending = next(n for n in row["nodes"] if n["key"] == "rules")
        assert pending["task_id"] == ""
        assert pending["task_url"] is None
        assert pending["error"] == ""
