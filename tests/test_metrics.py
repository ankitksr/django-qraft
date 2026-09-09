"""Tests for the metrics sink: resolution, isolation, emission points, labels."""

from unittest.mock import Mock, patch

import pytest

from qraft import metrics
from qraft.metrics import FORBIDDEN_LABELS, NullSink
from qraft.metrics.otel import OpenTelemetrySink
from qraft.models import QraftTask, QraftTaskAttempt, TaskStatus


class TestSinkResolution:
    def test_default_is_null_sink_and_dotted_path_resolves(self, monkeypatch):
        from qraft.conf import get_conf

        assert isinstance(metrics.get_sink(), NullSink)
        metrics.reset_sink()
        monkeypatch.setattr(get_conf(), "metrics_sink", "tests.conftest.RecordingSink")
        sink = metrics.get_sink()
        assert type(sink).__name__ == "RecordingSink"
        assert metrics.get_sink() is sink


class TestFailureIsolation:
    def test_raising_sink_is_counted_logged_once_and_kept(self, monkeypatch, caplog):
        broken = Mock()
        broken.counter.side_effect = RuntimeError("exporter down")
        monkeypatch.setattr(metrics, "get_sink", lambda: broken)

        with caplog.at_level("WARNING", logger="qraft.metrics"):
            for _ in range(3):
                metrics.counter("qraft.test", func="f")

        assert metrics.health() == {
            "failures": 3,
            "last_error": "RuntimeError: exporter down",
        }
        assert caplog.text.count("Metrics sink failed") == 1
        # Still called every time: a transient failure never disables the sink.
        assert broken.counter.call_count == 3

    def test_forbidden_label_never_reaches_the_sink(self, recording_sink):
        metrics.counter("qraft.test", subject_id="4117")
        metrics.histogram("qraft.test", 1.0, task_id="abc")
        assert recording_sink.calls == []
        assert metrics.health()["failures"] == 2
        assert "task_id" in metrics.health()["last_error"]


@pytest.mark.django_db
class TestEmissionPoints:
    def _task(self, **kwargs):
        kwargs.setdefault("func", "app.tasks.crunch")
        kwargs.setdefault("status", TaskStatus.RUNNING)
        kwargs.setdefault("subject_type", "worksheet")
        kwargs.setdefault("subject_id", "4117")
        return QraftTask.objects.create(**kwargs)

    def _assert_no_id_labels(self, sink, task):
        for _, _, _, labels in sink.calls:
            assert not FORBIDDEN_LABELS.intersection(labels)
            assert str(task.id) not in labels.values()
            assert "4117" not in labels.values()

    def test_start_finish_retry_and_scheduler_emissions(
        self, recording_sink, django_capture_on_commit_callbacks
    ):
        from datetime import timedelta

        from django.utils import timezone

        from qraft import lease
        from qraft.hooks import qraft_hook_handler
        from qraft.scheduler import dispatch_due

        task = self._task(retry_policy={"max_attempts": 2, "base_delay": 0})
        attempt = QraftTaskAttempt.objects.create(
            qraft_task=task,
            attempt_number=1,
            q2_task_id="q2-m1",
            cluster="test",
            enqueued_at=timezone.now() - timedelta(seconds=2),
        )

        lease.stamp_start("q2-m1")
        # Re-entry inside one delivery (the marker path does this): the claim
        # is memoised, so the start is announced and counted exactly once. A
        # true redelivery is covered in test_lease.TestRedeliveryGuard.
        lease.stamp_start("q2-m1")
        assert recording_sink.names() == ["qraft.attempt.started", "qraft.attempt.pickup"]
        assert recording_sink.labels("qraft.attempt.started") == [
            {"func": "app.tasks.crunch", "cluster": "test"}
        ]
        assert recording_sink.values("qraft.attempt.pickup")[0] >= 2

        q2 = Mock(id="q2-m1", success=False, stopped=timezone.now())
        q2.result = "boom : Traceback\nValueError: x"
        # Resolution counters ride `on_commit` with the signals, so nothing is
        # emitted until the resolving transaction commits.
        with django_capture_on_commit_callbacks(execute=True):
            qraft_hook_handler(q2)
        assert recording_sink.labels("qraft.attempt.finished") == [
            {
                "func": "app.tasks.crunch",
                "cluster": "test",
                "outcome": "failed",
                "exception_class": "ValueError",
            }
        ]
        assert recording_sink.labels("qraft.attempt.duration")[0]["outcome"] == "failed"
        assert recording_sink.labels("qraft.retry.scheduled") == [
            {"func": "app.tasks.crunch", "exception_class": "ValueError"}
        ]

        with patch("qraft.scheduler.q2_async_task", return_value="q2-m2", create=True):
            with patch("django_q.tasks.async_task", return_value="q2-m2"):
                assert dispatch_due() == 1
        assert recording_sink.labels("qraft.scheduler.lag") == [{"cluster": "test"}]
        retry = QraftTaskAttempt.objects.get(attempt_number=2)
        assert retry.enqueued_at is not None
        self._assert_no_id_labels(recording_sink, task)
        assert attempt.id  # the first attempt row is untouched by the retry

    def test_reaper_and_workflow_emissions(
        self, recording_sink, django_capture_on_commit_callbacks
    ):
        from datetime import timedelta

        from django.utils import timezone

        from qraft.dispatchers import settle_workflow
        from qraft.models import QraftChainModel, WorkflowStatus
        from qraft.reaper import reap_orphans

        task = self._task()
        QraftTaskAttempt.objects.create(
            qraft_task=task,
            attempt_number=1,
            q2_task_id="q2-orphan",
            cluster="test",
            date_started=timezone.now() - timedelta(hours=2),
            heartbeat_at=timezone.now() - timedelta(hours=1),
        )
        with django_capture_on_commit_callbacks(execute=True):
            assert reap_orphans() == 1
        assert recording_sink.labels("qraft.attempt.finished")[0]["outcome"] == "orphaned"
        assert recording_sink.labels("qraft.attempt.duration")[0]["outcome"] == "orphaned"
        assert recording_sink.labels("qraft.reaper.action") == [{"action": "orphaned"}]

        chain = QraftChainModel.objects.create(status=WorkflowStatus.RUNNING)
        with django_capture_on_commit_callbacks(execute=True):
            assert settle_workflow(
                QraftChainModel, chain.id, WorkflowStatus.SUCCEEDED, "chain"
            )
            assert (
                settle_workflow(QraftChainModel, chain.id, WorkflowStatus.FAILED, "chain")
                is None
            )
        assert recording_sink.labels("qraft.workflow.settled") == [
            {"workflow_type": "chain", "outcome": "succeeded"}
        ]
        self._assert_no_id_labels(recording_sink, task)

    def test_a_rolled_back_transition_emits_no_counter(self, recording_sink):
        """
        Counters ride `on_commit` for the reason the signals do: a savepoint
        that rolls back undoes the row the count reports, and a count with no
        row behind it is worse than no count.
        """
        from django.db import transaction

        from qraft.dispatchers import settle_workflow
        from qraft.models import QraftChainModel, WorkflowStatus

        chain = QraftChainModel.objects.create(status=WorkflowStatus.RUNNING)
        with pytest.raises(RuntimeError):
            with transaction.atomic():
                settle_workflow(
                    QraftChainModel, chain.id, WorkflowStatus.SUCCEEDED, "chain"
                )
                raise RuntimeError("the caller failed after settling")

        chain.refresh_from_db()
        assert chain.settled_at is None
        assert recording_sink.names() == []

    def test_gauges_come_from_the_flagged_cluster_only(self, recording_sink, monkeypatch):
        import threading

        from qraft.conf import get_conf
        from qraft.metrics.gauges import emit_gauges
        from qraft.scheduler import dispatch_loop

        emit_gauges()
        gauges = recording_sink.names("gauge")
        assert set(gauges) == {
            "qraft.queue.depth",
            "qraft.queue.oldest_ready_age",
            "qraft.attempt.active",
            "qraft.scheduler.overdue",
            "qraft.attempt.unrouted_age_max",
        }
        assert recording_sink.labels("qraft.queue.depth") == [{"cluster": "test"}]
        assert recording_sink.labels("qraft.scheduler.overdue") == [{}]

        stop = threading.Event()
        calls = []
        monkeypatch.setattr("qraft.scheduler.dispatch_due", lambda: stop.set() or 0)
        monkeypatch.setattr("qraft.scheduler.time.sleep", lambda _s: None)
        monkeypatch.setattr("qraft.metrics.gauges.emit_gauges", lambda: calls.append(1))

        monkeypatch.setattr(get_conf(), "metrics_gauges", False)
        dispatch_loop(stop)
        assert calls == []

        stop.clear()
        monkeypatch.setattr(get_conf(), "metrics_gauges", True)
        dispatch_loop(stop)
        assert calls == [1]


class TestOpenTelemetrySink:
    def test_records_through_a_meter(self):
        meter = Mock()
        sink = OpenTelemetrySink(meter=meter)
        sink.counter("qraft.attempt.started", 1, func="f")
        sink.counter("qraft.attempt.started", 1, func="g")
        sink.histogram("qraft.attempt.duration", 0.5, func="f")
        sink.gauge("qraft.queue.depth", 3, cluster="a")

        meter.create_counter.assert_called_once_with("qraft.attempt.started")
        assert meter.create_counter.return_value.add.call_count == 2
        meter.create_histogram.assert_called_once_with("qraft.attempt.duration", unit="s")
        meter.create_observable_gauge.assert_called_once()
        callback = meter.create_observable_gauge.call_args.kwargs["callbacks"][0]
        observations = callback(None)
        assert [(o.value, o.attributes) for o in observations] == [(3, {"cluster": "a"})]

    def test_no_sdk_is_a_silent_no_op(self):
        sink = OpenTelemetrySink()
        sink.counter("qraft.attempt.started", func="f")
        sink.histogram("qraft.attempt.duration", 1.0, func="f")
        sink.gauge("qraft.queue.depth", 1, cluster="a")
