"""Tests for qraft.brokers (priority lanes on the django_q ORM broker)."""

from unittest.mock import patch

import pytest
from django_q.conf import Conf

from qraft.brokers import QraftOrmBroker, priority_list_key
from qraft.models import QraftTask


@pytest.mark.django_db
class TestQraftOrmBrokerDequeue:
    """Tests for lane-priority draining in QraftOrmBroker.dequeue()."""

    def test_drains_high_before_default_before_low(self):
        base = Conf.CLUSTER_NAME
        high = QraftOrmBroker(list_key=f"{base}--high")
        default = QraftOrmBroker(list_key=base)
        low = QraftOrmBroker(list_key=f"{base}--low")

        # Enqueue out of order to prove drain order is lane-driven, not FIFO.
        low.enqueue("low-payload")
        default.enqueue("default-payload")
        high.enqueue("high-payload")

        broker = QraftOrmBroker(list_key=base)

        first = broker.dequeue()
        assert [payload for _, payload in first] == ["high-payload"]

        second = broker.dequeue()
        assert [payload for _, payload in second] == ["default-payload"]

        third = broker.dequeue()
        assert [payload for _, payload in third] == ["low-payload"]

    def test_dequeue_empty_returns_none(self):
        broker = QraftOrmBroker(list_key=Conf.CLUSTER_NAME)
        with patch("qraft.brokers.sleep"):
            assert broker.dequeue() is None

    def test_only_checks_suffixed_lanes(self):
        """A task queued under an unrelated key is never picked up."""
        base = Conf.CLUSTER_NAME
        other = QraftOrmBroker(list_key=f"{base}-unrelated")
        other.enqueue("stray-payload")

        broker = QraftOrmBroker(list_key=base)
        with patch("qraft.brokers.sleep"):
            assert broker.dequeue() is None


class TestPriorityListKey:
    """Tests for the priority -> list_key mapping helper."""

    def test_high_and_low_are_suffixed(self):
        base = Conf.CLUSTER_NAME
        assert priority_list_key("high") == f"{base}--high"
        assert priority_list_key("low") == f"{base}--low"

    def test_default_is_none(self):
        assert priority_list_key("default") is None


@pytest.mark.django_db
class TestAsyncTaskPriorityRouting:
    """Tests that async_task() routes priority options to the right lane."""

    @patch("qraft.tasks.q2_async_task")
    def test_high_priority_stores_priority_and_uses_suffixed_broker(
        self, mock_q2_async
    ):
        from qraft.tasks import async_task

        mock_q2_async.return_value = "q2-task-high"

        async_task("test.function", qraft_options={"priority": "high"})

        qraft_task = QraftTask.objects.get()
        assert qraft_task.priority == "high"

        call_kwargs = mock_q2_async.call_args[1]
        broker = call_kwargs["broker"]
        assert isinstance(broker, QraftOrmBroker)
        assert broker.list_key == f"{Conf.CLUSTER_NAME}--high"

    @patch("qraft.tasks.q2_async_task")
    def test_low_priority_uses_suffixed_broker(self, mock_q2_async):
        from qraft.tasks import async_task

        mock_q2_async.return_value = "q2-task-low"

        async_task("test.function", qraft_options={"priority": "low"})

        call_kwargs = mock_q2_async.call_args[1]
        broker = call_kwargs["broker"]
        assert isinstance(broker, QraftOrmBroker)
        assert broker.list_key == f"{Conf.CLUSTER_NAME}--low"

    @patch("qraft.tasks.q2_async_task")
    def test_default_priority_does_not_override_broker(self, mock_q2_async):
        from qraft.tasks import async_task

        mock_q2_async.return_value = "q2-task-default"

        async_task("test.function")

        qraft_task = QraftTask.objects.get()
        assert qraft_task.priority == "default"

        call_kwargs = mock_q2_async.call_args[1]
        assert "broker" not in call_kwargs

    @patch("qraft.tasks.q2_async_task")
    def test_explicit_broker_wins_over_priority(self, mock_q2_async):
        from qraft.tasks import async_task

        mock_q2_async.return_value = "q2-task-explicit"
        explicit_broker = object()

        async_task(
            "test.function",
            broker=explicit_broker,
            qraft_options={"priority": "high"},
        )

        call_kwargs = mock_q2_async.call_args[1]
        assert call_kwargs["broker"] is explicit_broker

    @patch("qraft.tasks.q2_async_task")
    def test_invalid_priority_raises(self, mock_q2_async):
        from qraft.tasks import async_task

        with pytest.raises(ValueError, match="Invalid priority"):
            async_task("test.function", qraft_options={"priority": "urgent"})
