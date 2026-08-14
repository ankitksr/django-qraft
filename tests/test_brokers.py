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


@pytest.fixture
def _clear_lane_cache():
    """The lane-support check is cached per broker_class to warn only once."""
    from qraft.brokers import _lanes_drained_by

    _lanes_drained_by.cache_clear()
    yield
    _lanes_drained_by.cache_clear()


class TestPriorityLaneTargeting:
    """The lane key must name the target cluster, not the enqueuing process."""

    def test_lane_is_keyed_off_the_target_cluster(self):
        # django_q resolves `broker or get_broker(cluster)`, so an explicit
        # broker drops `cluster=` entirely - the lane has to carry it.
        assert priority_list_key("high", cluster="io-workers") == "io-workers--high"
        assert priority_list_key("low", cluster="io-workers") == "io-workers--low"

    def test_falls_back_to_the_local_cluster(self):
        assert priority_list_key("high") == f"{Conf.CLUSTER_NAME}--high"

    def test_default_priority_never_routes(self):
        assert priority_list_key("default", cluster="io-workers") is None


@pytest.mark.usefixtures("_clear_lane_cache")
class TestPriorityLaneAvailability:
    """Nothing drains the suffixed lanes unless broker_class says so."""

    def test_declines_to_route_without_the_qraft_broker(self, monkeypatch, caplog):
        from qraft.brokers import priority_lanes_available

        monkeypatch.setattr(Conf, "BROKER_CLASS", None)

        assert priority_lanes_available() is False
        assert priority_list_key("high") is None
        assert "does not drain Qraft priority lanes" in caplog.text

    def test_declines_for_an_unrelated_broker_class(self, monkeypatch):
        from qraft.brokers import priority_lanes_available

        monkeypatch.setattr(Conf, "BROKER_CLASS", "django_q.brokers.orm.ORM")

        assert priority_lanes_available() is False

    def test_accepts_a_subclass_of_the_qraft_broker(self, monkeypatch):
        from qraft.brokers import priority_lanes_available

        monkeypatch.setattr(Conf, "BROKER_CLASS", "qraft.brokers.QraftOrmBroker")

        assert priority_lanes_available() is True


@pytest.mark.usefixtures("_clear_lane_cache")
class TestPriorityLaneAvailabilityTargetsCluster:
    """
    A6: availability has to be resolved against the *target* cluster's own
    config, not the enqueuing process's Conf.BROKER_CLASS - those two only
    coincide when the target happens to be the process's own cluster.
    """

    def test_named_target_ignores_the_producers_own_broker_class(
        self, monkeypatch, settings
    ):
        from qraft.brokers import priority_lanes_available

        # Producer's own Conf says no qraft broker at all...
        monkeypatch.setattr(Conf, "BROKER_CLASS", None)
        # ...but the named target cluster runs QraftOrmBroker.
        settings.Q_CLUSTER = {
            **settings.Q_CLUSTER,
            "ALT_CLUSTERS": {
                "io-workers": {"broker_class": "qraft.brokers.QraftOrmBroker"}
            },
        }

        assert priority_lanes_available("io-workers") is True
        # The producer's own (unnamed) lane is unaffected by the target's.
        assert priority_lanes_available() is False

    def test_named_target_running_a_plain_broker_is_unavailable(self, settings):
        from qraft.brokers import priority_lanes_available

        # Producer's own broker (via tests/settings.py) is QraftOrmBroker...
        settings.Q_CLUSTER = {
            **settings.Q_CLUSTER,
            "ALT_CLUSTERS": {"io-workers": {"broker_class": None}},
        }

        # ...but "io-workers" runs the plain ORM broker and never drains
        # the suffixed lanes.
        assert priority_lanes_available("io-workers") is False
        assert priority_lanes_available() is True

    def test_named_target_without_an_alt_clusters_override_inherits_the_base(
        self, settings
    ):
        from qraft.brokers import priority_lanes_available

        assert "ALT_CLUSTERS" not in settings.Q_CLUSTER
        assert priority_lanes_available("some-other-cluster") is True


@pytest.mark.django_db
@pytest.mark.usefixtures("_clear_lane_cache")
class TestAsyncTaskClusterAwarePriority:
    """Regression: priority routing used to discard `cluster=` silently."""

    @patch("qraft.tasks.q2_async_task")
    def test_priority_lane_follows_the_target_cluster(self, mock_q2_async):
        from qraft.tasks import async_task

        mock_q2_async.return_value = "q2-routed"

        async_task(
            "test.function",
            qraft_options={"priority": "high", "cluster": "io-workers"},
        )

        call_kwargs = mock_q2_async.call_args[1]
        assert call_kwargs["broker"].list_key == "io-workers--high"
        assert call_kwargs["cluster"] == "io-workers"

    @patch("qraft.tasks.q2_async_task")
    def test_no_cluster_given_falls_back_to_the_producers_own_broker(
        self, mock_q2_async, monkeypatch
    ):
        """
        cluster=None means "route to my own cluster" - Conf.BROKER_CLASS
        already reflects that (django_q resolves its own ALT_CLUSTERS
        against this process's Q_CLUSTER_NAME at import time), so this
        producer-side check is still the right one when no target is named.
        """
        from qraft.tasks import async_task

        monkeypatch.setattr(Conf, "BROKER_CLASS", None)
        mock_q2_async.return_value = "q2-local-fallback"

        async_task("test.function", qraft_options={"priority": "high"})

        call_kwargs = mock_q2_async.call_args[1]
        assert "broker" not in call_kwargs
        # The requested priority is still recorded for observability.
        assert QraftTask.objects.get().priority == "high"

    @patch("qraft.tasks.q2_async_task")
    def test_target_cluster_running_a_plain_broker_is_not_stranded(
        self, mock_q2_async, settings
    ):
        """
        A6 regression: the producer's own broker is QraftOrmBroker (drains
        the suffixed lanes fine for itself), but this task is routed to an
        ALT cluster running the plain ORM broker - that cluster never drains
        "io-workers--high". Checking the producer's own Conf.BROKER_CLASS
        (as before) would miss this entirely and strand the task in a lane
        nobody polls; the lane availability has to follow the target.
        """
        from qraft.tasks import async_task

        settings.Q_CLUSTER = {
            **settings.Q_CLUSTER,
            "ALT_CLUSTERS": {"io-workers": {"broker_class": None}},
        }
        mock_q2_async.return_value = "q2-not-stranded"

        async_task(
            "test.function",
            qraft_options={"priority": "high", "cluster": "io-workers"},
        )

        call_kwargs = mock_q2_async.call_args[1]
        # No broker override: "io-workers" doesn't drain the suffixed lane,
        # so the task runs at default priority there instead of stranding.
        assert "broker" not in call_kwargs
        assert call_kwargs["cluster"] == "io-workers"
        # The requested priority is still recorded for observability.
        assert QraftTask.objects.get().priority == "high"
