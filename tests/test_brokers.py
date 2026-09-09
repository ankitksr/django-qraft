"""Tests for qraft.brokers (priority lanes on the django_q ORM broker)."""

from unittest.mock import patch

import pytest
from django.utils import timezone
from django_q.brokers import Broker
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
    def test_default_priority_uses_the_unsuffixed_lane(self, mock_q2_async):
        from qraft.tasks import async_task

        mock_q2_async.return_value = "q2-task-default"

        async_task("test.function")

        qraft_task = QraftTask.objects.get()
        assert qraft_task.priority == "default"

        # Still an explicit broker - every enqueue names one so it reaches the
        # target cluster - but on the plain lane.
        call_kwargs = mock_q2_async.call_args[1]
        assert call_kwargs["broker"].list_key == Conf.CLUSTER_NAME

    @patch("qraft.tasks.q2_async_task")
    def test_explicit_broker_wins_over_priority(self, mock_q2_async):
        from qraft.tasks import async_task

        mock_q2_async.return_value = "q2-task-explicit"
        explicit_broker = object()

        async_task(
            "test.function",
            broker=explicit_broker,
            qraft_options={"priority": "high", "cluster": "io-workers"},
        )

        call_kwargs = mock_q2_async.call_args[1]
        assert call_kwargs["broker"] is explicit_broker

    def test_an_explicit_broker_must_name_its_cluster(self, db):
        """
        Attempt 1 goes wherever the broker points, but the attempt row records
        the cluster - and every retry and DLQ requeue inherits that. Without a
        cluster the row would name the enqueuing process's, sending retries to
        a different cluster than the one that ran attempt 1.
        """
        from qraft.models import QraftTask
        from qraft.tasks import async_task

        with pytest.raises(ValueError, match="must also name the cluster"):
            async_task("test.function", broker=object())
        assert not QraftTask.objects.exists()

    @patch("qraft.tasks.q2_async_task")
    def test_an_explicit_broker_records_the_cluster_retries_inherit(
        self, mock_q2_async, db
    ):
        from qraft.models import QraftTask
        from qraft.scheduler import _inherited_cluster
        from qraft.tasks import async_task

        mock_q2_async.return_value = "q2-explicit-cluster"
        async_task("test.function", broker=object(), cluster="io-workers")

        task = QraftTask.objects.get()
        assert _inherited_cluster(task) == "io-workers"

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
        assert caplog.records[0].levelname == "WARNING"
        assert caplog.records[0].args == (None,)

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
        # No lane suffix: this process's own broker does not drain them.
        assert call_kwargs["broker"].list_key == Conf.CLUSTER_NAME
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
        # No lane suffix: "io-workers" doesn't drain the suffixed lane, so the
        # task runs at default priority there instead of stranding.
        assert call_kwargs["broker"].list_key == "io-workers"
        assert call_kwargs["cluster"] == "io-workers"
        # The requested priority is still recorded for observability.
        assert QraftTask.objects.get().priority == "high"


class StubBroker(Broker):
    """A broker that records what was enqueued, standing in for a live server."""

    instances: list["StubBroker"] = []

    def __init__(self, list_key=None):
        self.list_key = list_key or Conf.CLUSTER_NAME
        self._info = None
        self.cache = None
        self.enqueued = []
        StubBroker.instances.append(self)

    @staticmethod
    def get_connection(list_key=None):
        return object()

    def enqueue(self, task):
        self.enqueued.append(task)
        return f"stub-{len(self.enqueued)}"

    def acknowledge(self, task_id):
        return None


@pytest.fixture
def _clear_broker_cache():
    """Resolved brokers are cached per cluster; each test builds its own."""
    from qraft.brokers import reset_broker_cache

    StubBroker.instances = []
    reset_broker_cache()
    yield
    StubBroker.instances = []
    reset_broker_cache()


@pytest.mark.django_db
@pytest.mark.usefixtures("_clear_broker_cache")
class TestBrokerForCluster:
    """The resolver reads the target cluster's config, not this process's."""

    def test_each_broker_key_selects_its_class(self):
        """
        Class selection follows django_q's own order. Resolved without
        instantiating, so SQS/Mongo/IronMQ need neither a driver nor a server.
        """
        from qraft.brokers import _broker_class_for_config

        cases = [
            ({"broker_class": "tests.test_brokers.StubBroker"}, "StubBroker"),
            ({"iron_mq": {"token": "x"}, "orm": "default"}, "IronMQBroker"),
            ({"sqs": {"aws_region": "eu-west-1"}, "orm": "default"}, "Sqs"),
            ({"orm": "default", "mongo": {"host": "x"}}, "ORM"),
            ({"mongo": {"host": "x"}}, "Mongo"),
            ({}, "Redis"),
            ({"redis": {"host": "127.0.0.1"}}, "Redis"),
        ]
        for config, expected in cases:
            try:
                assert _broker_class_for_config(config).__name__ == expected
            except ImportError:  # optional driver (boto3, pymongo, iron_mq)
                continue

    def test_the_routing_broker_is_never_selected_as_the_delegate(self):
        from qraft.brokers import _broker_class_for_config

        selected = _broker_class_for_config(
            {"broker_class": "qraft.brokers.RoutingBroker", "orm": "default"}
        )
        assert selected.__name__ == "ORM"

    def test_an_alt_entry_overrides_the_base_broker(self, settings):
        from django_q.brokers.orm import ORM

        from qraft.brokers import broker_for_cluster, reset_broker_cache

        settings.Q_CLUSTER = {
            "name": "test",
            "orm": "default",
            "ALT_CLUSTERS": {
                "stubbed": {"broker_class": "tests.test_brokers.StubBroker"}
            },
        }
        reset_broker_cache()

        assert isinstance(broker_for_cluster("test"), ORM)
        assert isinstance(broker_for_cluster("stubbed"), StubBroker)

    def test_an_unknown_cluster_falls_back_to_the_base_config(self, settings):
        from qraft.brokers import broker_for_cluster, reset_broker_cache

        settings.Q_CLUSTER = {
            "name": "test",
            "broker_class": "tests.test_brokers.StubBroker",
            "ALT_CLUSTERS": {"known": {}},
        }
        reset_broker_cache()

        broker = broker_for_cluster("never-declared")
        assert isinstance(broker, StubBroker)
        assert broker.list_key == "never-declared"

    def test_the_instance_is_cached_per_cluster_and_lane(self):
        from qraft.brokers import broker_for_cluster

        assert broker_for_cluster("a") is broker_for_cluster("a")
        assert broker_for_cluster("a") is not broker_for_cluster("b")
        assert broker_for_cluster("a") is not broker_for_cluster("a", "high")

    def test_a_settings_change_invalidates_the_cache(self, settings):
        from django_q.brokers.orm import ORM

        from qraft.brokers import broker_for_cluster

        assert isinstance(broker_for_cluster("test"), ORM)
        settings.Q_CLUSTER = {
            "name": "test",
            "broker_class": "tests.test_brokers.StubBroker",
        }
        assert isinstance(broker_for_cluster("test"), StubBroker)

    def test_the_orm_alias_is_pinned_to_the_target_clusters_database(
        self, settings, monkeypatch
    ):
        """
        `ORM.get_connection` reads `Conf.ORM` on every call, so an unpinned
        instance would enqueue into the *enqueuing* process's alias.
        """
        from qraft.brokers import broker_for_cluster, reset_broker_cache

        settings.Q_CLUSTER = {
            "name": "test",
            "redis": {"host": "127.0.0.1"},
            "ALT_CLUSTERS": {"revenue": {"orm": "default"}},
        }
        reset_broker_cache()

        broker = broker_for_cluster("revenue")
        assert broker.qraft_orm_alias == "default"

        # The enqueuing process is not an ORM cluster at all; the pin wins.
        monkeypatch.setattr(Conf, "ORM", None)
        assert broker.get_connection().db == "default"

    def test_a_resolved_broker_survives_a_pickle_round_trip(self):
        """
        The cluster hands its broker to spawned worker processes, which pickle
        it on any non-fork start method. The pinned ORM alias has to come back.
        """
        import pickle

        from qraft.brokers import broker_for_cluster

        restored = pickle.loads(pickle.dumps(broker_for_cluster("test")))
        assert restored.list_key == "test"
        assert restored.qraft_orm_alias == "default"

    def test_the_priority_lane_rides_on_the_targets_own_broker(self, settings):
        from qraft.brokers import QraftOrmBroker, broker_for_cluster, reset_broker_cache

        settings.Q_CLUSTER = {
            "name": "test",
            "redis": {"host": "127.0.0.1"},
            "ALT_CLUSTERS": {
                "lanes": {
                    "orm": "default",
                    "broker_class": "qraft.brokers.QraftOrmBroker",
                }
            },
        }
        reset_broker_cache()

        broker = broker_for_cluster("lanes", "high")
        assert isinstance(broker, QraftOrmBroker)
        assert broker.list_key == "lanes--high"


@pytest.mark.django_db
@pytest.mark.usefixtures("_clear_broker_cache")
class TestEnqueueRouting:
    """An enqueue to cluster X must use X's broker instance, not this one's."""

    def test_async_task_enqueues_onto_the_target_clusters_broker(self, settings):
        from qraft.brokers import reset_broker_cache
        from qraft.tasks import async_task

        settings.Q_CLUSTER = {
            "name": "test",
            "orm": "default",
            "ALT_CLUSTERS": {
                "stubbed": {"broker_class": "tests.test_brokers.StubBroker"}
            },
        }
        reset_broker_cache()

        async_task("tests.e2e_tasks.succeed", 1, qraft_options={"cluster": "stubbed"})

        assert len(StubBroker.instances) == 1
        stub = StubBroker.instances[0]
        assert stub.list_key == "stubbed"
        assert len(stub.enqueued) == 1

    def test_the_scheduler_dispatches_onto_the_target_clusters_broker(self, settings):
        from qraft.brokers import reset_broker_cache
        from qraft.models import QraftTask, TaskStatus
        from qraft.scheduler import dispatch_due, schedule_attempt

        settings.Q_CLUSTER = {
            "name": "test",
            "orm": "default",
            "ALT_CLUSTERS": {
                "stubbed": {"broker_class": "tests.test_brokers.StubBroker"}
            },
        }
        reset_broker_cache()

        task = QraftTask.objects.create(
            func="tests.e2e_tasks.succeed",
            task_args=[1],
            task_kwargs={},
            status=TaskStatus.PENDING,
        )
        schedule_attempt(task, 1, timezone.now(), cluster="stubbed")

        assert dispatch_due() == 1
        assert [b.list_key for b in StubBroker.instances] == ["stubbed"]
        assert len(StubBroker.instances[0].enqueued) == 1

    def test_two_orm_clusters_use_distinct_list_keys(self, settings):
        """
        The mixed-broker deployment end to end: two ORM-backed clusters and a
        Redis-configured one, with nothing live behind the Redis entry.
        """
        from django_q.models import OrmQ

        from qraft.brokers import reset_broker_cache
        from qraft.tasks import async_task

        settings.Q_CLUSTER = {
            "name": "test",
            "orm": "default",
            "ALT_CLUSTERS": {
                "revenue": {"orm": "default"},
                "legacy": {"orm": None, "redis": {"host": "127.0.0.1"}},
                "stubbed": {"broker_class": "tests.test_brokers.StubBroker"},
            },
        }
        reset_broker_cache()

        async_task("tests.e2e_tasks.succeed", 1, qraft_options={"cluster": "revenue"})
        async_task("tests.e2e_tasks.succeed", 2)
        async_task("tests.e2e_tasks.succeed", 3, qraft_options={"cluster": "stubbed"})

        assert set(OrmQ.objects.values_list("key", flat=True)) == {"revenue", "test"}
        # The Redis-configured cluster is asserted through the stub instead of
        # a live server: resolving it must not touch the ORM queue.
        assert [b.list_key for b in StubBroker.instances] == ["stubbed"]


@pytest.mark.django_db
@pytest.mark.usefixtures("_clear_broker_cache")
class TestRoutingBroker:
    """Plain django_q enqueues route through the cluster's own broker."""

    def test_a_plain_q2_enqueue_lands_on_the_named_clusters_broker(self, settings):
        from django_q.tasks import async_task as q2_async_task

        from qraft.brokers import reset_broker_cache

        settings.Q_CLUSTER = {
            "name": "test",
            "broker_class": "qraft.brokers.RoutingBroker",
            "orm": "default",
            "ALT_CLUSTERS": {
                "stubbed": {"broker_class": "tests.test_brokers.StubBroker"}
            },
        }
        reset_broker_cache()

        with patch.object(Conf, "BROKER_CLASS", "qraft.brokers.RoutingBroker"):
            q2_async_task("tests.e2e_tasks.succeed", 1, cluster="stubbed")

        assert [b.list_key for b in StubBroker.instances] == ["stubbed"]
        assert len(StubBroker.instances[0].enqueued) == 1

    def test_the_router_never_recurses_into_itself(self, settings):
        from django_q.brokers.orm import ORM

        from qraft.brokers import RoutingBroker, broker_for_cluster, reset_broker_cache

        settings.Q_CLUSTER = {
            "name": "test",
            "broker_class": "qraft.brokers.RoutingBroker",
            "orm": "default",
        }
        reset_broker_cache()

        target = RoutingBroker(list_key="test").target
        assert isinstance(target, ORM)
        assert not isinstance(target, RoutingBroker)
        assert broker_for_cluster("test") is target

    def test_the_router_survives_a_pickle_round_trip(self, settings):
        import pickle

        from qraft.brokers import RoutingBroker, reset_broker_cache

        settings.Q_CLUSTER = {
            "name": "test",
            "broker_class": "qraft.brokers.RoutingBroker",
            "orm": "default",
        }
        reset_broker_cache()

        restored = pickle.loads(pickle.dumps(RoutingBroker(list_key="test")))
        assert restored.list_key == "test"
        assert restored.target.list_key == "test"

    def test_capability_checks_see_through_the_router(self, settings):
        """
        A forwarding method is still an override, so the receipts check has to
        ask the delegate or every routed cluster would claim receipts.
        """
        from qraft.brokers import RoutingBroker, reset_broker_cache
        from qraft.cluster import _broker_supports_receipts

        settings.Q_CLUSTER = {
            "name": "test",
            "broker_class": "qraft.brokers.RoutingBroker",
            "ALT_CLUSTERS": {
                "stubbed": {"broker_class": "tests.test_brokers.StubBroker"}
            },
        }
        reset_broker_cache()

        # StubBroker overrides acknowledge but not fail: receipts supported.
        assert _broker_supports_receipts(RoutingBroker(list_key="stubbed"))


class TestClusterProcessConfig:
    """`qraftcluster` re-execs with Q_CLUSTER_NAME so Conf carries the ALT entry."""

    def test_conf_reflects_the_merged_alt_entry(self, monkeypatch):
        import importlib

        import django_q.conf

        monkeypatch.setenv("Q_CLUSTER_NAME", "revenue")
        monkeypatch.setattr(
            django_q.conf.settings,
            "Q_CLUSTER",
            {
                "name": "test",
                "timeout": 30,
                "retry": 60,
                "redis": {"host": "127.0.0.1"},
                "ALT_CLUSTERS": {"revenue": {"orm": "default"}},
            },
            raising=False,
        )
        reloaded = importlib.reload(django_q.conf)
        try:
            assert reloaded.Conf.CLUSTER_NAME == "revenue"
            assert reloaded.Conf.ORM == "default"
            assert reloaded.Conf.REDIS == {"host": "127.0.0.1"}
        finally:
            monkeypatch.undo()
            importlib.reload(django_q.conf)
