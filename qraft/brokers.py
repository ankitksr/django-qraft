"""
Per-cluster broker resolution, and priority lanes for Django-Q2's ORM broker.

Two related jobs live here. `broker_for_cluster()` builds the broker a *named*
cluster runs, so a project may run `revenue` on the ORM broker while `default`
and `reports` stay on Redis and every enqueue still lands on the target
cluster's queue (see the "per-cluster broker resolution" section below).
`RoutingBroker` extends that to enqueues Qraft does not make itself. The rest
of this module is the priority-lane scheme described next.

Priority lanes
--------------

Django-Q2 dequeues from a single `list_key` (default: `Conf.CLUSTER_NAME`).
`QraftOrmBroker` implements priority without any schema changes by treating
`{list_key}--high` / `{list_key}` / `{list_key}--low` as three separate
lanes on the same `OrmQ` table, and polling them in that order on every
`dequeue()` call: the first non-empty lane wins.

Enqueue side (`qraft/tasks.py`) routes `qraft_options={"priority": "high"}`
or `"low"` tasks to the suffixed lane by passing a `QraftOrmBroker` instance
constructed with the suffixed `list_key` as django_q's `broker=` kwarg.
`"default"` (or omitted) priority is unchanged: no broker override, normal
`Conf.CLUSTER_NAME` lane.

The lane key is built from the *target* cluster, not the enqueuing process:
django_q resolves `broker = task.pop("broker") or get_broker(task["cluster"])`,
so an explicit broker short-circuits `cluster=` entirely and the lane has to
carry the routing itself.

To consume priority lanes, point a cluster's `Q_CLUSTER["broker_class"]` at
`"qraft.brokers.QraftOrmBroker"` (django_q's `get_broker()` reads this
setting and instantiates the class with the cluster's `list_key`). Without
that setting nothing drains the suffixed lanes, so `priority_list_key()`
declines to route and the task falls back to the default lane with a
warning - running at the wrong priority beats never running at all.

Draining is strictly high -> default -> low, by design: a lane is only
touched once every lane ahead of it is empty, so sustained high-lane load
can starve low indefinitely. That is accepted for the metered-AI use case
this module targets - weighted fairness across lanes is deliberately out
of scope.

Because the lane is keyed off the target cluster, whether it can even be
drained also has to be checked against the *target* cluster's config, not
the enqueuing process's own `Conf.BROKER_CLASS` - a process whose own
broker is `QraftOrmBroker` can still route into a lane that nothing drains,
if the cluster it names runs a plain broker. `priority_lanes_available()`
resolves this via `qraft.conf._merge_alt_cluster` against
`settings.Q_CLUSTER`, the same way `qraft.conf` resolves `QRAFT_CLUSTER`'s
own `ALT_CLUSTERS`.
"""

import logging
import os
import threading
from functools import lru_cache
from time import sleep

from django.conf import settings as django_settings
from django.core.signals import setting_changed
from django.dispatch import receiver
from django.utils import timezone
from django.utils.module_loading import import_string
from django_q.brokers import Broker
from django_q.brokers.orm import ORM
from django_q.conf import Conf

from qraft.conf import _merge_alt_cluster

_logger = logging.getLogger("qraft")


class QraftOrmBroker(ORM):
    """ORM broker that drains high, then default, then low priority lanes."""

    def dequeue(self):
        base = self.list_key
        for key in (f"{base}--high", base, f"{base}--low"):
            tasks = self.get_connection().filter(
                key=key, lock__lt=timezone.now()
            )[
                0 : Conf.BULK  # noqa: E203
            ]
            if not tasks:
                continue
            task_list = []
            for task in tasks:
                if (
                    self.get_connection()
                    .filter(id=task.id, lock=task.lock)
                    .update(lock=self.timeout(task))
                ):
                    task_list.append((task.pk, task.payload))
            if task_list:
                return task_list
        # All lanes empty: spare the CPU, same as the base ORM broker.
        sleep(Conf.POLL)


@lru_cache(maxsize=8)
def _lanes_drained_by(broker_class: str | None) -> bool:
    """
    Whether `broker_class` drains the suffixed lanes.

    Cached on the setting value so the warning below is emitted once per
    process rather than once per enqueue.
    """
    if not broker_class:
        resolved = None
    else:
        try:
            resolved = import_string(broker_class)
        except ImportError:
            resolved = None

    if resolved is not None and issubclass(resolved, QraftOrmBroker):
        return True

    _logger.warning(
        "Q_CLUSTER['broker_class'] is %r, which does not drain Qraft priority "
        "lanes; tasks asking for high/low priority will run in the default "
        "lane. Set broker_class='qraft.brokers.QraftOrmBroker' to enable them.",
        broker_class,
    )
    return False


def _broker_class_for_cluster(cluster: str | None) -> str | None:
    """
    Resolve `broker_class` for the cluster a task is routed to.

    `cluster=None` means "the enqueuing process's own cluster" -
    `Conf.BROKER_CLASS` already reflects that correctly, since django_q
    resolves its own `ALT_CLUSTERS` against this process's `Q_CLUSTER_NAME`
    at import time. A *named* target cluster is somebody else's process
    though, so its `broker_class` has to be resolved the same way
    `qraft.conf` resolves `QRAFT_CLUSTER`'s own `ALT_CLUSTERS`: merge that
    cluster's entry onto the base `Q_CLUSTER` dict rather than trusting this
    process's already-resolved `Conf`.
    """
    if cluster is None:
        return Conf.BROKER_CLASS

    q_cluster = getattr(django_settings, "Q_CLUSTER", {})
    if not isinstance(q_cluster, dict):
        return None
    return _merge_alt_cluster(q_cluster, cluster).get("broker_class")


def priority_lanes_available(cluster: str | None = None) -> bool:
    """
    Whether the target cluster's configured broker class drains the
    suffixed priority lanes.

    Args:
        cluster: Target cluster name. None checks the enqueuing process's
            own cluster.
    """
    return _lanes_drained_by(_broker_class_for_cluster(cluster))


def priority_list_key(priority: str, cluster: str | None = None) -> str | None:
    """
    Return the suffixed list_key for a priority, or None for "default".

    None signals "don't override the broker" to the caller - returned both
    for default priority and when the target cluster's broker doesn't drain
    the lanes.

    Args:
        priority: "high", "low", or "default".
        cluster: Target cluster name. Defaults to the enqueuing process's
            own cluster, which is only correct when the two coincide.
    """
    if priority not in ("high", "low"):
        return None
    if not priority_lanes_available(cluster):
        return None
    return f"{cluster or Conf.CLUSTER_NAME}--{priority}"


# --- per-cluster broker resolution -----------------------------------------
#
# django_q's `get_broker()` reads the *enqueuing* process's `Conf`, so a web
# process configured for Redis hands every enqueue to Redis even when the
# named target cluster runs the ORM broker. `broker_for_cluster()` resolves
# the broker from the target cluster's own merged `Q_CLUSTER` entry instead,
# and every Qraft enqueue passes the result as django_q's `broker=` kwarg -
# which short-circuits `get_broker(cluster)` entirely.
#
# Connection settings live on `django_q.conf.Conf` as module-level globals
# that the broker classes read from inside `get_connection()`, so the only
# faithful way to build "the broker some other cluster would build" is to
# apply that cluster's values to `Conf` for the duration of the construction.
# That is done under a lock and cached, so it happens once per cluster per
# process; the window in which another thread could read a patched `Conf` is
# that one construction, and django_q mutates the same globals itself
# (`aws_sqs` clamps `Conf.BULK`, `mongo` fills in `Conf.MONGO_DB`). Every
# broker but the ORM one captures its connection in `__init__`; the ORM broker
# re-reads `Conf.ORM` (a database alias) on every call, so its alias is pinned
# onto a subclass instead.

# Conf attribute -> Q_CLUSTER key, with the default django_q itself applies.
_CONF_BROKER_KEYS = {
    "BROKER_CLASS": ("broker_class", None),
    "ORM": ("orm", None),
    "REDIS": ("redis", {}),
    "DJANGO_REDIS": ("django_redis", None),
    "SQS": ("sqs", None),
    "MONGO": ("mongo", None),
    "MONGO_DB": ("mongo_db", None),
    "IRON_MQ": ("iron_mq", None),
}

_build_lock = threading.Lock()

# (pid, cluster, list_key) -> broker. The pid is part of the key because a
# forked worker inherits this dict along with connection objects that are not
# fork-safe; a child rebuilds rather than reusing its parent's socket.
_broker_cache: dict[tuple[int, str, str], "Broker"] = {}

# (base class, database alias) -> subclass pinned to that alias.
_pinned_orm_classes: dict[tuple[type, str | None], type] = {}


def reset_broker_cache() -> None:
    """Forget every resolved broker. Called on a settings change and in tests."""
    with _build_lock:
        _broker_cache.clear()
    _lanes_drained_by.cache_clear()


@receiver(setting_changed)
def _reset_on_settings_change(sender, setting, **kwargs):
    if setting in ("Q_CLUSTER", "QRAFT_CLUSTER"):
        reset_broker_cache()


def cluster_broker_config(cluster: str | None = None) -> dict:
    """
    The `Q_CLUSTER` dict a named cluster runs under.

    The base settings with that cluster's `ALT_CLUSTERS` entry overlaid, the
    same overlay `qraft.conf` applies to `QRAFT_CLUSTER`. A cluster that
    declares no entry - including an unknown name - gets the base config,
    which is what django_q would also fall back to.
    """
    q_cluster = getattr(django_settings, "Q_CLUSTER", {})
    if not isinstance(q_cluster, dict):
        return {}
    return _merge_alt_cluster(q_cluster, cluster)


def _orm_get_connection(self, list_key: str = None):
    """`ORM.get_connection` with the database alias pinned to the instance."""
    from django import db
    from django.db import transaction
    from django_q.models import OrmQ

    alias = self.qraft_orm_alias
    if transaction.get_autocommit(using=alias):
        db.close_old_connections()
    return OrmQ.objects.using(alias)


def _orm_class_pinned_to(base: type, alias: str | None) -> type:
    """
    An ORM broker subclass whose queue always lives in `alias`.

    `ORM.get_connection` reads `Conf.ORM` on every call, so an instance built
    for another cluster would still enqueue into the *enqueuing* process's
    database alias. The generated class is registered in this module so it
    pickles by name, which is what the cluster's spawned worker processes need.
    """
    key = (base, alias)
    pinned = _pinned_orm_classes.get(key)
    if pinned is not None:
        return pinned

    stem = f"{base.__name__}On{(alias or 'default').replace('-', '_').title()}"
    # Two different base classes could produce the same stem; pickle resolves
    # by name, so the name has to be unique or an unpickled broker would come
    # back as the wrong class.
    name = stem
    suffix = 2
    while name in globals():
        name = f"{stem}{suffix}"
        suffix += 1

    pinned = type(
        name,
        (base,),
        {
            "qraft_orm_alias": alias,
            "get_connection": _orm_get_connection,
            "__doc__": f"{base.__name__} pinned to the '{alias}' database alias.",
        },
    )
    pinned.__module__ = __name__
    pinned.__qualname__ = name
    globals()[name] = pinned
    _pinned_orm_classes[key] = pinned
    return pinned


def _broker_class_for_config(config: dict) -> type:
    """
    Which broker class a cluster with this config runs.

    Mirrors `django_q.brokers.get_broker`'s own order exactly. A
    `broker_class` naming `RoutingBroker` is ignored rather than obeyed: it is
    the routing entry point, and honouring it here would recurse forever.
    """
    path = config.get("broker_class")
    if path:
        try:
            resolved = import_string(path)
        except ImportError:
            resolved = None
        if resolved is not None and not issubclass(resolved, RoutingBroker):
            return resolved

    if config.get("iron_mq"):
        from django_q.brokers.ironmq import IronMQBroker

        return IronMQBroker
    if isinstance(config.get("sqs"), dict):
        from django_q.brokers.aws_sqs import Sqs

        return Sqs
    if config.get("orm"):
        from django_q.brokers.orm import ORM

        return ORM
    if config.get("mongo"):
        from django_q.brokers.mongo import Mongo

        return Mongo
    from django_q.brokers.redis_broker import Redis

    return Redis


def _build_broker(config: dict, list_key: str):
    """Instantiate the cluster's broker class under that cluster's Conf values."""
    from django_q.brokers.orm import ORM

    broker_class = _broker_class_for_config(config)
    if issubclass(broker_class, ORM):
        broker_class = _orm_class_pinned_to(broker_class, config.get("orm"))

    saved = {}
    for attribute, (key, default) in _CONF_BROKER_KEYS.items():
        saved[attribute] = getattr(Conf, attribute, default)
        setattr(Conf, attribute, config.get(key, default))
    try:
        return broker_class(list_key=list_key)
    finally:
        for attribute, value in saved.items():
            setattr(Conf, attribute, value)


def broker_for_cluster(cluster: str | None = None, priority: str | None = None):
    """
    The broker instance that enqueues onto `cluster`'s queue.

    Args:
        cluster: Target cluster name. None means the enqueuing process's own
            cluster, which is what `Conf.CLUSTER_NAME` already resolves to.
        priority: "high" or "low" to enqueue into that priority lane, when the
            target cluster's broker class drains the lanes (see
            `priority_list_key`). Anything else uses the default lane.

    Returns:
        A `django_q.brokers.Broker`, cached per (process, cluster, lane).
    """
    # `priority_list_key` is given the caller's own argument, not the
    # resolved name: `cluster=None` means "my own cluster", whose lane support
    # `Conf.BROKER_CLASS` already answers directly.
    lane = priority_list_key(priority, cluster) if priority else None
    cluster = cluster or Conf.CLUSTER_NAME
    list_key = lane or cluster

    key = (os.getpid(), cluster, list_key)
    broker = _broker_cache.get(key)
    if broker is not None:
        return broker

    with _build_lock:
        # Re-check: another thread may have built it while this one waited,
        # and one broker per lane per process is the point of the cache.
        broker = _broker_cache.get(key)
        if broker is None:
            broker = _build_broker(cluster_broker_config(cluster), list_key)
            _broker_cache[key] = broker
    return broker


def delivering_broker(broker=None):
    """
    The broker that actually moves a cluster's messages.

    `RoutingBroker` forwards every call to the broker its cluster runs, so any
    question about broker *capability* - delivery receipts, whether the queue
    is readable as `OrmQ` rows - has to be asked of that delegate. Asking the
    router itself answers "yes" to both, because a forwarding method is still
    an override of the base no-op.

    Args:
        broker: The broker to unwrap; None resolves this process's own.
    """
    if broker is None:
        from django_q.brokers import get_broker

        broker = get_broker()
    if isinstance(broker, RoutingBroker):
        return broker.target
    return broker


class RoutingBroker(Broker):
    """
    Base `broker_class` that sends every operation to its cluster's own broker.

    Qraft's own enqueues name their broker explicitly, so this exists for the
    code that does not: `django_q.tasks.async_task(..., cluster="revenue")`
    from an application, a third-party library, or django_q's own internals.
    django_q resolves that to `get_broker("revenue")`, which builds
    `RoutingBroker(list_key="revenue")` - and every call then lands on
    `broker_for_cluster("revenue")`.

    Declare it once on the base entry::

        Q_CLUSTER = {
            "broker_class": "qraft.brokers.RoutingBroker",
            "redis": {"host": "127.0.0.1"},
            "ALT_CLUSTERS": {"revenue": {"orm": "default"}},
        }

    A cluster process is unaffected: `qraftcluster` re-execs with
    `Q_CLUSTER_NAME` set, so django_q merges the ALT entry into `Conf` before
    the broker is built, and the delegate this returns is the same broker the
    cluster would have built for itself.
    """

    def __init__(self, list_key: str = None):
        # Deliberately not Broker.__init__: that would open a connection
        # against the *enqueuing* process's Conf, which is the misrouting this
        # class exists to prevent. The delegate is resolved per call instead.
        self.list_key = list_key or Conf.CLUSTER_NAME
        self._info = None
        self.cache = self.get_cache()

    @property
    def target(self):
        """The concrete broker for this list key's cluster."""
        return broker_for_cluster(self.list_key)

    @property
    def connection(self):
        return self.target.connection

    def enqueue(self, task):
        return self.target.enqueue(task)

    def dequeue(self):
        return self.target.dequeue()

    def queue_size(self):
        return self.target.queue_size()

    def lock_size(self):
        return self.target.lock_size()

    def delete_queue(self):
        return self.target.delete_queue()

    def purge_queue(self):
        return self.target.purge_queue()

    def delete(self, task_id):
        return self.target.delete(task_id)

    def acknowledge(self, task_id):
        return self.target.acknowledge(task_id)

    def fail(self, task_id):
        return self.target.fail(task_id)

    def ping(self) -> bool:
        return self.target.ping()

    def info(self):
        return f"Routing -> {self.target.info()}"

    def set_stat(self, key: str, value: str, timeout: int):
        return self.target.set_stat(key, value, timeout)

    def get_stat(self, key: str):
        return self.target.get_stat(key)

    def get_stats(self, pattern: str):
        return self.target.get_stats(pattern)

    def __getstate__(self):
        return self.list_key, self._info

    def __setstate__(self, state):
        self.list_key, self._info = state
        self.cache = self.get_cache()
