"""
Priority lanes for Django-Q2's ORM broker.

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

v1 limitation: scheduled retries go through `schedule_retry()`'s default
path and are not re-routed by priority.
"""

import logging
from functools import lru_cache
from time import sleep

from django.utils import timezone
from django.utils.module_loading import import_string
from django_q.brokers.orm import ORM
from django_q.conf import Conf

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


def priority_lanes_available() -> bool:
    """Whether the configured broker class drains the suffixed priority lanes."""
    return _lanes_drained_by(Conf.BROKER_CLASS)


def priority_list_key(priority: str, cluster: str | None = None) -> str | None:
    """
    Return the suffixed list_key for a priority, or None for "default".

    None signals "don't override the broker" to the caller - returned both
    for default priority and when no configured broker drains the lanes.

    Args:
        priority: "high", "low", or "default".
        cluster: Target cluster name. Defaults to the enqueuing process's
            own cluster, which is only correct when the two coincide.
    """
    if priority not in ("high", "low"):
        return None
    if not priority_lanes_available():
        return None
    return f"{cluster or Conf.CLUSTER_NAME}--{priority}"
