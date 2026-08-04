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

To consume priority lanes, point a cluster's `Q_CLUSTER["broker_class"]` at
`"qraft.brokers.QraftOrmBroker"` (django_q's `get_broker()` reads this
setting and instantiates the class with the cluster's `list_key`). Without
that setting, high/low tasks are still enqueued into their lanes but never
drained by a stock django_q ORM broker.

v1 limitation: scheduled retries go through `schedule_retry()`'s default
path and are not re-routed by priority.
"""

from time import sleep

from django.utils import timezone
from django_q.brokers.orm import ORM
from django_q.conf import Conf


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


def priority_list_key(priority: str) -> str | None:
    """
    Return the suffixed list_key for a priority, or None for "default".

    None signals "don't override the broker" to the caller.
    """
    if priority == "high":
        return f"{Conf.CLUSTER_NAME}--high"
    if priority == "low":
        return f"{Conf.CLUSTER_NAME}--low"
    return None
