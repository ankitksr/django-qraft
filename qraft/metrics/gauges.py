"""
Backlog gauges, emitted by the one cluster flagged `metrics_gauges=True`.

Every cluster runs a dispatcher and a reaper; a gauge emitted by all of them
would let a consumer sum the same backlog once per replica. The dispatcher
loop calls `emit_gauges()` once per pass when the flag is set.
"""

import logging

from django.db.models import Min
from django.utils import timezone

from qraft.conf import executing_cluster
from qraft.metrics import gauge, seconds_between

logger = logging.getLogger("qraft.metrics")


def _queue_gauges(cluster: str, now) -> None:
    """
    Depth and oldest age of the ORM queue this cluster drains, all lanes.

    Both readings cover the messages a worker could take right now, which is
    the same set django_q's own `ORM.queue_size()` counts. The broker leases a
    message by pushing its `lock` into the future: counting those as depth
    would repeat the in-flight work the active gauge already reports, and
    their future lock would drag the oldest reading below the true age of what
    is waiting.
    """
    from django_q.brokers.orm import ORM
    from django_q.models import OrmQ

    from qraft.brokers import delivering_broker

    broker = delivering_broker()
    if not isinstance(broker, ORM):
        return
    base = broker.list_key
    ready = OrmQ.objects.filter(
        key__in=(base, f"{base}--high", f"{base}--low"), lock__lte=now
    )
    gauge("qraft.queue.depth", ready.count(), cluster=cluster)
    # ORM.enqueue() stamps lock=now, so the smallest lock is the oldest message.
    oldest = ready.aggregate(Min("lock"))["lock__min"]
    gauge(
        "qraft.queue.oldest_ready_age",
        seconds_between(oldest, now) or 0.0,
        cluster=cluster,
    )


def _run_gauges(now) -> None:
    """Oldest open run per subject type; unbounded run ids never become labels."""
    from qraft.models.runs import QraftRun, RunStatus

    oldest = (
        QraftRun.objects.filter(status=RunStatus.OPEN)
        .values("subject_type")
        .annotate(oldest=Min("date_started"))
    )
    for row in oldest:
        gauge(
            "qraft.run.open_age_max",
            seconds_between(row["oldest"], now) or 0.0,
            subject_type=row["subject_type"],
        )


def emit_gauges() -> None:
    from qraft.models import QraftTaskAttempt
    from qraft.models.tasks import AttemptState

    now = timezone.now()
    cluster = executing_cluster()

    try:
        _queue_gauges(cluster, now)
    except Exception:
        logger.exception("Queue gauges skipped; broker not readable")

    gauge(
        "qraft.attempt.active",
        QraftTaskAttempt.objects.filter(
            success__isnull=True, date_started__isnull=False, cluster=cluster
        ).count(),
        cluster=cluster,
    )
    gauge(
        "qraft.scheduler.overdue",
        QraftTaskAttempt.objects.filter(
            state=AttemptState.SCHEDULED, not_before__lt=now
        ).count(),
    )
    oldest_unrouted = QraftTaskAttempt.objects.filter(
        success__isnull=False, routed=False
    ).aggregate(Min("date_completed"))["date_completed__min"]
    gauge(
        "qraft.attempt.unrouted_age_max", seconds_between(oldest_unrouted, now) or 0.0
    )
    _run_gauges(now)
