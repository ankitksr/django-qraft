"""
Bounded metric queries for the dashboard.

Everything is computed on request straight from qraft's own tables - there is
no collector process and no extra storage. Each sample query is capped so a
large backlog costs a bounded amount of work per poll; beyond the cap the
percentiles stop moving, which is acceptable for a monitoring page.
"""

import math
from datetime import timedelta

from django.db.models import Count, DurationField, ExpressionWrapper, F
from django.utils import timezone

from qraft.models import QraftTask, QraftTaskAttempt, TaskStatus

WINDOWS = {"15m": 900, "1h": 3600, "24h": 86400}
DEFAULT_WINDOW = "1h"

SAMPLE_CAP = 5000
FUNCTION_CAP = 20
EXCEPTION_CAP = 8
SPARKLINE_BARS = 30

SETTLED = (TaskStatus.SUCCEEDED, TaskStatus.FAILED, TaskStatus.EXHAUSTED)


def resolve_window(name: str | None) -> str:
    return name if name in WINDOWS else DEFAULT_WINDOW


def _percentile(values: list[float], q: float) -> float | None:
    """Nearest-rank percentile over a pre-sorted list."""
    if not values:
        return None
    index = max(0, math.ceil(q * len(values)) - 1)
    return round(values[index], 3)


def _stats(deltas) -> dict:
    values = sorted(delta.total_seconds() for delta in deltas if delta is not None)
    return {
        "n": len(values),
        "p50": _percentile(values, 0.5),
        "p95": _percentile(values, 0.95),
    }


def pickup_latency(since) -> dict:
    """Enqueue-to-start delay for first attempts (retries would skew it)."""
    lags = (
        QraftTaskAttempt.objects.filter(attempt_number=1, date_started__gte=since)
        .annotate(
            lag=ExpressionWrapper(
                F("date_started") - F("qraft_task__date_created"),
                output_field=DurationField(),
            )
        )
        .values_list("lag", flat=True)[:SAMPLE_CAP]
    )
    return _stats(lags)


def dispatch_lag(since) -> dict:
    """How late delayed attempts started relative to their due time."""
    lags = (
        QraftTaskAttempt.objects.filter(
            not_before__isnull=False, date_started__gte=since
        )
        .annotate(
            lag=ExpressionWrapper(
                F("date_started") - F("not_before"),
                output_field=DurationField(),
            )
        )
        .values_list("lag", flat=True)[:SAMPLE_CAP]
    )
    return _stats(lags)


def retry_rate(since) -> dict:
    """Share of recently settled tasks that needed more than one attempt."""
    settled_qs = QraftTask.objects.filter(status__in=SETTLED, date_updated__gte=since)
    settled = settled_qs.count()
    retried = (
        settled_qs.annotate(n_attempts=Count("attempts"))
        .filter(n_attempts__gt=1)
        .count()
    )
    return {
        "settled": settled,
        "retried": retried,
        "rate": round(retried / settled, 3) if settled else None,
    }


def exception_leaderboard(since) -> list[dict]:
    rows = (
        QraftTaskAttempt.objects.filter(
            success=False,
            date_completed__gte=since,
            exception_class__isnull=False,
        )
        .values("exception_class")
        .annotate(n=Count("id"))
        .order_by("-n")[:EXCEPTION_CAP]
    )
    return list(rows)


def throughput(since, window_seconds: int) -> dict:
    """Completions bucketed for a CSS sparkline, oldest bucket first."""
    completed = QraftTaskAttempt.objects.filter(date_completed__gte=since).values_list(
        "date_completed", flat=True
    )[:SAMPLE_CAP]

    bucket_seconds = window_seconds // SPARKLINE_BARS
    buckets = [0] * SPARKLINE_BARS
    for done_at in completed:
        index = int((done_at - since).total_seconds() // bucket_seconds)
        buckets[min(max(index, 0), SPARKLINE_BARS - 1)] += 1

    return {
        "bucket_seconds": bucket_seconds,
        "buckets": buckets,
        "per_minute": round(sum(buckets) * 60 / window_seconds, 2),
    }


def per_function(since) -> list[dict]:
    """Count, success rate, and p95 run time, grouped by task function."""
    rows = QraftTaskAttempt.objects.filter(
        date_completed__gte=since, date_started__isnull=False
    ).values_list("qraft_task__func", "success", "date_started", "date_completed")[
        :SAMPLE_CAP
    ]

    grouped: dict[str, dict] = {}
    for func, success, started, completed in rows:
        entry = grouped.setdefault(func, {"count": 0, "successes": 0, "durations": []})
        entry["count"] += 1
        if success:
            entry["successes"] += 1
        entry["durations"].append((completed - started).total_seconds())

    result = []
    for func, entry in grouped.items():
        durations = sorted(entry["durations"])
        result.append(
            {
                "func": func,
                "count": entry["count"],
                "success_pct": round(100 * entry["successes"] / entry["count"], 1),
                "p95": _percentile(durations, 0.95),
            }
        )
    result.sort(key=lambda item: -item["count"])
    return result[:FUNCTION_CAP]


def snapshot(window_name: str | None) -> dict:
    window = resolve_window(window_name)
    window_seconds = WINDOWS[window]
    since = timezone.now() - timedelta(seconds=window_seconds)
    return {
        "window": window,
        "since": since.isoformat(),
        "pickup": pickup_latency(since),
        "dispatch_lag": dispatch_lag(since),
        "retry": retry_rate(since),
        "exceptions": exception_leaderboard(since),
        "throughput": throughput(since, window_seconds),
        "functions": per_function(since),
    }
