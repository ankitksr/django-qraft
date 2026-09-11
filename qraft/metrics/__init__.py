"""
Metrics sink: counters, histograms and gauges emitted at Qraft's own transition
points, labelled without ids.

`QRAFT_CLUSTER["metrics_sink"]` names a class implementing `Sink`; the default
`NullSink` discards everything. `qraft.metrics.otel.OpenTelemetrySink` records
through `opentelemetry-api` and leaves the SDK, exporter and process lifecycle
to the host application.

A sink that raises is caught here: the exception is logged at most once per
minute per process and counted in `health()`, which the dashboard exposes. The
sink is never disabled, so a transient exporter failure does not silence the
process until restart.

Labels are bounded by design. Function, cluster, outcome, exception class,
subject type and run kind are labels; subject id, run id, task id, revision and
metadata never are. `_check_labels` enforces the name-level rule.
"""

import logging
import threading
import time
from typing import Protocol

from django.db import transaction
from django.utils.module_loading import import_string

_logger = logging.getLogger("qraft.metrics")

# Label names that would carry per-entity cardinality into the metrics system.
FORBIDDEN_LABELS = frozenset(
    {
        "subject_id",
        "graph_id",
        "graph",
        "task_id",
        "attempt_id",
        "revision",
        "metadata",
        "generation",
    }
)

LOG_INTERVAL = 60.0


class Sink(Protocol):
    def counter(self, name: str, value: float = 1, **labels) -> None: ...

    def histogram(self, name: str, value: float, **labels) -> None: ...

    def gauge(self, name: str, value: float, **labels) -> None: ...


class NullSink:
    """Discards every emission; the default."""

    def counter(self, name: str, value: float = 1, **labels) -> None:
        pass

    def histogram(self, name: str, value: float, **labels) -> None:
        pass

    def gauge(self, name: str, value: float, **labels) -> None:
        pass


class _Health:
    """In-process failure counter with a once-a-minute log throttle."""

    def __init__(self):
        self._lock = threading.Lock()
        self.failures = 0
        self.last_error: str | None = None
        self._last_logged: float | None = None

    def record(self, exc: Exception) -> None:
        with self._lock:
            self.failures += 1
            self.last_error = f"{type(exc).__name__}: {exc}"
            now = time.monotonic()
            should_log = (
                self._last_logged is None or now - self._last_logged >= LOG_INTERVAL
            )
            if should_log:
                self._last_logged = now
        if should_log:
            _logger.warning("Metrics sink failed (%d so far): %s", self.failures, exc)

    def snapshot(self) -> dict:
        with self._lock:
            return {"failures": self.failures, "last_error": self.last_error}

    def reset(self) -> None:
        with self._lock:
            self.failures = 0
            self.last_error = None
            self._last_logged = None


_health = _Health()
_sink: Sink | None = None
_sink_lock = threading.Lock()


def get_sink() -> Sink:
    """The configured sink, instantiated once per process."""
    global _sink
    if _sink is None:
        from qraft.conf import get_conf

        with _sink_lock:
            if _sink is None:
                _sink = import_string(get_conf().metrics_sink)()
    return _sink


def reset_sink() -> None:
    """Drop the cached sink so the next call re-reads settings (tests)."""
    global _sink
    _sink = None
    _health.reset()


def health() -> dict:
    return _health.snapshot()


def _check_labels(labels: dict) -> dict:
    leaked = FORBIDDEN_LABELS.intersection(labels)
    if leaked:
        raise ValueError(f"metric labels must never carry {sorted(leaked)}")
    return {key: ("" if value is None else str(value)) for key, value in labels.items()}


def _emit(method: str, name: str, value, labels: dict) -> None:
    try:
        getattr(get_sink(), method)(name, value, **_check_labels(labels))
    except Exception as exc:
        _health.record(exc)


def counter(name: str, value: float = 1, **labels) -> None:
    _emit("counter", name, value, labels)


def histogram(name: str, value: float, **labels) -> None:
    _emit("histogram", name, value, labels)


def gauge(name: str, value: float, **labels) -> None:
    _emit("gauge", name, value, labels)


def counter_on_commit(name: str, value: float = 1, **labels) -> None:
    """
    Count a transition once the transaction performing it commits.

    The same rule `qraft.signals.send` follows, for the same reason: a metric
    emitted from inside `transaction.atomic()` survives a rollback that undoes
    the row change it reports, leaving a count with nothing behind it. Outside
    a transaction the callback runs immediately.
    """
    transaction.on_commit(lambda: counter(name, value, **labels))


def histogram_on_commit(name: str, value: float, **labels) -> None:
    """Record a measurement once the transaction performing it commits."""
    transaction.on_commit(lambda: histogram(name, value, **labels))


def seconds_between(start, end) -> float | None:
    """Duration in seconds, or None when either side is missing."""
    if start is None or end is None:
        return None
    return max(0.0, (end - start).total_seconds())
