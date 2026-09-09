"""
OpenTelemetry sink: instruments on a meter named `qraft`, through the API only.

The host configures the SDK, exporter and resources; without an SDK the API
hands back a no-op meter and every record is dropped silently, which is the
boundary OpenTelemetry draws between a library and an application. Installed by
the `django-qraft[otel]` extra.
"""

import threading

from opentelemetry import metrics as otel_metrics

METER_NAME = "qraft"


class OpenTelemetrySink:
    def __init__(self, meter=None):
        self._meter = meter or otel_metrics.get_meter(METER_NAME)
        self._lock = threading.Lock()
        self._counters = {}
        self._histograms = {}
        self._gauges = {}
        # Observable gauges read from here; a plain set-and-forget gauge is not
        # part of the API's synchronous instrument set on older releases.
        self._gauge_values: dict[str, dict[tuple, float]] = {}

    def counter(self, name: str, value: float = 1, **labels) -> None:
        with self._lock:
            instrument = self._counters.get(name)
            if instrument is None:
                instrument = self._counters[name] = self._meter.create_counter(name)
        instrument.add(value, attributes=labels)

    def histogram(self, name: str, value: float, **labels) -> None:
        with self._lock:
            instrument = self._histograms.get(name)
            if instrument is None:
                instrument = self._histograms[name] = self._meter.create_histogram(
                    name, unit="s"
                )
        instrument.record(value, attributes=labels)

    def gauge(self, name: str, value: float, **labels) -> None:
        key = tuple(sorted(labels.items()))
        with self._lock:
            self._gauge_values.setdefault(name, {})[key] = value
            if name not in self._gauges:
                self._gauges[name] = self._meter.create_observable_gauge(
                    name, callbacks=[self._observe(name)]
                )

    def _observe(self, name: str):
        def callback(options):
            from opentelemetry.metrics import Observation

            with self._lock:
                items = list(self._gauge_values.get(name, {}).items())
            return [Observation(value, dict(key)) for key, value in items]

        return callback
