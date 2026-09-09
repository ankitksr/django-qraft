"""
W3C trace propagation across the enqueue boundary, when `opentelemetry-api` is
importable. Without it every function here is a no-op and the attempt's
`trace_context` column stays null.
"""

import threading

try:
    from opentelemetry import context as otel_context
    from opentelemetry import trace
    from opentelemetry.trace.propagation.tracecontext import (
        TraceContextTextMapPropagator,
    )
except ImportError:  # pragma: no cover - exercised only without the extra
    trace = None

TRACER_NAME = "qraft"

# One attached context per worker thread. Detaching the previous task's token
# before attaching the next keeps a pool thread (or the single-task standard
# worker) from carrying a finished attempt's span into the next one.
_thread_state = threading.local()


def available() -> bool:
    return trace is not None


def current_traceparent() -> str | None:
    """The current span's `traceparent`, or None when no span is recording."""
    if trace is None:
        return None
    if not trace.get_current_span().get_span_context().is_valid:
        return None
    carrier: dict[str, str] = {}
    TraceContextTextMapPropagator().inject(carrier)
    return carrier.get("traceparent")


def detach_current() -> None:
    """
    Detach this thread's attached span context, if there is one.

    `end_span()` runs on the heartbeat thread, which cannot reach the worker
    thread's own thread-local token. Without this the worker thread stays
    inside a finished attempt's span, and the next attempt - an untraced one
    included - enqueues its own work as that span's child.
    """
    if trace is None:
        return
    token = getattr(_thread_state, "token", None)
    if token is None:
        return
    _thread_state.token = None
    otel_context.detach(token)


def start_attempt_span(traceparent: str | None, name: str):
    """
    Start a child span of `traceparent` and make it current on this thread.

    Returns the span, which the caller ends when the attempt's lease closes.
    """
    if trace is None:
        return None
    # Before the early return too: an attempt that carries no traceparent must
    # still not run inside the previous one's span.
    detach_current()
    if not traceparent:
        return None
    parent = TraceContextTextMapPropagator().extract({"traceparent": traceparent})
    span = trace.get_tracer(TRACER_NAME).start_span(name, context=parent)
    _thread_state.token = otel_context.attach(trace.set_span_in_context(span))
    return span


def end_span(span) -> None:
    if span is not None:
        span.end()
