"""OpenTelemetry, by hand: Laser SDK's Python package has no OTEL integration yet.

One trace per ticket. The server (or the CLI runner) opens the root span and
puts a W3C traceparent in the handoff body; each agent continues it, so the
trace crosses process boundaries through the log. Spans export over OTLP/HTTP
to OTEL_EXPORTER_OTLP_ENDPOINT (default http://localhost:4318, Jaeger in
compose.yaml). With the packages missing or DESK_OTEL=0 everything is a no-op."""

from __future__ import annotations

import contextlib
import os

_tracer = None
_provider = None
_propagator = None

try:
    if os.environ.get("DESK_OTEL", "1") != "0":
        from opentelemetry import trace
        from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
        from opentelemetry.sdk.resources import Resource
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import SimpleSpanProcessor
        from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
        _OTEL = True
    else:
        _OTEL = False
except ImportError:
    _OTEL = False


def _reachable(endpoint: str) -> bool:
    import socket
    from urllib.parse import urlparse
    url = urlparse(endpoint)
    try:
        with socket.create_connection((url.hostname or "localhost", url.port or 4318), timeout=0.5):
            return True
    except OSError:
        return False


def init(service: str) -> bool:
    """Set up the exporter for this process. Returns whether tracing is on."""
    global _tracer, _provider, _propagator
    if not _OTEL:
        return False
    endpoint = os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4318").rstrip("/")
    if not _reachable(endpoint):
        # No collector listening: export would retry and block on every span.
        print(f"  tracing off: nothing listening on {endpoint} (docker compose up -d starts Jaeger)",
              flush=True)
        return False
    _provider = TracerProvider(resource=Resource.create({"service.name": service}))
    # Simple (synchronous) export: an agent can be SIGKILLed at any moment, and a
    # batch would die with it.
    _provider.add_span_processor(SimpleSpanProcessor(OTLPSpanExporter(endpoint=f"{endpoint}/v1/traces")))
    trace.set_tracer_provider(_provider)
    _tracer = trace.get_tracer("support-desk")
    _propagator = TraceContextTextMapPropagator()
    return True


@contextlib.contextmanager
def span(name: str, parent: dict | None = None, **attributes):
    """A span, continuing `parent` (a carrier dict) when given."""
    if _tracer is None:
        yield None
        return
    context = _propagator.extract(parent) if parent else None
    with _tracer.start_as_current_span(name, context=context) as current:
        for key, value in attributes.items():
            if value is not None:
                current.set_attribute(f"desk.{key}", str(value))
        yield current


def start_root(name: str, **attributes):
    """A root span held open across awaits (the ticket). End it with end_root."""
    if _tracer is None:
        return None
    root = _tracer.start_span(name)
    for key, value in attributes.items():
        root.set_attribute(f"desk.{key}", str(value))
    return root


def carrier(of=None) -> dict:
    """The W3C trace context of the current span, or of `of`, to put on the log."""
    if _tracer is None:
        return {}
    out: dict = {}
    if of is not None:
        from opentelemetry import trace as _trace
        _propagator.inject(out, context=_trace.set_span_in_context(of))
    else:
        _propagator.inject(out)
    return out


def trace_id(root) -> str | None:
    if root is None:
        return None
    return format(root.get_span_context().trace_id, "032x")


def end_root(root, **attributes) -> None:
    if root is None:
        return
    for key, value in attributes.items():
        root.set_attribute(f"desk.{key}", str(value))
    root.end()
