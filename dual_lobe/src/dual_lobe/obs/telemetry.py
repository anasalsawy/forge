"""Structured logging and OpenTelemetry setup."""
from __future__ import annotations

import logging
import sys

import structlog


def configure_logging(level: str = "INFO", json: bool = False) -> None:
    logging.basicConfig(
        format="%(asctime)s %(levelname)s %(message)s",
        level=getattr(logging, level.upper(), logging.INFO),
        stream=sys.stdout,
        force=True,
    )
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.StackInfoRenderer(),
            structlog.dev.set_exc_info,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.format_exc_info,
            (
                structlog.processors.JSONRenderer()
                if json
                else structlog.dev.ConsoleRenderer()
            ),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(
            getattr(logging, level.upper(), logging.INFO)
        ),
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )


class Tracer:
    enabled = False

    def configure(self, enabled: bool) -> None:
        self.enabled = enabled
        if not enabled:
            return
        from opentelemetry import trace
        from opentelemetry.sdk.resources import Resource
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter

        provider = TracerProvider(resource=Resource.create({"service.name": "dual-lobe"}))
        provider.add_span_processor(BatchSpanProcessor(ConsoleSpanExporter()))
        trace.set_tracer_provider(provider)

    def tracer_name(self) -> str:
        return "dual-lobe"


TRACER = Tracer()