"""FastAPI application entrypoint (``uvicorn dual_lobe.api.app:app``)."""
from __future__ import annotations

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from ..core.engine import dispose_engines
from ..core.settings import get_settings
from ..obs.telemetry import TRACER
from . import chat, events, health, state

LOG = logging.getLogger("dual_lobe.api")


@asynccontextmanager
async def lifespan(app: FastAPI):
    s = get_settings()
    TRACER.configure(s.otel_enabled)
    LOG.info("dual-lobe gateway starting host=%s port=%d stage=%s", s.host, s.port, s.rollout_stage)
    try:
        yield
    finally:
        await dispose_engines()


app = FastAPI(
    title="Dual-Lobe Inference Proxy",
    version="0.1.0",
    lifespan=lifespan,
)

app.include_router(chat.router)
app.include_router(events.router)
app.include_router(state.router)
app.include_router(health.router)


@app.get("/")
async def root():
    return {"service": "dual-lobe", "status": "ok"}