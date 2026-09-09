"""POST /v1/dual-lobe/events — external ledger ingestion (CrewAI tools etc.)."""
from __future__ import annotations

from fastapi import APIRouter, Depends, Request

from ..core.engine import tenant_session
from ..core.redact import redact_payload
from ..state import repositories as repo
from . import auth
from .schemas import EventIngest

router = APIRouter()


@router.post("/v1/dual-lobe/events")
async def ingest_event(
    body: EventIngest,
    request: Request,
    principal: auth.Principal = Depends(auth.require_scope(auth.SCOPE_EVENTS_WRITE)),
):
    run_id = body.run_id
    async with tenant_session(principal.tenant_id) as session:
        if run_id:
            run = await repo._get_run_or_none(session, principal.tenant_id, run_id)
            if run is None:
                return {"status": "error", "error": f"run {run_id} not found in tenant"}
        ev = await repo.append_event(
            session,
            body.kind,
            principal.tenant_id,
            run_id=run_id,
            actor=body.actor or "",
            payload=redact_payload(body.payload),
            idempotency_key=body.idempotency_key,
        )
        await session.commit()
    return {"status": "ok", "event_id": str(ev.id), "run_id": run_id, "kind": body.kind}