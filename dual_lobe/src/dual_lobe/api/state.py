"""GET /v1/dual-lobe/state/{run_id} — derived current B state for a run."""
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Request

from ..core.engine import tenant_session
from ..state import repositories as repo
from . import auth
from .schemas import StateResponse

router = APIRouter()


@router.get("/v1/dual-lobe/state/{run_id}")
async def run_state(
    run_id: str,
    request: Request,
    principal: auth.Principal = Depends(auth.require_scope(auth.SCOPE_STATE_READ)),
):
    async with tenant_session(principal.tenant_id) as session:
        run = await repo._get_run_or_none(session, principal.tenant_id, run_id)
        if run is None:
            raise HTTPException(status_code=404, detail=f"run {run_id} not found in tenant")
        internal = str(run.id)
        latest = await repo.latest_b_state(session, internal)
        claims = [repo.claim_to_dict(c) for c in await repo.list_claims(session, run_id=internal, limit=50)]
        evidence = [repo.evidence_to_dict(e) for e in await repo.list_evidence(session, run_id=internal, limit=50)]
        events = [repo.event_to_dict(e) for e in await repo.list_events(session, run_id=internal, limit=25)]
    return StateResponse(
        run_id=internal,
        revision=latest["revision"] if latest else None,
        pulse=latest["pulse"] if latest else None,
        payload=latest["payload"] if latest else {},
        claims=claims,
        evidence=evidence,
        recent_events=events,
    )