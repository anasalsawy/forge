"""Async repositories over Postgres — the single source of truth.

Tenant-scoped methods take an ``AsyncSession`` bound to a tenant (RLS session);
admin methods (auth lookup, worker bookkeeping) take an admin session.
"""
from __future__ import annotations

import uuid
from typing import Any

from sqlalchemy import delete, func, select, text, update
from sqlalchemy.ext.asyncio import AsyncSession

from ..core import idgen, models
from ..core import stage as stage_mod


async def get_tenant_by_slug(session: AsyncSession, slug: str) -> models.Tenant | None:
    res = await session.execute(select(models.Tenant).where(models.Tenant.slug == slug))
    return res.scalar_one_or_none()


async def get_api_key(session: AsyncSession, key_hash: str) -> models.ApiKey | None:
    res = await session.execute(
        select(models.ApiKey).where(models.ApiKey.key_hash == key_hash, models.ApiKey.revoked_at.is_(None))
    )
    return res.scalar_one_or_none()


async def _get_run_or_none(session: AsyncSession, tenant_id: int, run_id: str) -> models.Run | None:
    """Resolve a run by internal UUID or external_run_id, within a tenant."""
    from sqlalchemy import or_

    try:
        as_uuid = uuid.UUID(run_id)
        res = await session.execute(
            select(models.Run).where(
                models.Run.tenant_id == tenant_id,
                or_(models.Run.id == as_uuid, models.Run.external_run_id == run_id),
            )
        )
    except (ValueError, TypeError):
        res = await session.execute(
            select(models.Run).where(
                models.Run.tenant_id == tenant_id, models.Run.external_run_id == run_id
            )
        )
    return res.scalar_one_or_none()


async def get_or_create_run(
    session: AsyncSession,
    tenant_id: int,
    external_run_id: str,
    floor_id: str = "",
    attempt: int = 1,
    goal: str = "",
) -> models.Run:
    res = await session.execute(
        select(models.Run).where(
            models.Run.tenant_id == tenant_id, models.Run.external_run_id == external_run_id
        )
    )
    run = res.scalar_one_or_none()
    if run is not None:
        if floor_id or goal:
            run.current_floor = floor_id or run.current_floor
            run.current_attempt = attempt or run.current_attempt
            run.goal = goal or run.goal
            await session.flush()
        return run
    run = models.Run(
        tenant_id=tenant_id,
        external_run_id=external_run_id or str(uuid.uuid4()),
        current_floor=floor_id,
        current_attempt=attempt,
        goal=goal,
    )
    session.add(run)
    await session.flush()
    return run


async def append_event(
    session: AsyncSession,
    kind: str,
    tenant_id: int,
    run_id: str | None = None,
    actor: str = "",
    payload: dict[str, Any] | None = None,
    idempotency_key: str | None = None,
) -> models.Event:
    ev = models.Event(
        tenant_id=tenant_id,
        run_id=uuid.UUID(run_id) if run_id else None,
        kind=kind,
        actor=actor,
        payload=payload or {},
        idempotency_key=idempotency_key,
    )
    session.add(ev)
    await session.flush()
    return ev


async def list_events(
    session: AsyncSession, run_id: str | None = None, tenant_id: int | None = None, limit: int = 200
) -> list[models.Event]:
    q = select(models.Event).order_by(models.Event.ts.desc(), models.Event.id.desc()).limit(limit)
    if tenant_id is not None:
        q = q.where(models.Event.tenant_id == tenant_id)
    if run_id:
        q = q.where(models.Event.run_id == uuid.UUID(run_id))
    res = await session.execute(q)
    return list(res.scalars().all())


async def append_evidence(
    session: AsyncSession,
    tenant_id: int,
    kind: str,
    validation_status: str = "PENDING",
    *,
    run_id: str | None = None,
    claim_id: str | None = None,
    source: str = "",
    artifact_ref: str = "",
    artifact_hash: str = "",
    tool_name: str = "",
    idempotency_key: str | None = None,
) -> models.Evidence:
    ev = models.Evidence(
        tenant_id=tenant_id,
        run_id=uuid.UUID(run_id) if run_id else None,
        claim_id=uuid.UUID(claim_id) if claim_id else None,
        kind=kind,
        validation_status=validation_status,
        source=source,
        artifact_ref=artifact_ref,
        artifact_hash=artifact_hash,
        tool_name=tool_name,
        idempotency_key=idempotency_key,
    )
    session.add(ev)
    await session.flush()
    return ev


async def list_evidence(session: AsyncSession, run_id: str | None = None, limit: int = 200) -> list[models.Evidence]:
    q = select(models.Evidence).order_by(models.Evidence.observed_at.desc()).limit(limit)
    if run_id:
        q = q.where(models.Evidence.run_id == uuid.UUID(run_id))
    res = await session.execute(q)
    return list(res.scalars().all())


async def create_claim(
    session: AsyncSession,
    tenant_id: int,
    run_id: str,
    source_event_id: str | None,
    claim_text: str,
    *,
    claim_type: str = "generic",
    severity_weight: int = 1,
    created_by_role: str = "",
) -> models.Claim:
    claim = models.Claim(
        tenant_id=tenant_id,
        run_id=uuid.UUID(run_id) if run_id else None,
        source_event_id=uuid.UUID(source_event_id) if source_event_id else None,
        claim_text=claim_text,
        claim_type=claim_type,
        severity_weight=severity_weight,
        status="claimed",
        created_by_role=created_by_role,
    )
    session.add(claim)
    await session.flush()
    return claim


async def list_claims(
    session: AsyncSession, run_id: str | None = None, status: str | None = None, limit: int = 200
) -> list[models.Claim]:
    q = select(models.Claim).order_by(models.Claim.created_at.desc()).limit(limit)
    if run_id:
        q = q.where(models.Claim.run_id == uuid.UUID(run_id))
    if status:
        q = q.where(models.Claim.status == status)
    res = await session.execute(q)
    return list(res.scalars().all())


async def set_claim_status(
    session: AsyncSession,
    claim_id: str,
    status: str,
    verified_at: Any = None,
    evidence_ids: list[str] | None = None,
) -> int:
    vals: dict[str, Any] = {"status": status}
    if verified_at is not None:
        vals["verified_at"] = verified_at
    if evidence_ids is not None:
        vals["evidence_ids"] = [uuid.UUID(x) for x in evidence_ids]
    res = await session.execute(
        update(models.Claim).where(models.Claim.id == uuid.UUID(claim_id)).values(**vals)
    )
    return int(res.rowcount or 0)


async def latest_b_state(session: AsyncSession, run_id: str) -> dict[str, Any] | None:
    res = await session.execute(
        select(models.BState)
        .where(models.BState.run_id == uuid.UUID(run_id))
        .order_by(models.BState.revision.desc())
        .limit(1)
    )
    state = res.scalar_one_or_none()
    if state is None:
        return None
    return {"revision": state.revision, "pulse": state.pulse, "payload": state.payload}


async def save_b_state(
    session: AsyncSession,
    tenant_id: int,
    run_id: str,
    payload: dict[str, Any],
    pulse: str = "post",
) -> models.BState:
    latest = await latest_b_state(session, run_id)
    revision = int(latest["revision"]) + 1 if latest else 1
    state = models.BState(
        tenant_id=tenant_id,
        run_id=uuid.UUID(run_id),
        revision=revision,
        pulse=pulse,
        payload=payload,
    )
    session.add(state)
    await session.flush()
    return state


async def count_attempts(session: AsyncSession, run_id: str) -> int:
    res = await session.execute(
        select(func.count())
        .select_from(models.ProviderAttempt)
        .where(models.ProviderAttempt.run_id == uuid.UUID(run_id))
    )
    return int(res.scalar_one())


async def enqueue_outbox(
    session: AsyncSession,
    tenant_id: int,
    aggregate: str,
    event_key: str,
    payload: dict[str, Any],
) -> models.Outbox | None:
    existing = await session.execute(select(models.Outbox).where(models.Outbox.event_key == event_key))
    if existing.scalar_one_or_none() is not None:
        return None
    row = models.Outbox(tenant_id=tenant_id, aggregate=aggregate, event_key=event_key, payload=payload)
    session.add(row)
    await session.flush()
    return row


async def record_provider_attempt(
    session: AsyncSession,
    tenant_id: int,
    *,
    run_id: str | None = None,
    call_id: str = "",
    provider_alias: str = "",
    logical_model: str = "",
    stream: bool = False,
    status: str = "SUCCESS",
    request_hash: str = "",
    latency_ms: int | None = None,
    prompt_tokens: int | None = None,
    completion_tokens: int | None = None,
    error_kind: str = "",
) -> models.ProviderAttempt:
    attempt = models.ProviderAttempt(
        tenant_id=tenant_id,
        run_id=uuid.UUID(run_id) if run_id else None,
        call_id=call_id,
        provider_alias=provider_alias,
        logical_model=logical_model,
        stream=stream,
        status=status,
        request_hash=request_hash,
        latency_ms=latency_ms,
        prompt_tokens=prompt_tokens,
        completion_tokens=completion_tokens,
    )
    if status == "FAILED":
        attempt.error_kind = error_kind
    attempt.finished_at = func.now()
    session.add(attempt)
    await session.flush()
    return attempt


async def claim_worker_jobs(session: AsyncSession, limit: int = 8, lock_seconds: int = 90) -> list[models.BJob]:
    """Idempotent claim of due jobs (pending, or failed-but-retryable) for processing."""
    res = await session.execute(
        text(
            """
            UPDATE b_jobs SET status='processing', lock_until=now() + make_interval(secs => :lock_seconds)
            WHERE id IN (
                SELECT id FROM b_jobs
                WHERE (status='pending')
                   OR (status='failed' AND attempts < max_attempts AND lock_until IS NULL)
                   OR (status='processing' AND lock_until < now())
                ORDER BY created_at ASC
                LIMIT :limit
                FOR UPDATE SKIP LOCKED
            )
            RETURNING id, tenant_id, run_id, kind, job_key, payload
            """
        ),
        {"lock_seconds": int(lock_seconds), "limit": int(limit)},
    )
    rows = res.mappings().all()
    await session.commit()
    return rows if hasattr(rows, "__iter__") else list(rows)


async def move_outbox_to_jobs(session: AsyncSession, limit: int = 16) -> int:
    res = await session.execute(
        text(
            """
            WITH pending AS (
                SELECT id, tenant_id, aggregate, event_key, payload
                FROM outbox WHERE published_at IS NULL
                ORDER BY id ASC LIMIT :limit FOR UPDATE SKIP LOCKED
            ), inserted AS (
                INSERT INTO b_jobs (id, tenant_id, run_id, kind, job_key, payload, status, max_attempts)
                SELECT gen_random_uuid(), tenant_id,
                       (payload->>'run_id')::uuid,
                       (payload->>'kind')::text,
                       event_key,
                       payload,
                       'pending',
                       COALESCE((payload->>'max_attempts')::int, 5)
                FROM pending
                ON CONFLICT (job_key) DO NOTHING
                RETURNING job_key
            )
            UPDATE outbox o SET published_at = now()
            FROM pending p
            WHERE o.id = p.id
            RETURNING (SELECT count(*) FROM inserted)
            """
        ),
        {"limit": int(limit)},
    )
    row = res.fetchone()
    await session.commit()
    return int(row[0]) if row else 0


async def mark_job_done(session: AsyncSession, job_key: str, processed_at: Any = None) -> None:
    await session.execute(
        update(models.BJob)
        .where(models.BJob.job_key == job_key)
        .values(status="done", processed_at=func.now() if processed_at is None else processed_at)
    )
    await session.commit()


async def mark_job_failed(session: AsyncSession, job_key: str) -> None:
    await session.execute(
        text(
            """
            UPDATE b_jobs SET attempts = attempts + 1,
                  status = CASE WHEN attempts + 1 >= max_attempts THEN 'failed' ELSE 'pending' END,
                  lock_until = NULL
            WHERE job_key = :key
            """
        ),
        {"key": job_key},
    )
    await session.commit()


async def count_pending_outbox(session: AsyncSession) -> int:
    res = await session.execute(select(func.count()).select_from(models.Outbox).where(models.Outbox.published_at.is_(None)))
    return int(res.scalar_one())


async def _list_jobs_by_key(session: AsyncSession, job_key: str) -> list[models.BJob]:
    res = await session.execute(
        select(models.BJob).where(models.BJob.job_key == job_key).order_by(models.BJob.created_at)
    )
    return list(res.scalars().all())


def event_to_dict(ev: models.Event) -> dict[str, Any]:
    return {
        "id": str(ev.id),
        "tenant_id": ev.tenant_id,
        "run_id": str(ev.run_id) if ev.run_id else None,
        "seq": ev.seq,
        "kind": ev.kind,
        "ts": ev.ts,
        "actor": ev.actor,
        "payload": ev.payload,
    }


def claim_to_dict(c: models.Claim) -> dict[str, Any]:
    return {
        "id": str(c.id),
        "run_id": str(c.run_id) if c.run_id else None,
        "source_event_id": str(c.source_event_id) if c.source_event_id else None,
        "claim_text": c.claim_text,
        "claim_type": c.claim_type,
        "severity_weight": c.severity_weight,
        "status": c.status,
        "created_by_role": c.created_by_role,
        "created_at": c.created_at,
        "verified_at": c.verified_at,
        "evidence_ids": [str(x) for x in (c.evidence_ids or [])],
    }


def evidence_to_dict(e: models.Evidence) -> dict[str, Any]:
    return {
        "id": str(e.id),
        "run_id": str(e.run_id) if e.run_id else None,
        "claim_id": str(e.claim_id) if e.claim_id else None,
        "kind": e.kind,
        "validation_status": e.validation_status,
        "source": e.source,
        "artifact_ref": e.artifact_ref,
        "artifact_hash": e.artifact_hash,
        "tool_name": e.tool_name,
        "observed_at": e.observed_at,
    }


def probe_stage(stage: str) -> str:
    return stage_mod.challenge_mode(stage)