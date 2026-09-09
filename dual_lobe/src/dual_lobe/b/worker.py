"""B worker: polls the outbox, moves rows into idempotent ``b_jobs``, and runs
Lobe-B shadow cycles with per-run serialization and bounded concurrency."""
from __future__ import annotations

import asyncio
import logging
import uuid

from sqlalchemy import text

from ..core.engine import admin_session_factory, dispose_engines
from ..core.settings import get_settings
from ..state import repositories as repo
from .context_shadow import run_shadow_cycle

LOG = logging.getLogger("dual_lobe.b.worker")


def _lock_key(run_id: str) -> int:
    try:
        return uuid.UUID(run_id).int & ((1 << 63) - 1)
    except Exception:  # pragma: no cover - defensive
        return abs(hash(run_id)) & ((1 << 63) - 1)


async def _process_job(job: dict) -> None:
    s = get_settings()
    run_id = str(job.get("run_id") or (job.get("payload") or {}).get("run_id") or "")
    job_key = str(job.get("job_key"))
    tenant_id = int(job.get("tenant_id"))
    if not run_id:
        async with admin_session_factory()() as session:
            await repo.mark_job_failed(session, job_key)
        return
    try:
        async with admin_session_factory()() as session:
            # Per-run serialization: only one B job for a given run processes at a time.
            await session.execute(text("SELECT pg_advisory_xact_lock(:k)"), {"k": _lock_key(run_id)})
            await repo.append_event(
                session, "b_job_start", tenant_id,
                run_id=run_id, actor="worker",
                payload={"kind": job.get("kind"), "job_key": job_key},
            )
            await session.commit()
            async with admin_session_factory()() as session:
                await session.execute(text("SELECT pg_advisory_xact_lock(:k)"), {"k": _lock_key(run_id)})
                result = await run_shadow_cycle(session, job, tenant_id)
            LOG.info("Lobe-B job done run=%s result=%s", run_id, result)
    except Exception:
        LOG.exception("Lobe-B job failed run=%s job_key=%s", run_id, job_key)
        try:
            async with admin_session_factory()() as session:
                await repo.mark_job_failed(session, job_key)
        except Exception:
            LOG.exception("could not mark job failed")
        return
    try:
        async with admin_session_factory()() as session:
            await repo.mark_job_done(session, job_key)
    except Exception:
        LOG.exception("could not mark job done")


async def _cycle_once(s: object) -> None:
    async with admin_session_factory()() as session:
        await repo.move_outbox_to_jobs(session, limit=16)
    async with admin_session_factory()() as session:
        jobs = await repo.claim_worker_jobs(
            session, limit=_slots(), lock_seconds=get_settings().worker_lock_seconds
        )
    if jobs:
        await asyncio.gather(*(_process_job(j) for j in jobs))


def _slots() -> int:
    return max(1, get_settings().worker_max_concurrency)


async def run_forever() -> None:
    s = get_settings()
    LOG.info("B worker starting (poll=%ss, concurrency=%d)", s.worker_poll_seconds, s.worker_max_concurrency)
    while True:
        try:
            await _cycle_once(s)
        except asyncio.CancelledError:
            LOG.info("B worker stopping")
            return
        except Exception:
            LOG.exception("B worker cycle error")
        await asyncio.sleep(s.worker_poll_seconds)


def main() -> None:
    logging.basicConfig(level=get_settings().log_level, format="%(asctime)s %(levelname)s %(message)s")
    try:
        asyncio.run(run_forever())
    except KeyboardInterrupt:
        pass
    finally:
        asyncio.run(dispose_engines())


if __name__ == "__main__":
    main()