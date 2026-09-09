"""Transactional outbox -> idempotent B jobs -> shadow cycle writes derived state."""
from __future__ import annotations

import asyncio

from dual_lobe.b.context_shadow import run_shadow_cycle
from dual_lobe.b.outbox import shadow_job_key, shadow_payload
from dual_lobe.core.bootstrap import ensure_tenant
from dual_lobe.core.engine import admin_session_factory, dispose_engines
from dual_lobe.state import repositories as repo


async def _seed_run(run_ext: str = "biz-run-1"):
    async with admin_session_factory()() as s:
        t = await ensure_tenant(s, "biz", "Biz")
        run = await repo.get_or_create_run(s, t.id, run_ext, floor_id="f1", attempt=1, goal="reveal")
        run_id = str(run.id)
        ctx = "context of the run"
        resp = "response text"
        job_key = shadow_job_key(t.id, run_id, 1, ctx, resp)
        payload = shadow_payload(t.id, run_id, run_ext, 1, ctx, resp, floor_id="f1", stage="observation")
        await repo.enqueue_outbox(s, t.id, "b", job_key, payload)
        await s.commit()
        return t.id, run_id, job_key


def test_outbox_idempotent_duplicate_key():
    async def _run():
        tid, run_id, job_key = await _seed_run("dup-run")
        async with admin_session_factory()() as s:
            again = await repo.enqueue_outbox(s, tid, "b", job_key, {"whatever": 1})
            assert again is None
        async with admin_session_factory()() as s:
            moved = await repo.move_outbox_to_jobs(s, limit=16)
        # may also drain unpublished outbox rows left by HTTP tests; >= 1 here
        assert moved >= 1
        async with admin_session_factory()() as s:
            # idempotent move: nothing left to publish, and only one b_job for this key
            moved2 = await repo.move_outbox_to_jobs(s, limit=16)
            assert moved2 == 0
            jobs = await repo._list_jobs_by_key(s, job_key)
            assert len(jobs) == 1
        await dispose_engines()

    asyncio.run(_run())


def test_worker_cycle_writes_derived_state(monkeypatch):
    from tests.helpers import FakeBExtractor

    async def _run():
        tid, run_id, job_key = await _seed_run("cycle-run")
        extractor = FakeBExtractor(
            payload={
                "oversight": {"pulse": "post", "challenges": [{"claim": "the fix is safe", "severity": "HIGH"}]},
                "context_injection": ["current working directory is /repo; use pwd first"],
                "evidence_ledger": [
                    {"claim": "the deploy passed", "status": "VERIFIED", "severity": "MEDIUM"},
                    {"claim": "created ./out/report.txt as stated", "status": "VERIFIED", "severity": "LOW"},
                ],
                "required_preconditions": ["verify config exists"],
                "blocker_insights": [],
                "state_corrections": [],
                "unasked_questions": ["what about staging?"],
                "hidden_assumptions": ["network available"],
                "context_expansion": [],
                "blocker_hypotheses": [],
                "severity": "HIGH",
            }
        )
        monkeypatch.setattr("dual_lobe.b.context_shadow._call_b", extractor)

        async with admin_session_factory()() as s:
            await repo.move_outbox_to_jobs(s, limit=16)
        async with admin_session_factory()() as s:
            jobs = await repo._list_jobs_by_key(s, job_key)
            jo = {
                "tenant_id": tid,
                "run_id": run_id,
                "kind": jobs[0].kind,
                "job_key": jobs[0].job_key,
                "payload": jobs[0].payload,
            }
            result = await run_shadow_cycle(s, jo, tid)
            await repo.mark_job_done(s, job_key)
            await s.commit()

        assert result["ok"] is True
        assert result["claims_created"] == 2

        async with admin_session_factory()() as s:
            st = await repo.latest_b_state(s, run_id)
            assert st is not None
            assert st["revision"] == 1
            assert st["pulse"] == "post"
            assert st["payload"]["severity"] == "HIGH"
            assert st["payload"]["injection"]
            claims = await repo.list_claims(s, run_id=run_id)
            by_status: dict[str, int] = {}
            for c in claims:
                by_status[c.status] = by_status.get(c.status, 0) + 1
            # file claim resolves to cwd="." so path missing -> contradicted; generic stays claimed
            assert by_status.get("verified", 0) == 0
            assert by_status.get("claimed") == 1
            assert by_status.get("contradicted") == 1
            generic = [c for c in claims if c.claim_text == "the deploy passed"]
            assert generic and generic[0].status == "claimed"
            events = await repo.list_events(s, run_id=run_id)
            kinds = {e.kind for e in events}
            assert {"b_context_shadow", "oversight", "oversight_escalated"} <= kinds
        await dispose_engines()

    asyncio.run(_run())


def test_shadow_failure_fails_open_no_state(monkeypatch):
    from tests.helpers import FakeBExtractor

    async def _run():
        tid, run_id, job_key = await _seed_run("failopen-run")
        monkeypatch.setattr(
            "dual_lobe.b.context_shadow._call_b",
            FakeBExtractor(fail=RuntimeError("provider down")),
        )
        async with admin_session_factory()() as s:
            await repo.move_outbox_to_jobs(s, limit=16)
        async with admin_session_factory()() as s:
            jobs = await repo._list_jobs_by_key(s, job_key)
            jo = {
                "tenant_id": tid,
                "run_id": run_id,
                "kind": jobs[0].kind,
                "job_key": jobs[0].job_key,
                "payload": jobs[0].payload,
            }
            result = await run_shadow_cycle(s, jo, tid)
        assert result["ok"] is False and result["degraded"] is True
        async with admin_session_factory()() as s:
            assert await repo.latest_b_state(s, run_id) is None
            events = await repo.list_events(s, run_id=run_id)
            assert any(e.kind == "shadow_degraded" for e in events)
        await dispose_engines()

    asyncio.run(_run())


def test_verifier_grounds_file_claim(tmp_path):
    import os

    from dual_lobe.core.bootstrap import ensure_tenant
    from dual_lobe.core.engine import admin_session_factory, dispose_engines
    from dual_lobe.evidence import verifier

    target = tmp_path / "created.txt"
    target.write_text("hello")

    async def _run():
        async with admin_session_factory()() as s:
            t = await ensure_tenant(s, "verify-tenant", "Verify")
            ok, ev = await verifier.verify_artifact(
                s, t.id, "file_exists", str(target), run_id=None
            )
            assert ok is True
            assert ev.validation_status == "PASS"
            # re-verify: idempotent (no crash, still PASS)
            ok2, ev2 = await verifier.verify_artifact(s, t.id, "file_exists", str(target), run_id=None)
            assert ok2 is True
            assert ev2.validation_status == "PASS"
        await dispose_engines()

    asyncio.run(_run())


def test_severity_weight_map():
    from dual_lobe.evidence.classifier import SEVERITY_WEIGHT

    assert SEVERITY_WEIGHT == {"CRITICAL": 5, "HIGH": 3, "MEDIUM": 2, "LOW": 1}