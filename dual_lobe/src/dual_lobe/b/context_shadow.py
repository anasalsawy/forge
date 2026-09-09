"""B1 Context Shadow + B2 Integrity Sentinel execution (single structured B call).

The worker consumes one ``b_jobs`` row of kind ``context_shadow``, runs the
combined B prompt, and writes: a new ``b_state`` revision (derived state), ledger
claims/evidence via the classifier, and oversight challenge events. B can never
write a verdict; the verifier is the only verdict writer.
"""
from __future__ import annotations

import json
import logging
import re
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from ..core import redact
from ..core.settings import get_settings
from ..evidence import classifier
from ..provider.registry import get_registry
from ..state import repositories as repo
from . import prompts

LOG = logging.getLogger("dual_lobe.b.context_shadow")


def _parse_json(text: str) -> dict[str, Any]:
    text = (text or "").strip()
    try:
        return json.loads(text)
    except Exception:
        pass
    m = re.search(r"\{.*\}", text, re.S)
    if m:
        try:
            return json.loads(m.group(0))
        except Exception:
            pass
    return {}


def _uniq(items: Any) -> list[str]:
    if not isinstance(items, list):
        return []
    out, seen = [], set()
    for x in items:
        if isinstance(x, str) and (x := re.sub(r"\s+", " ", x).strip()) and x not in seen:
            seen.add(x)
            out.append(x)
    return out


async def _call_b(target_alias: str, prompt: str) -> dict[str, Any]:
    from ..provider.adapters import NormalizedRequest
    from ..core.settings import get_settings as _s

    s = _s()
    adapter = get_registry().adapter(target_alias)
    last: Exception | None = None
    for attempt in range(max(1, s.b_retries)):
        try:
            resp = await adapter.buffered(
                NormalizedRequest(
                    messages=[
                        {"role": "system", "content": prompts.B_SYSTEM},
                        {"role": "user", "content": prompt},
                    ],
                    temperature=0.2,
                    timeout=s.b_timeout,
                )
            )
            content = (resp.choices[0].message.content or "") if getattr(resp, "choices", None) else ""
            if not (content or "").strip():
                raise RuntimeError("empty Lobe-B response")
            return _parse_json(content)
        except Exception as e:  # noqa: PERF203
            last = e
            LOG.warning("Lobe-B attempt %d/%d failed: %s", attempt + 1, s.b_retries, e)
            if attempt + 1 < s.b_retries:
                import asyncio

                delay = min(4.0, 0.75 * (attempt + 1))
                m = re.search(r"retry in (~?)(\d+)(?:\.?\d*)?s", str(e))
                if m:
                    suggested = float(m.group(2)) * (1.5 if m.group(1) == "~" else 1.0)
                    delay = max(delay, min(90.0, suggested))
                await asyncio.sleep(delay)
    raise last or RuntimeError("Lobe-B call failed")


async def run_shadow_cycle(
    session: AsyncSession,
    job: dict[str, Any],
    tenant_id: int,
) -> dict[str, Any]:
    s = get_settings()
    payload = job.get("payload") or {}
    run_id = str(payload.get("run_id") or job.get("run_id") or "")
    external_run_id = str(payload.get("external_run_id") or "")
    context_text = str(payload.get("context_text") or "")
    response_text = str(payload.get("response_text") or "")

    events_rows = await repo.list_events(session, run_id=run_id, limit=30)
    events_preview = "\n".join(
        f"- {e.kind} {json.dumps(redact.redact_payload(e.payload or {}), ensure_ascii=False)[:1200]}"
        for e in events_rows
    )
    latest = await repo.latest_b_state(session, run_id)
    prior_state = json.dumps(latest or {}, ensure_ascii=False)

    prompt = prompts.build_cycle_prompt(
        context_text,
        response_text,
        events_preview,
        prior_state,
        max_chars=s.max_shadow_input_chars,
    )
    try:
        data = await _call_b("lobe-b", prompt)
    except Exception:
        if not s.b_fail_open:
            raise
        # Fail-open: a shadow failure must never take down the worker path. It is
        # recorded as an auditable degradation, and effective state is untouched.
        await repo.append_event(
            session, "shadow_degraded", tenant_id,
            run_id=run_id, actor="lobe-b",
            payload={"kind_ref": "context_shadow", "error": "Lobe-B model call failed"},
        )
        await session.commit()
        return {"ok": False, "degraded": True}

    payload = _build_state_payload(data, run_id)
    await repo.save_b_state(session, tenant_id, run_id, payload, pulse=str((data.get("oversight") or {}).get("pulse") or "post"))

    source_event = await repo.append_event(
        session, "b_context_shadow", tenant_id,
        run_id=run_id, actor="lobe-b",
        payload={
            "severity": payload["severity"],
            "n_injection": len(payload["injection"]),
            "n_ledger": len(payload["evidence_ledger"]),
            "n_challenges": len(payload["challenges"]),
        },
    )

    ledger = data.get("evidence_ledger") or []
    created, verified = await classifier.ingest_ledger(
        session, tenant_id, run_id, str(source_event.id), ledger,
        created_by_role="integrity_sentinel",
        cwd=".",
    )

    challenges = payload.get("challenges") or []
    if challenges:
        await repo.append_event(
            session, "oversight", tenant_id,
            run_id=run_id, actor="lobe-b",
            payload={"pulse": (data.get("oversight") or {}).get("pulse") or "post", "challenges": challenges},
        )

    if payload["severity"] in ("HIGH", "CRITICAL"):
        await repo.append_event(
            session, "oversight_escalated", tenant_id,
            run_id=run_id, actor="lobe-b",
            payload={"severity": payload["severity"], "n_challenges": len(challenges)},
        )

    await session.commit()
    return {"ok": True, "claims_created": created, "claims_verified": verified, "severity": payload["severity"]}


def _build_state_payload(data: dict[str, Any], run_id: str) -> dict[str, Any]:
    return {
        "run_id": run_id,
        "revision": 0,
        "injection": _uniq(data.get("context_injection", [])),
        "evidence_ledger": data.get("evidence_ledger", []),
        "blocker_insights": _uniq(data.get("blocker_insights", [])),
        "required_preconditions": _uniq(data.get("required_preconditions", [])),
        "state_corrections": _uniq(data.get("state_corrections", [])),
        "unasked_questions": _uniq(data.get("unasked_questions", [])),
        "hidden_assumptions": _uniq(data.get("hidden_assumptions", [])),
        "context_expansion": _uniq(data.get("context_expansion", [])),
        "blocker_hypotheses": _uniq(data.get("blocker_hypotheses", [])),
        "severity": str(data.get("severity", "LOW")).upper(),
        "challenges": (data.get("oversight") or {}).get("challenges", []),
        "oversight_status": "ok",
    }