"""OpenAI-compatible chat completions pass-through with dual-lobe correlation,
transparent Lobe-A proxying, provider-attempt accounting, and off-critical-path
Lobe-B job enqueueing (transactional outbox)."""
from __future__ import annotations

import json
import logging
import re
import time
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import JSONResponse, StreamingResponse

from ..b.outbox import shadow_job_key, shadow_payload
from ..core import stage as stage_mod
from ..core.engine import tenant_session
from ..core.settings import get_settings
from ..provider.adapters import resolve_request
from ..provider.registry import get_registry
from ..state import repositories as repo
from . import auth, correlation, limits
from .schemas import ChatCompletionRequest

LOG = logging.getLogger("dual_lobe.api.chat")

router = APIRouter()


def _token_estimate(messages: list[dict[str, Any]]) -> int:
    total = 0
    for m in messages:
        content = m.get("content", "")
        if isinstance(content, str):
            total += len(content)
        elif isinstance(content, list):
            total += sum(len(str(x)) for x in content)
    return max(0, total // 4)


def _messages_text(messages: list[dict[str, Any]], max_chars: int) -> str:
    parts = []
    for m in messages:
        content = m.get("content", "")
        if isinstance(content, list):
            content = "\n".join(
                str(x.get("text", x)) if isinstance(x, dict) else str(x) for x in content
            )
        parts.append(f"[{m.get('role', 'unknown')}] {content}")
    return "\n\n".join(parts)[-max_chars:]


def _injection_message(state_payload: dict[str, Any] | None, max_chars: int) -> str | None:
    if not state_payload:
        return None
    parts: list[str] = []
    injection = state_payload.get("injection") or []
    if injection:
        parts += injection
    for x in state_payload.get("required_preconditions") or []:
        parts.append(f"Required precondition: {x}")
    for x in state_payload.get("state_corrections") or []:
        parts.append(f"Current effective state: {x}")
    for x in (state_payload.get("blocker_insights") or [])[:6]:
        parts.append(f"Relevant blocker context: {x}")
    for x in (state_payload.get("unasked_questions") or [])[:5]:
        parts.append(f"Unresolved question: {x}")
    if not parts:
        return None
    body = "\n".join(f"- {x}" for x in parts)[:max_chars]
    return "Additional working context and execution constraints:\n" + body


def _challenge_lines(state_payload: dict[str, Any] | None, mode: str, max_chars: int) -> list[str]:
    if not state_payload:
        return []
    severity = str(state_payload.get("severity") or "LOW").upper()
    challenges = state_payload.get("challenges") or []
    lines: list[str] = []
    for c in challenges[:5]:
        claim = str(c.get("claim") or "")
        if not claim:
            continue
        csev = str(c.get("severity") or state_payload.get("severity") or "LOW").upper()
        if mode == "block" and csev == "CRITICAL":
            lines.append(f"Hold before treating the following as settled: {claim}")
        elif mode == "precondition" and csev in ("HIGH", "CRITICAL"):
            lines.append(f"Verify before relying on this: {claim}")
        elif mode == "warn" and csev in ("HIGH", "CRITICAL"):
            lines.append(f"Do not rely on the following until confirmed: {claim}")
        elif mode in ("note", "warn"):
            lines.append(f"Consider verifying: {claim}")
    return [x[:max_chars] for x in lines]


async def _shadow_projection(request: Request, principal: auth.Principal, run_id: str) -> str:
    """Compact, attributable evidence projection for Analyst/Auditor calls.

    Reviewers bypass the silent worker injection but must read the ledger
    independently. If the ledger is unreachable, no clean PASS may be issued.
    """
    from ..core.redact import redact_payload

    async with tenant_session(principal.tenant_id) as session:
        try:
            events = await repo.list_events(session, run_id=run_id, limit=40)
            claims = await repo.list_claims(session, run_id=run_id, limit=40)
        except Exception:
            LOG.exception("shadow ledger read failed; projecting unavailable")
            return (
                "<shadow_context>\n"
                "Evidence ledger is CURRENTLY UNREACHABLE. No clean PASS may be issued until "
                "evidence persistence is restored and the material claims are recorded.\n"
                "</shadow_context>"
            )
    pend = sum(1 for c in claims if c.status in ("claimed", "unverified"))
    lines = ["<shadow_context>", "Durable evidence ledger (append-only):"]
    for e in events[:10]:
        if e.kind == "worker_call":
            lines.append(f"- worker call ({e.payload.get('floor', '?')})")
        elif e.kind == "oversight":
            for c in (e.payload.get("challenges") or [])[:3]:
                lines.append(f"- oversight {e.payload.get('pulse', 'post')}: verify {c.get('claim')}")
    lines.append(f"{pend} unsettled/unverified claim(s); ledger reachable")
    if pend:
        lines.append("Do not treat unresolved claims as settled. A clean PASS requires claims "
                     "material to the verdict to be VERIFIED or explicitly waived in the verdict.")
    lines.append("</shadow_context>")
    return "\n".join(lines)


def _to_openai_response(resp: Any, public_model: str) -> dict[str, Any]:
    if hasattr(resp, "model_dump"):
        data = resp.model_dump()
    elif isinstance(resp, dict):
        data = dict(resp)
    else:
        data = json.loads(resp.json())
    data["model"] = public_model
    return data


def _is_retryable(e: Exception) -> bool:
    msg = str(e).lower()
    return any(
        k in msg
        for k in ("empty", "429", "500", "502", "503", "504", "timeout", "rate limit", "insufficient credits", "connection", "service unavailable")
    )


async def _call_a_with_retry(fn, retries: int):
    last: Exception | None = None
    for attempt in range(max(1, retries)):
        try:
            out = await fn()
            choices = getattr(out, "choices", None)
            if choices is None and isinstance(out, dict):
                choices = out.get("choices")
            if not choices:
                raise RuntimeError(f"empty model response ({getattr(out, 'status', '?')})")
            return out
        except Exception as e:  # noqa: PERF203
            last = e
            LOG.warning("Lobe-A attempt %d/%d failed: %s", attempt + 1, retries, e)
            if not _is_retryable(e):
                break
            if attempt + 1 < retries:
                await asyncio_backoff(attempt, str(e))
    raise last


async def asyncio_backoff(attempt: int, err: str) -> None:
    import asyncio

    delay = min(4.0, 0.75 * (attempt + 1))
    m = re.search(r"retry in (~?)(\d+)(?:\.?\d*)?s", err)
    if m:
        suggested = float(m.group(2)) * (1.5 if m.group(1) == "~" else 1.0)
        delay = max(delay, min(90.0, suggested))
    await asyncio.sleep(delay)


def _sse_body(deltas: list[dict[str, Any]], public_model: str, stream_id: str, usage: Any) -> str:
    lines: list[str] = []
    for i, d in enumerate(deltas):
        finish = d.get("finish_reason") if i == len(deltas) - 1 else None
        chunk = {
            "id": stream_id,
            "object": "chat.completion.chunk",
            "model": public_model,
            "choices": [{"index": 0, "delta": d.get("delta", {}), "finish_reason": finish}],
        }
        lines.append(f"data: {json.dumps(chunk, ensure_ascii=False)}\n\n")
    if usage:
        last = {
            "id": stream_id,
            "object": "chat.completion.chunk",
            "model": public_model,
            "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}],
            "usage": usage,
        }
        lines.append(f"data: {json.dumps(last, ensure_ascii=False)}\n\n")
    lines.append("data: [DONE]\n\n")
    return "".join(lines)


async def _extract_usage(resp: Any) -> dict[str, Any] | None:
    try:
        u = resp.usage
        if hasattr(u, "model_dump"):
            return u.model_dump()
        return dict(u)
    except Exception:
        return None


@router.post("/v1/chat/completions")
async def chat_completions(
    body: ChatCompletionRequest,
    request: Request,
    principal: auth.Principal = Depends(auth.require_scope(auth.SCOPE_INFERENCE_INVOKE)),
):
    s = get_settings()
    payload = body.model_dump(exclude_none=True)
    messages = payload.get("messages") or []
    if not isinstance(messages, list) or not messages:
        raise HTTPException(status_code=400, detail="messages must be a non-empty list")

    corr = correlation.parse_headers(request.headers)
    bypass = correlation.is_bypass(corr)
    target_alias = payload.get("model") or "lobe-a"
    if not bool(get_registry().target(target_alias).enabled):
        raise HTTPException(status_code=400, detail=f"unknown model: {target_alias}")

    limit = await limits.check_limits(principal.tenant_id, _token_estimate(messages))
    if not limit.allowed:
        raise HTTPException(
            status_code=429,
            detail="rate limit exceeded",
            headers={"Retry-After": str(int(limit.retry_after + 1))},
        )

    external_run = str(corr.get("run") or request.headers.get("X-DL-Run-ID") or "")
    call_seq = int(corr.get("call_seq") or 0)
    started = time.monotonic()

    async with tenant_session(principal.tenant_id) as session:
        run = await repo.get_or_create_run(
            session,
            principal.tenant_id,
            external_run,
            floor_id=str(corr.get("floor") or ""),
            attempt=int(corr.get("attempt") or 1),
            goal=_messages_text(messages, 500),
        )
        run_id = str(run.id)

        effective = list(messages)
        injection: str | None = None
        mode = stage_mod.challenge_mode(s.rollout_stage)
        if bypass:
            injection = await _shadow_projection(request, principal, run_id)
        else:
            latest = await repo.latest_b_state(session, run_id)
            if latest and stage_mod.injection_enabled(s.rollout_stage):
                injection = _build_injection(latest.get("payload"), mode, s.max_injection_chars)
        if injection:
            effective.insert(max(0, len(effective) - 1), {"role": "system", "content": injection})

        context_text = _messages_text(effective, s.max_shadow_input_chars)
        n_calls = await repo.count_attempts(session, run_id)
        pulse_every = max(1, s.pulse_every)
        pulse = (not bypass) and (n_calls == 0 or (n_calls % pulse_every) == 0)

        req = resolve_request(payload)
        req.messages = effective
        req.timeout = s.a_timeout
        adapter = get_registry().adapter(target_alias)
        stream = bool(payload.get("stream", False))
        status_code = "SUCCESS"
        error_kind = ""
        deltas: list[dict[str, Any]] = []
        buffered_resp = None
        usage = None

        if stream:
            try:
                async for d in adapter.stream(req):
                    deltas.append(d)
            except Exception as e:
                LOG.exception("upstream stream interrupted")
                status_code = "INCOMPLETE"
                error_kind = f"{type(e).__name__}: {e}"
        else:
            try:
                buffered_resp = await _call_a_with_retry(
                    lambda: adapter.buffered(req), s.a_retries
                )
                usage = await _extract_usage(buffered_resp)
            except Exception as e:
                status_code = "FAILED"
                error_kind = f"{type(e).__name__}: {e}"

        latency_ms = int((time.monotonic() - started) * 1000)
        await repo.record_provider_attempt(
            session,
            principal.tenant_id,
            run_id=run_id,
            call_id=str(call_seq),
            provider_alias=target_alias,
            logical_model=get_registry().target(target_alias).model,
            stream=stream,
            status=status_code,
            latency_ms=latency_ms,
            error_kind=error_kind,
        )

        output = ""
        data: dict[str, Any] | None = None
        if not stream and buffered_resp is not None:
            data = _to_openai_response(buffered_resp, target_alias)
            try:
                output = data["choices"][0]["message"].get("content") or ""
            except Exception:
                output = ""
        elif stream and status_code == "SUCCESS":
            output = "".join(d.get("delta", {}).get("content") or "" for d in deltas)

        if status_code == "FAILED":
            await repo.append_event(
                session, "worker_error", principal.tenant_id,
                run_id=run_id, actor=target_alias,
                payload={"error": error_kind},
            )
            await session.commit()
            raise HTTPException(status_code=502, detail=f"upstream provider error: {error_kind}")

        await repo.append_event(
            session,
            "worker_call",
            principal.tenant_id,
            run_id=run_id,
            actor=target_alias,
            payload={
                "call": call_seq,
                "bypass": bypass,
                "status": status_code,
                "latency_ms": latency_ms,
                "mode": corr.get("mode") or "",
                "role": corr.get("role") or "",
            },
        )

        if pulse and not bypass:
            job_key = shadow_job_key(
                principal.tenant_id, run_id, call_seq, context_text, output
            )
            await repo.enqueue_outbox(
                session,
                principal.tenant_id,
                "b",
                job_key,
                shadow_payload(
                    principal.tenant_id,
                    run_id,
                    external_run,
                    call_seq,
                    context_text,
                    output,
                    floor_id=str(corr.get("floor") or ""),
                    attempt_id=int(corr.get("attempt") or 1),
                    stage=s.rollout_stage,
                ),
            )

        await session.commit()

    stream_id = f"chatcmpl-{run_id[:8]}"
    if stream:
        if status_code == "INCOMPLETE":
            body_str = (
                f"data: {json.dumps({'id': stream_id, 'object': 'chat.completion.chunk', 'model': target_alias, 'choices': [{'index': 0, 'delta': {}, 'finish_reason': 'INCOMPLETE'}]}, ensure_ascii=False)}\n\n"
                "data: [DONE]\n\n"
            )
        else:
            body_str = _sse_body(deltas, target_alias, stream_id, usage)
        return StreamingResponse(
            iter([body_str]),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Dual-Lobe-Run-Id": run_id},
        )

    return JSONResponse(data or {}, headers={"X-Dual-Lobe-Run-Id": run_id})


def _build_injection(state_payload: dict[str, Any] | None, mode: str, max_chars: int) -> str | None:
    base = _injection_message(state_payload, max_chars - 400)
    extra = _challenge_lines(state_payload, mode, max_chars - 400)
    if not base and not extra:
        return None
    parts = []
    if base:
        parts.append(base)
    if extra:
        parts.append("\n".join(f"- {x}" for x in extra))
    return "Additional working context and execution constraints:\n" + "\n".join(parts)[:max_chars]