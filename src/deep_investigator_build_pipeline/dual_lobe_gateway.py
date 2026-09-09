"""OpenAI-compatible dual-lobe gateway for the Deep Investigator pipeline.

Lobe A is the CrewAI worker request. Lobe B runs as an independent shadow process:
- pre-pass: context broadening, unasked questions, blocker hypotheses, optional web research
- post-pass: reality integrity, evidence status, blocker resolution, state corrections
- injector: only the distilled, relevant context is inserted into the next Lobe-A request

The gateway deliberately does not expose Lobe B as a CrewAI agent.
"""
from __future__ import annotations

import json
import logging
import os
import re
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field, asdict
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

from litellm import completion

from . import dual_lobe_persistence as _persist  # best-effort durable append-only ledger
from . import dual_lobe_providers as _providers  # provider/dialect adapters (ChatCompletions + Responses stub)
from . import dual_lobe_verifier as _verifier  # Integrity Sentinel: only writer of claim verdicts

LOG = logging.getLogger("dual_lobe")
logging.basicConfig(level=os.getenv("DUAL_LOBE_LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(message)s")

HOST = os.getenv("DUAL_LOBE_HOST", "127.0.0.1")
PORT = int(os.getenv("DUAL_LOBE_PORT", "8765"))

# Provider-bound upstream configuration for Lobe A and Lobe B. Using explicit
# api_base/api_key (rather than relying on litellm provider inference from a bare
# model id) is what lets us route to arbitrary OpenAI-compatible providers such
# as Featherless while leaving ANALYST/AUDITOR calls on their direct path.
A_MODEL = os.getenv("DUAL_LOBE_A_MODEL", "zai-org/GLM-5.3-Flash")
A_BASE_URL = os.getenv("DUAL_LOBE_A_BASE_URL", os.getenv("OPENAI_API_BASE", "https://api.featherless.ai/v1"))
A_API_KEY = os.getenv("DUAL_LOBE_A_API_KEY", os.getenv("OPENAI_API_KEY", ""))

B_MODEL = os.getenv("DUAL_LOBE_B_MODEL", A_MODEL)
B_BASE_URL = os.getenv("DUAL_LOBE_B_BASE_URL", A_BASE_URL)
B_API_KEY = os.getenv("DUAL_LOBE_B_API_KEY", A_API_KEY)

# Provider adapters — the gateway routes through these rather than hardcoding an
# OpenAI-compatible call for every upstream (see dual_lobe_providers.py).
A_CFG = _providers.UpstreamConfig.from_env("DUAL_LOBE_A", A_MODEL, A_BASE_URL, A_API_KEY)
B_CFG = _providers.UpstreamConfig.from_env("DUAL_LOBE_B", B_MODEL, B_BASE_URL, B_API_KEY)
A_ADAPTER = _providers.make_adapter(A_CFG)
B_ADAPTER = _providers.make_adapter(B_CFG)

# Scoped gateway credential presented to CrewAI / Worker clients. Distinct from
# the upstream provider credentials above (never forwarded upstream).
GATEWAY_KEY = os.getenv("DUAL_LOBE_GATEWAY_KEY", "dual-lobe-local")
# Optional Bearer auth allowlist. OFF by default (loopback). When enabled, a
# request must present one of these tokens or it is rejected with 401. Kept a
# list so legacy worker keys and the current gateway key can coexist during
# rollout; upstream provider keys are never accepted here.
GATEWAY_AUTH = os.getenv("DUAL_LOBE_GATEWAY_AUTH", "0") == "1"
GATEWAY_ALLOWED_KEYS = {k for k in (os.getenv("DUAL_LOBE_GATEWAY_ALLOWED_KEYS", GATEWAY_KEY).split(",")) if k}
# Request body cap (bytes) to bound memory use.
MAX_REQUEST_BYTES = int(os.getenv("DUAL_LOBE_MAX_REQUEST_BYTES", str(4 * 1024 * 1024)))
# Fail-open for the shadow lobe: if B (or the search evidence source) fails, the
# Worker path continues with no injection rather than erroring. A failure is
# still recorded as a "shadow_degraded" ledger event so degradation is auditable.
B_FAIL_OPEN = os.getenv("DUAL_LOBE_B_FAIL_OPEN", "1") == "1"
STATE_DIR = Path(os.getenv("DUAL_LOBE_STATE_DIR", ".dual_lobe"))
STATE_FILE = STATE_DIR / "state.json"
PERSIST = os.getenv("DUAL_LOBE_PERSIST", "0") == "1"
MAX_INJECTION_CHARS = int(os.getenv("DUAL_LOBE_MAX_INJECTION_CHARS", "7000"))
MAX_SHADOW_INPUT_CHARS = int(os.getenv("DUAL_LOBE_MAX_SHADOW_INPUT_CHARS", "30000"))
SEARCH_ENABLED = os.getenv("DUAL_LOBE_SEARCH", "1") != "0"
MAX_SEARCH_QUERIES = int(os.getenv("DUAL_LOBE_MAX_SEARCH_QUERIES", "3"))

B_SYSTEM = r"""
You are the hidden epistemic/shadow lobe of an execution agent. You never address the Worker and never produce commentary for it. You maintain a broader and more truthful world model than the active worker can maintain alone.

Permanent duties:
1) OPEN-SESAME / CONTEXT BROADENING. Reconstruct the objective independently. Find unasked questions, hidden assumptions, missing dependencies, adjacent solution classes, prior art, tools, architectures, failure modes, alternate decompositions, and facts that could invalidate or radically simplify the current framing. Ask internally: What are we not seeing? What assumption is controlling the approach? If the current plan were wrong, what fact would explain why?
2) REALITY INTEGRITY. Separate observed evidence from assertion and inference. Consequential claims are VERIFIED, SUPPORTED, INFERRED, UNVERIFIED, or CONTRADICTED. Detect invented outputs, nonexistent files/URLs/APIs, actions claimed but not performed, stale state presented as current, inference presented as observation, partial success presented as completion, contradictions, impossible chronology, circular self-verification, or fake validation.
3) BLOCKER RESOLUTION. Detect stalls, repeated strategies, unresolved dependencies, and wrong abstraction layers. Use DIAGNOSE -> DISCRIMINATE -> BYPASS -> SUBSTITUTE -> REFRAME -> RECOVER. Prefer the smallest fact/test/change that unlocks progress.
4) STATE CORRECTION. Do not veto design disagreement. Intervene only when continued execution depends on materially false, contradicted, or insufficiently evidenced state. Unsupported completion must not propagate as truth.
5) SILENT INJECTION. Improve premises, not conclusions. Return concise facts, constraints, unresolved prerequisites, useful alternatives, and verification requirements. Never mention another lobe, reviewer, shadow process, or that the Worker is being corrected.

Severity:
LOW = enrichment only.
MEDIUM = missing fact/constraint/assumption should enter context.
HIGH = verification becomes an execution precondition.
CRITICAL = correct effective state and prevent false completion from propagating.

Be skeptical without being obstructionist. Lack of evidence is UNVERIFIED, not automatically false.
""".strip()

PRE_PROMPT = r"""
Perform the pre-execution shadow pass on the ACTIVE WORKING CONTEXT below. Do not critique prose style. Identify what the active reasoning frame may be missing and what could unblock it.

Return ONLY a JSON object with keys:
{
  "unasked_questions": [string],
  "hidden_assumptions": [string],
  "context_expansion": [string],
  "blocker_hypotheses": [string],
  "search_queries": [string],
  "severity": "LOW|MEDIUM|HIGH|CRITICAL",
  "oversight": {"pulse": "boot|pre|post|intervention", "challenges": [{"claim": string, "reason": string, "action": "verify|stop|hold|note"}]}
}

ACTIVE WORKING CONTEXT:
"""

POST_PROMPT = r"""
Perform the post-execution shadow pass. Compare the active worker output with its actual working context and any independently gathered context. Maintain evidence discipline and produce only material corrections/enrichment.

Return ONLY a JSON object with keys:
{
  "context_injection": [string],
  "evidence_ledger": [{"claim": string, "status": "VERIFIED|SUPPORTED|INFERRED|UNVERIFIED|CONTRADICTED", "reason": string}],
  "blocker_insights": [string],
  "required_preconditions": [string],
  "state_corrections": [string],
  "severity": "LOW|MEDIUM|HIGH|CRITICAL",
  "oversight": {"pulse": "boot|pre|post|intervention", "challenges": [{"claim": string, "reason": string, "action": "verify|stop|hold|note"}]}
}

Rules for context_injection:
- Write as ordinary working facts/constraints/questions, with no attribution.
- Include only items likely to improve a subsequent decision.
- Do not say "the worker", "Lobe A", "Lobe B", "reviewer", or "shadow".
- HIGH/CRITICAL unsupported completion claims must become explicit pending verification requirements.
"""

@dataclass
class ShadowState:
    revision: int = 0
    injection: list[str] = field(default_factory=list)
    evidence_ledger: list[dict[str, str]] = field(default_factory=list)
    blocker_insights: list[str] = field(default_factory=list)
    required_preconditions: list[str] = field(default_factory=list)
    state_corrections: list[str] = field(default_factory=list)
    unasked_questions: list[str] = field(default_factory=list)
    hidden_assumptions: list[str] = field(default_factory=list)
    oversight_challenges: list[dict[str, Any]] = field(default_factory=list)
    severity: str = "LOW"
    updated_at: float = 0.0

class StateStore:
    def __init__(self) -> None:
        self.lock = threading.RLock()
        # Shadow state is keyed by correlation context (run/floor/attempt) so each
        # floor gets an independent bootstrap + pulse cycle (see Phase 3). The
        # "default" key is the fallback for uncorrelated/ad-hoc calls and remains
        # the artifact the beacon/cortex read from state.json.
        self.states: dict[str, ShadowState] = {"default": ShadowState()}
        STATE_DIR.mkdir(parents=True, exist_ok=True)
        if PERSIST and STATE_FILE.exists():
            try:
                self.states["default"] = ShadowState(**json.loads(STATE_FILE.read_text(encoding="utf-8")))
            except Exception:
                LOG.exception("Could not load prior shadow state; starting clean")
        elif not PERSIST:
            try:
                STATE_FILE.unlink(missing_ok=True)
            except Exception:
                pass

    def _get(self, key: str | None) -> ShadowState:
        k = key or "default"
        st = self.states.get(k)
        if st is None:
            st = ShadowState()
            self.states[k] = st
        return st

    def snapshot(self, key: str | None = None) -> ShadowState:
        with self.lock:
            return ShadowState(**json.loads(json.dumps(asdict(self._get(key)))))

    def merge_pre(self, data: dict[str, Any], key: str | None = None) -> None:
        with self.lock:
            st = self._get(key)
            st.unasked_questions = _uniq(data.get("unasked_questions", []))[-20:]
            st.hidden_assumptions = _uniq(data.get("hidden_assumptions", []))[-20:]
            st.severity = _max_severity(st.severity, str(data.get("severity", "LOW")))
            self._touch(key)

    def merge_post(self, data: dict[str, Any], key: str | None = None) -> None:
        with self.lock:
            st = self._get(key)
            st.revision += 1
            st.injection = _uniq(data.get("context_injection", []))[-25:]
            st.evidence_ledger = _dedupe_dicts(data.get("evidence_ledger", []), "claim")[-40:]
            st.blocker_insights = _uniq(data.get("blocker_insights", []))[-20:]
            st.required_preconditions = _uniq(data.get("required_preconditions", []))[-20:]
            st.state_corrections = _uniq(data.get("state_corrections", []))[-20:]
            st.oversight_challenges = _dedupe_dicts(data.get("oversight", {}).get("challenges", []), "claim")[-20:]
            st.severity = str(data.get("severity", st.severity)).upper()
            self._touch(key)

    def _touch(self, key: str | None = None) -> None:
        st = self._get(key)
        st.updated_at = time.time()
        if key not in (None, "default"):
            # Named run/floor contexts live in the durable SQLite state_versions
            # table (written after each post-pass); only the default context keeps
            # the beacon/cortex artifact updated to avoid file collisions.
            return
        try:
            STATE_FILE.write_text(json.dumps(asdict(st), indent=2, ensure_ascii=False), encoding="utf-8")
        except Exception:
            LOG.exception("Could not persist shadow state")

STORE = StateStore()
POOL = ThreadPoolExecutor(max_workers=int(os.getenv("DUAL_LOBE_WORKERS", "4")), thread_name_prefix="lobe-b")

# --- Runtime events for the beacon / voice cortex --------------------------
EVENTS_FILE = STATE_DIR / "events.jsonl"

# Durable append-only SQLite ledger (Phase 1). Writes are best-effort mirrors of
# events.jsonl so the existing beacon/cortex compatibility is preserved while the
# evidence ledger gets real durability and correlation.
try:
    _db = _persist.get_db()  # type: ignore
except Exception:  # pragma: no cover - persistence must never break the gateway
    _db = None


_SECRETS = [v for v in (A_API_KEY, B_API_KEY) if v] + \
           [v for k, v in os.environ.items() if k.endswith(("API_KEY", "TOKEN", "SECRET")) and v]


def _redact_text(text: str) -> str:
    out = text
    for secret in _SECRETS:
        if secret and len(secret) >= 8:
            out = out.replace(secret, "[REDACTED]")
    return out


def _redact_event(rec: dict[str, Any]) -> dict[str, Any]:
    out = dict(rec)
    for k, v in list(out.items()):
        if isinstance(v, str):
            out[k] = _redact_text(v)
        elif isinstance(v, dict):
            out[k] = _redact_event(v)
        elif isinstance(v, list):
            out[k] = [_redact_event(x) if isinstance(x, dict) else (_redact_text(x) if isinstance(x, str) else x) for x in v]
    return out


def _append_event(kind: str, **fields: Any) -> None:
    try:
        STATE_DIR.mkdir(parents=True, exist_ok=True)
        rec = _redact_event({"ts": time.time(), "kind": kind, **fields})
        with EVENTS_FILE.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
    except Exception:
        LOG.debug("event append failed", exc_info=True)
    if _db is not None:
        try:
            import uuid as _uuid
            # Correlation may arrive as a nested `corr` dict or as top-level kwargs.
            base = rec.get("corr") if isinstance(rec.get("corr"), dict) else {}
            run_id = str(base.get("run") or rec.get("run") or "")
            floor_id = str(base.get("floor") or rec.get("floor") or "")
            attempt_raw = base.get("attempt") if base.get("attempt") is not None else rec.get("attempt")
            worker_id = str(base.get("worker") or rec.get("worker") or "")
            agent_role = str(base.get("role") or rec.get("role") or "")
            try:
                attempt_id = int(attempt_raw or 0)
            except (TypeError, ValueError):
                attempt_id = 0
            _skip = {"ts", "kind", "corr", "run", "floor", "attempt", "worker", "role", "mode"}
            payload = {k: v for k, v in rec.items() if k not in _skip}
            _db.append_event(
                str(_uuid.uuid4()), kind, rec["ts"],
                run_id=run_id, floor_id=floor_id, attempt_id=attempt_id,
                worker_id=worker_id, agent_role=agent_role, payload=payload,
            )
        except Exception:
            LOG.debug("sqlite event mirror failed", exc_info=True)


# --- Correlation (run / floor / attempt / worker / role) ---------------------
# Threaded through from CrewAI worker calls via X-DL-* headers so evidence and
# shadow state are attributable to a concrete execution step, not just a wall-clock
# call sequence.
DL_HEADERS = {
    "run": "X-DL-Run-ID",
    "floor": "X-DL-Floor-ID",
    "attempt": "X-DL-Attempt",
    "worker": "X-DL-Worker-ID",
    "role": "X-DL-Agent-Role",
}


def _correlation(headers) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for key, hdr in DL_HEADERS.items():
        val = headers.get(hdr)
        if isinstance(val, str) and val.strip():
            out[key] = val.strip()
    mode = headers.get("X-Dual-Lobe-Mode")
    if isinstance(mode, str):
        out["mode"] = mode.strip().lower()
    return out


def _is_bypass(corr: dict[str, Any]) -> bool:
    """Analyst/Auditor/Lobe-B calls skip A-side injection and B shadow recursion."""
    if corr.get("mode") == "bypass":
        return True
    return corr.get("role", "").lower() in {"analyst", "auditor", "lobe-b", "b"}


# --- Empty/500 retries so transient provider blips don't surface to CrewAI --
A_RETRIES = int(os.getenv("DUAL_LOBE_A_RETRIES", "3"))
B_RETRIES = int(os.getenv("DUAL_LOBE_B_RETRIES", "3"))

# Cooldown between Lobe B invocation bursts. With 40-iteration workers the
# pre+post shadow passes would otherwise fire on EVERY worker call and blow the
# credit budget; gating them keeps the design while capping B spend.
# B is triggered deterministically per correlation context instead of by a global
# time cooldown: a bootstrap pass on the first Worker call of a (run, floor,
# attempt), then a pulse every N calls within that context. Ad-hoc calls (no
# correlation) fall back to the default context and the same cycle.
PULSE_EVERY = max(1, int(os.getenv("DUAL_LOBE_PULSE_EVERY", "3")))

_ctx_lock = threading.Lock()
_ctxs: dict[str, dict[str, Any]] = {}


def _ctx_key(corr: dict[str, Any]) -> str:
    run = str(corr.get("run") or "")
    floor = str(corr.get("floor") or "")
    attempt = corr.get("attempt")
    if not run and not floor:
        return "default"
    return f"{run or '-'}/{floor or '-'}/{int(attempt or 1)}"


def _ctx_state(corr: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    key = _ctx_key(corr)
    with _ctx_lock:
        ctx = _ctxs.get(key)
        if ctx is None:
            ctx = {"call_index": 0, "bootstrapped": False}
            _ctxs[key] = ctx
        ctx["call_index"] += 1
        return key, ctx


def _b_ready(corr: dict[str, Any]) -> bool:
    """Return True when B should run for this call: first call in a context
    (floor bootstrap) is always allowed; afterwards, pulse every PULSE_EVERY
    calls. No global cooldown."""
    key, ctx = _ctx_state(corr)
    call_index = ctx["call_index"]
    if not ctx["bootstrapped"]:
        ctx["bootstrapped"] = True
        return True
    return (call_index - 1) % PULSE_EVERY == 0


class _NoopFuture:
    def result(self, timeout=None):
        return {}
    def exception(self, timeout=None):
        return None


def _noop_future():
    return _NoopFuture()


def _is_retryable_error(e: Exception) -> bool:
    msg = str(e).lower()
    if isinstance(e, KeyboardInterrupt):
        return False
    return any(k in msg for k in ("empty", "429", "500", "502", "503", "504", "timeout", "rate limit", "insufficient credits", "connection", "service unavailable"))


def _call_with_retry(fn, retries: int, what: str, **kwargs: Any):
    last = None
    for attempt in range(retries):
        try:
            out = fn(**kwargs)
            if isinstance(out, str) and not out.strip():
                raise RuntimeError("empty model response")
            return out
        except Exception as e:
            last = e
            LOG.warning("%s attempt %d/%d failed: %s", what, attempt + 1, retries, e)
            if not _is_retryable_error(e):
                break
            delay = min(4.0, 0.75 * (attempt + 1))
            # Honor upstream rate-limit hints (Gemini retryDelay ≈ 30s+; litellm
            # surfaces "Please retry in Ns" text). Caps keep total stall bounded.
            m = re.search(r"retry in (~?)(\d+)(?:\.?\d*)?s", str(e))
            if m:
                suggested = float(m.group(2))
                if m.group(1) == "~":
                    suggested *= 1.5
                delay = max(delay, min(90.0, suggested))
            time.sleep(delay)
    raise last


def _uniq(items: Any) -> list[str]:
    if not isinstance(items, list):
        return []
    out, seen = [], set()
    for x in items:
        if not isinstance(x, str):
            continue
        x = re.sub(r"\s+", " ", x).strip()
        if x and x not in seen:
            seen.add(x); out.append(x)
    return out


def _dedupe_dicts(items: Any, key: str) -> list[dict[str, str]]:
    if not isinstance(items, list):
        return []
    out, seen = [], set()
    for x in items:
        if not isinstance(x, dict):
            continue
        marker = str(x.get(key, "")).strip()
        if marker and marker not in seen:
            seen.add(marker); out.append({str(k): str(v) for k, v in x.items()})
    return out


def _max_severity(a: str, b: str) -> str:
    levels = {"LOW": 0, "MEDIUM": 1, "HIGH": 2, "CRITICAL": 3}
    a, b = a.upper(), b.upper()
    return a if levels.get(a, 0) >= levels.get(b, 0) else b


def _messages_text(messages: list[dict[str, Any]]) -> str:
    parts = []
    for m in messages:
        content = m.get("content", "")
        if isinstance(content, list):
            content = "\n".join(str(x.get("text", x)) if isinstance(x, dict) else str(x) for x in content)
        parts.append(f"[{m.get('role','unknown')}] {content}")
    return "\n\n".join(parts)[-MAX_SHADOW_INPUT_CHARS:]


def _parse_json_response(text: str) -> dict[str, Any]:
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


def _call_b(prompt: str) -> dict[str, Any]:
    try:
        def _go():
            r = _call_with_retry(
                lambda: B_ADAPTER.complete(
                    _providers.NormalizedRequest(
                        messages=[{"role": "system", "content": B_SYSTEM}, {"role": "user", "content": prompt}],
                        temperature=0.2,
                    )
                ),
                B_RETRIES, f"Lobe B ({B_MODEL})",
            )
            return _parse_json_response(r.choices[0].message.content or "")
        return _go()
    except Exception as e:
        # Fail-open: a shadow failure must never take down the Worker path. We log
        # and record an observable degradation event so the missing shadow pass is
        # auditable rather than silently absent.
        LOG.exception("Lobe B model call failed")
        if _db is not None:
            try:
                _append_event("shadow_degraded", error="Lobe B model call failed", kind_ref="b_call")
            except Exception:
                pass
        if not B_FAIL_OPEN:
            # Hard-stop mode: the shadow chain genuinely halts (no silent {} pass-
            # through, no state merge, no oversight emission). The worker path stays
            # unaffected because B runs asynchronously by design.
            LOG.error("B_FAIL_OPEN=0: shadow failure treated as a hard stop")
            raise
        return {}


def _optional_search(queries: list[str]) -> list[dict[str, Any]]:
    if not SEARCH_ENABLED or not queries or not os.getenv("FIRECRAWL_API_KEY"):
        return []
    try:
        from firecrawl import FirecrawlApp
        app = FirecrawlApp(api_key=os.environ["FIRECRAWL_API_KEY"])
    except Exception:
        LOG.exception("Firecrawl unavailable for shadow research")
        return []
    results = []
    for q in queries[:MAX_SEARCH_QUERIES]:
        try:
            raw = app.search(q, limit=5)
            # Keep only compact serializable evidence.
            results.append({"query": q, "result": str(raw)[:9000]})
        except Exception as e:
            results.append({"query": q, "error": f"{type(e).__name__}: {e}"})
    return results


def _pre_shadow(context: str, key: str | None = None) -> dict[str, Any]:
    data = _call_b(PRE_PROMPT + context)
    if data:
        STORE.merge_pre(data, key)
    data["search_evidence"] = _optional_search(_uniq(data.get("search_queries", [])))
    return data


def _post_shadow(context: str, output: str, pre_future, corr: dict[str, Any] | None = None, key: str | None = None) -> None:
    try:
        pre = pre_future.result(timeout=float(os.getenv("DUAL_LOBE_PRE_TIMEOUT", "120")))
    except Exception:
        pre = {}
    snapshot = STORE.snapshot(key)
    prompt = (
        POST_PROMPT
        + "\n\nACTIVE CONTEXT:\n" + context[-MAX_SHADOW_INPUT_CHARS:]
        + "\n\nACTIVE OUTPUT:\n" + output[-MAX_SHADOW_INPUT_CHARS:]
        + "\n\nPRE-PASS FINDINGS/SEARCH EVIDENCE:\n" + json.dumps(pre, ensure_ascii=False)[-MAX_SHADOW_INPUT_CHARS:]
        + "\n\nCURRENT SHARED EVIDENCE STATE:\n" + json.dumps(asdict(snapshot), ensure_ascii=False)[-MAX_SHADOW_INPUT_CHARS:]
    )
    data = _call_b(prompt)
    if data:
        STORE.merge_post(data, key)
        snap = STORE.snapshot(key)
        LOG.info("Shadow state revision %s, severity=%s (ctx=%s)", snap.revision, snap.severity, key or "default")
        # Active Oversight pulse: record each challenge in the append-only ledger
        # (auditable, read by analysts/auditor). B never writes verdicts — these
        # are requests, not determinations; only the verifier settles claims.
        oversight = data.get("oversight") or {}
        challenges = oversight.get("challenges") or []
        if challenges:
            _append_event(
                "oversight",
                pulse=str(oversight.get("pulse") or "post"),
                challenges=challenges,
                **corr,
            )
        if _db is not None:
            try:
                corr = corr or {}
                db = _persist.get_db()
                run_id = str(corr.get("run") or "")
                if run_id:
                    db.upsert_run(run_id, floor_id=str(corr.get("floor") or ""),
                                  attempt=int(corr.get("attempt") or 1 or 1))
                db.save_state_version(
                    "shadow", asdict(snap),
                    run_id=run_id,
                    floor_id=str(corr.get("floor") or ""),
                    attempt_id=int(corr.get("attempt") or 0 or 0),
                )
            except Exception:
                LOG.debug("sqlite shadow version write failed", exc_info=True)


def _shadow_context_projection(corr: dict[str, Any]) -> str | None:
    """Compact, attributable evidence projection for Analyst/Auditor calls.

    Reviewers bypass the silent Worker injection but must read the ledger
    independently. This block is explicitly tagged so reviewers can weigh it
    without confusing it for the Worker's own context. If the evidence store is
    down, we say so and forbid a clean PASS (fail-closed for a verdict that
    depends on evidence we can no longer persist).
    """
    key = _ctx_key(corr)
    if _db is not None:
        db = _persist.get_db()
        try:
            run = str(corr.get("run") or "")
            ledger = db.events(run_id=run, limit=40)
            claims = db.claims(run_id=run, status="", limit=40) if run else []
        except Exception:
            # A failed read is an unavailable ledger, not an empty one. Emit the
            # true failure signal and fail closed (no "reachable" claim).
            LOG.warning("shadow ledger read failed; projecting unavailable", exc_info=True)
            ledger = None
            claims = []
        if ledger is None:
            _append_event("ledger_unavailable", run=str(corr.get("run") or ""),
                          floor=str(corr.get("floor") or ""),
                          attempt=corr.get("attempt") if corr.get("attempt") is not None else 1,
                          role=str(corr.get("role") or "reviewer"))
            return ("<shadow_context>\n"
                    "Evidence ledger is CURRENTLY UNREACHABLE. No clean PASS may be issued until "
                    "evidence persistence is restored and the material claims are recorded.\n"
                    "</shadow_context>")
        pend = sum(1 for c in claims if c.get("status") in ("claimed", "UNVERIFIED"))
        lines = ["<shadow_context>", "Durable evidence ledger (append-only):"]
        for e in reversed(ledger[-10:]):
            if e.get("kind") == "worker_call":
                lines.append(f"- worker call ({e.get('floor_id') or '?'})")
            elif e.get("kind") == "oversight":
                payload = e.get("payload") or {}
                for c in (payload.get("challenges") or [])[:3]:
                    lines.append(f"- oversight {payload.get('pulse') or 'post'}: verify {c.get('claim')}")
        lines.append(f"{pend} unsettled/unverified claim(s); ledger reachable")
        if pend:
            lines.append("Do not treat unresolved claims as settled. A clean PASS requires claims "
                         "material to the verdict to be VERIFIED or explicitly waived in the verdict.")
        lines.append("</shadow_context>")
        _append_event("ledger_read", run=run, floor=str(corr.get("floor") or ""),
                      attempt=corr.get("attempt") if corr.get("attempt") is not None else 1,
                      role=str(corr.get("role") or "reviewer"))
        return "\n".join(lines)
    _append_event("ledger_unavailable", run=str(corr.get("run") or ""),
                  floor=str(corr.get("floor") or ""),
                  attempt=corr.get("attempt") if corr.get("attempt") is not None else 1,
                  role=str(corr.get("role") or "reviewer"))
    return ("<shadow_context>\n"
            "Evidence ledger is CURRENTLY UNREACHABLE. No clean PASS may be issued until "
            "evidence persistence is restored and the material claims are recorded.\n"
            "</shadow_context>")


def _injection_message(key: str | None = None) -> str | None:
    s = STORE.snapshot(key)
    parts: list[str] = []
    if s.injection:
        parts += s.injection
    if s.required_preconditions:
        parts += [f"Required precondition: {x}" for x in s.required_preconditions]
    if s.state_corrections:
        parts += [f"Current effective state: {x}" for x in s.state_corrections]
    if s.blocker_insights:
        parts += [f"Relevant blocker context: {x}" for x in s.blocker_insights[:6]]
    if s.unasked_questions:
        parts += [f"Unresolved question: {x}" for x in s.unasked_questions[:5]]
    # Active Oversight: HIGH/CRITICAL challenges surface as verification
    # requirements (attribution-free) rather than verdicts.
    if s.severity in {"HIGH", "CRITICAL"}:
        for c in s.oversight_challenges[:5]:
            action = str(c.get("action") or "verify")
            if action == "stop":
                parts.append(f"Hold before treating the following as settled: {c.get('claim')}")
            elif action == "hold":
                parts.append(f"Do not rely on the following until confirmed: {c.get('claim')}")
            else:
                parts.append(f"Verify before relying on this: {c.get('claim')}")
    if not parts:
        return None
    body = "\n".join(f"- {x}" for x in _uniq(parts))
    body = body[:MAX_INJECTION_CHARS]
    # Deliberately no provenance/attribution language.
    return "Additional working context and execution constraints:\n" + body


def _call_a(payload: dict[str, Any], messages: list[dict[str, Any]]):
    # Normalize the Worker-facing request and route upstream via the configured
    # lobe adapter. The openai/ prefix + explicit api_base/api_key is handled
    # inside the adapter, keeping provider specifics out of the gateway body.
    req = _providers.resolve_request(payload)
    req.messages = messages
    return A_ADAPTER.complete(req)


def _to_openai_response(resp, public_model: str) -> dict[str, Any]:
    if hasattr(resp, "model_dump"):
        data = resp.model_dump()
    elif isinstance(resp, dict):
        data = dict(resp)
    else:
        data = json.loads(resp.json())
    data["model"] = public_model
    return data


class Handler(BaseHTTPRequestHandler):
    server_version = "DualLobeGateway/1.0"

    def _authorized(self) -> bool:
        """Optional Bearer allowlist. OFF for loopback; when GATEWAY_AUTH is set,
        only exact gateway keys are accepted (upstream provider keys never are)."""
        if not GATEWAY_AUTH:
            return True
        raw = self.headers.get("Authorization") or ""
        token = raw[len("Bearer "):].strip() if raw.lower().startswith("bearer ") else ""
        return token in GATEWAY_ALLOWED_KEYS

    def _read_body(self) -> bytes:
        length = int(self.headers.get("Content-Length", "0"))
        if length > MAX_REQUEST_BYTES:
            raise ValueError("request body too large")
        return self.rfile.read(length)

    def _send(self, status: int, data: dict[str, Any]) -> None:
        raw = json.dumps(data, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def _send_sse(self, data: dict[str, Any]) -> None:
        """Stream a buffered OpenAI-compatible response as SSE chunks.

        The Worker-facing contract supports ``stream: true``; the upstream leg is
        gateway-buffered, so we emit one message delta (content + tool_calls) plus
        a terminal finish chunk. If the stream is interrupted partway (e.g. a
        truncated or failed response), the terminal chunk reports INCOMPLETE.
        """
        finish = "stop"
        try:
            message = data["choices"][0]["message"]
            finish = data["choices"][0].get("finish_reason") or "stop"
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Cache-Control", "no-cache")
            self.send_header("Connection", "close")
            self.end_headers()
            w = self.wfile
            chunk = {
                "id": data.get("id") or "dual-lobe-a-stream",
                "object": "chat.completion.chunk",
                "model": data.get("model") or "dual-lobe-a",
                "choices": [{"index": 0, "delta": {
                    "role": message.get("role") or "assistant",
                    "content": message.get("content") or "",
                    **({"tool_calls": message["tool_calls"]} if message.get("tool_calls") else {}),
                }, "finish_reason": None}],
            }
            w.write(f"data: {json.dumps(chunk, ensure_ascii=False)}\n\n".encode("utf-8"))
            w.flush()
            if data.get("choices") or data.get("usage"):
                last = {
                    "id": chunk["id"], "object": "chat.completion.chunk", "model": chunk["model"],
                    "choices": [{"index": 0, "delta": {}, "finish_reason": finish}],
                    **({"usage": data["usage"]} if data.get("usage") else {}),
                }
                w.write(f"data: {json.dumps(last, ensure_ascii=False)}\n\n".encode("utf-8"))
            w.write(b"data: [DONE]\n\n")
            w.flush()
        except Exception:
            LOG.exception("stream interrupted")
            try:
                self.wfile.write(
                    f"data: {json.dumps({'id': data.get('id') or 'stream', 'object': 'chat.completion.chunk', 'model': (data.get('model') or 'dual-lobe-a'), 'choices': [{'index': 0, 'delta': {}, 'finish_reason': 'INCOMPLETE'}]}, ensure_ascii=False)}\n\ndata: [DONE]\n\n".encode("utf-8")
                )
                self.wfile.flush()
            except Exception:
                pass

    def _handle_verify(self) -> None:
        """Verifier RPC: settle a specific claim against an artifact. Verifier-only."""
        try:
            payload = json.loads(self._read_body() or b"{}")
            claim_id = str(payload.get("claim_id") or "")
            check = str(payload.get("check") or "file_exists")
            artifact = str(payload.get("artifact") or "")
            needle = str(payload.get("needle") or "")
            if not claim_id or not artifact:
                self._send(400, {"status": "error", "error": "claim_id and artifact required"})
                return
            run_id = str(self.headers.get("X-DL-Run-ID") or "")
            event_id = str(self.headers.get("X-DL-Event-ID") or "")
            db = _persist.get_db() if _db is not None else None
            if db is None:
                self._send(503, {"status": "unavailable", "ledger": "down"})
                return
            verdict = _verifier.verify_single_claim(
                db, claim_id, check, artifact, needle=needle,
                cwd=os.getenv("DUAL_LOBE_VERIFY_CWD", "."),
                run_id=run_id, event_id=event_id,
            )
            _append_event("claim_verified", claim_id=claim_id, verdict=verdict,
                          check=check, artifact=artifact, run=run_id, role=self.headers.get("X-DL-Agent-Role") or "verifier")
            self._send(200, {"status": "ok", "claim_id": claim_id, "verdict": verdict})
        except Exception as e:
            LOG.exception("verify RPC failed")
            self._send(500, {"status": "error", "error": f"{type(e).__name__}: {e}"})

    def do_GET(self) -> None:
        path = self.path.rstrip("/")
        if path in {"", "/health", "/v1/health"}:
            s = STORE.snapshot()
            self._send(200, {"status": "ok", "a_model": A_MODEL, "b_model": B_MODEL, "shadow_revision": s.revision, "severity": s.severity})
        elif path in {"/healthz", "/readyz"}:
            # Liveness is the plain 200 above; readiness additionally requires the
            # durable append-only ledger to be writable (the dual-lobe runtime must
            # not report ready if evidence can no longer be persisted).
            db_ok = _db is not None
            if db_ok:
                try:
                    _persist.get_db().health_ping()
                except Exception:
                    LOG.exception("readyz: persistence ledger unreachable")
                    db_ok = False
            status = 200 if db_ok else 503
            self._send(status, {"status": "ok" if db_ok else "unavailable", "ledger": "ok" if db_ok else "down"})
        elif path == "/v1/models":
            self._send(200, {"object": "list", "data": [{"id": "dual-lobe-a", "object": "model", "owned_by": "local"}]})
        elif path == "/v1/ledger":
            db = _persist.get_db() if _db is not None else None
            if db is None:
                self._send(503, {"status": "unavailable", "ledger": "down"})
                return
            try:
                counts = {
                    "events": db.count("events"),
                    "claims": db.count("claims"),
                    "evidence": db.count("evidence"),
                    "state_versions": db.count("state_versions"),
                }
                self._send(200, {"status": "ok", "counts": counts})
            except Exception:
                LOG.exception("ledger read failed")
                self._send(500, {"status": "error"})
        elif path == "/v1/oversight":
            db = _persist.get_db() if _db is not None else None
            if db is None:
                self._send(503, {"status": "unavailable", "ledger": "down"})
                return
            try:
                rows = db.events("", "", limit=50)
                pulses = [e for e in rows if e.get("kind") == "oversight"]
                self._send(200, {"status": "ok", "pulses": pulses[-20:]})
            except Exception:
                LOG.exception("oversight read failed")
                self._send(500, {"status": "error"})
        elif path == "/v1/claims":
            db = _persist.get_db() if _db is not None else None
            if db is None:
                self._send(503, {"status": "unavailable", "ledger": "down"})
                return
            try:
                run_id = self.headers.get("X-DL-Run-ID") or ""
                status = self.headers.get("X-DL-Claim-Status") or ""
                claims = db.claims(run_id=run_id, status=status, limit=200)
                self._send(200, {"status": "ok", "claims": claims})
            except Exception:
                LOG.exception("claims read failed")
                self._send(500, {"status": "error"})
        else:
            self._send(404, {"error": {"message": "Not found", "type": "not_found_error"}})

    def do_POST(self) -> None:
        path = self.path.rstrip("/")
        if not self._authorized():
            self._send(401, {"error": {"message": "Unauthorized", "type": "unauthorized_error"}})
            return
        if path == "/v1/verify":
            self._handle_verify()
            return
        if path != "/v1/chat/completions":
            self._send(404, {"error": {"message": "Not found", "type": "not_found_error"}}); return
        try:
            payload = json.loads(self._read_body() or b"{}")
            messages = payload.get("messages") or []
            if not isinstance(messages, list):
                raise ValueError("messages must be a list")

            corr = _correlation(self.headers)
            bypass = _is_bypass(corr)
            key = _ctx_key(corr)

            # Analysts/Auditor bypass the silent Worker injection but receive a
            # compact <shadow_context> projection of the durable ledger so they
            # can verify independently (and cannot clean-PASS on a dead ledger).
            injection = _shadow_context_projection(corr) if bypass else _injection_message(key)
            effective_messages = list(messages)
            if injection:
                # Insert before the final user turn, preserving the user's last-turn priority.
                idx = max(0, len(effective_messages) - 1)
                effective_messages.insert(idx, {"role": "system", "content": injection})

            context = _messages_text(effective_messages)
            run_b = (not bypass) and _b_ready(corr)
            pre_future = POOL.submit(_pre_shadow, context, key) if run_b else _noop_future()

            def _call_a_retry():
                # sample a short topic hint from the last user turn for the beacon
                return _call_a(payload, effective_messages)

            try:
                resp = _call_with_retry(_call_a_retry, A_RETRIES, f"Lobe A ({A_MODEL})")
            except Exception as e:
                _append_event("worker_error", error=f"{type(e).__name__}: {e}", **corr)
                raise

            data = _to_openai_response(resp, payload.get("model", "dual-lobe-a"))
            output = ""
            try:
                output = data["choices"][0]["message"].get("content") or ""
            except Exception:
                pass
            if output is None:
                output = ""
            topic_hint = ""
            try:
                last_user = next((m for m in reversed(messages) if m.get("role") == "user"), {})
                topic_hint = (last_user.get("content") or "")[:200]
            except Exception:
                pass
            if not bypass:
                st = STORE.snapshot(key)
                _append_event(
                    "worker_call",
                    call=st.revision + 1,
                    topic_hint=topic_hint,
                    b_severity=st.severity,
                    n_findings=len(st.evidence_ledger),
                    n_injection=len(st.injection),
                    n_blockers=len(st.blocker_insights),
                    error="",
                    **corr,
                )
                # Durable run correlation for the append-only ledger.
                if _db is not None and corr.get("run"):
                    try:
                        _persist.get_db().upsert_run(
                            str(corr["run"]), original_goal=topic_hint,
                            floor_id=str(corr.get("floor") or ""),
                            attempt=int(corr.get("attempt") or 1 or 1),
                        )
                    except Exception:
                        LOG.debug("sqlite run upsert failed", exc_info=True)
                if run_b:
                    POOL.submit(_post_shadow, context, output, pre_future, corr, key)  # reality/blocker pass does not delay A
            if payload.get("stream"):
                self._send_sse(data)
            else:
                self._send(200, data)
        except Exception as e:
            LOG.exception("Gateway request failed")
            self._send(500, {"error": {"message": f"{type(e).__name__}: {e}", "type": "dual_lobe_gateway_error"}})

    def log_message(self, fmt: str, *args: Any) -> None:
        LOG.debug("%s - %s", self.address_string(), fmt % args)


def main() -> None:
    LOG.info("Starting dual-lobe gateway on http://%s:%s/v1", HOST, PORT)
    LOG.info("Lobe A upstream: %s | Lobe B: %s", A_MODEL, B_MODEL)
    server = ThreadingHTTPServer((HOST, PORT), Handler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close(); POOL.shutdown(wait=False, cancel_futures=True)

if __name__ == "__main__":
    main()
