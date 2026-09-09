"""Runtime narrator: proactive, human-voiced narration of pipeline state.

Distinct from the operator agent. The narrator *speaks on its own* when the
pipeline does something meaningful, in the voice of a human colleague narrating
their work — never a robot. It reads the same live cortex snapshot() the model
and beacon see, so it can never outrun reality.

Delta detection (only emit on meaningful change) + a person-paced cooldown so
it talks like a human, not a telemetry spitter.
"""
from __future__ import annotations

import os
import threading
import time
import urllib.request
import urllib.parse
import json
from typing import Any

from deep_investigator_build_pipeline import runtime_cortex as cortex

MODEL = os.getenv("RUNTIME_NARRATOR_MODEL", "zai-org/GLM-5.3-Flash")
API_BASE = os.getenv("RUNTIME_NARRATOR_BASE", os.getenv("OPENAI_API_BASE", "https://api.featherless.ai/v1"))
API_KEY = os.getenv("RUNTIME_NARRATOR_KEY", os.getenv("OPENAI_API_KEY", ""))
MIN_INTERVAL = float(os.getenv("RUNTIME_NARRATOR_INTERVAL", "5.0"))
MAX_PER_MIN = int(os.getenv("RUNTIME_NARRATOR_MAX_PER_MIN", "3"))

SYSTEM = (
    "You are the live narrator of an autonomous software research-and-build "
    "pipeline. You speak aloud to the human operator as work progresses, like a "
    "seasoned engineer narrating what is happening on their bench. Be warm, "
    "concrete, and honest. Use contractions and active voice. Your job is to "
    "translate ONE recent event into ONE short spoken line (under 40 words) that "
    "a person would actually say aloud while watching the work.\n"
    "NEVER use robot phrasing such as 'processing', 'executing task', "
    "'initializing module', 'the system is now'. Do NOT mention 'Lobe', "
    "'the model', 'the agent', or 'snapshot'. Refer to the work naturally "
    "('it is now...', 'we just...'). If the event is a blocker or a credit "
    "failure, say so plainly with mild concern, not alarm. If it is progress, "
    "say so with quiet satisfaction.\n"
    "Output ONLY the spoken line. No prefix, no quotes, no explanation."
)


def _llm(prompt: str) -> str:
    if not API_KEY:
        return ""
    body = json.dumps({
        "model": MODEL,
        "messages": [{"role": "system", "content": SYSTEM},
                     {"role": "user", "content": prompt}],
        "max_tokens": 110,
        "temperature": 0.9,
    }).encode("utf-8")
    try:
        req = urllib.request.Request(
            f"{API_BASE}/chat/completions", data=body,
            headers={"Content-Type": "application/json", "Authorization": f"Bearer {API_KEY}"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=30) as r:
            data = json.loads(r.read().decode("utf-8"))
        return (data["choices"][0]["message"]["content"] or "").strip()
    except Exception:
        return ""


class Narrator:
    def __init__(self, sink) -> None:
        """sink(line: str) -> None is called with each narration line."""
        self.sink = sink
        self._last: dict[str, Any] = {}
        self._last_time = 0.0
        self._this_min = 0
        self._min_reset = time.time()
        self._lock = threading.Lock()
        self._emit_lock = threading.Lock()

    def _guardian_line(self, cur: dict[str, Any], prev: dict[str, Any]) -> str | None:
        """Templated critical lines (structure, never free-form prose).

        The guardian has priority over the narrator: blockers, credit failures,
        contradictions, and gate blocks are spoken directly from the structured
        fields, so they are always accurate and never softened by generation.
        """
        # Credit failure — the run is physically stalled.
        if cur.get("credit_blocked") and not prev.get("credit_blocked"):
            return "Heads up. The run is out of credits and stalled until they get sorted."
        # A new blocker appeared on the desk.
        pb = prev.get("n_blockers", 0)
        cb = cur.get("n_blockers", 0)
        if cb > pb:
            sev = (cur.get("severity") or "LOW")
            note = " — that pressure is high" if sev in ("HIGH", "CRITICAL") else ""
            return f"There is a fresh blocker, {cb} open now{note}. I would not move past this until it clears."
        # A contradiction just entered the evidence ledger.
        pc = prev.get("integrity_conflict", 0.0)
        cc = cur.get("integrity_conflict", 0.0)
        if cc > pc and cc > 0:
            pct = int(round(cc * 100))
            return (f"There is a contradiction in the evidence — {pct} percent of it is "
                    "contradicted, so that part stays flagged, not verified.")
        # Completion gate closed (contradiction or credit) for the first time.
        if cur.get("completion_blocked") and not cur.get("credit_blocked") \
                and not prev.get("completion_blocked"):
            return "The completion gate is blocked. It cannot pass until the contradiction is resolved."
        return None

    def _describe_event(self, cur: dict[str, Any], prev: dict[str, Any]) -> str | None:
        """Return an event description if a meaningful delta occurred."""
        # Credit blocker
        if cur.get("credit_blocked") and not prev.get("credit_blocked"):
            return "the run has run out of credits and cannot proceed"
        # Worker call count increased
        if cur.get("worker_calls", 0) > prev.get("worker_calls", 0):
            n = cur.get("worker_calls", 0)
            return f"work just advanced; that is worker call number {n}"
        # Severity escalated
        cur_sev, prev_sev = cur.get("severity"), prev.get("severity")
        sev_rank = {"LOW": 0, "MEDIUM": 1, "HIGH": 2, "CRITICAL": 3}
        if sev_rank.get(cur_sev, 0) > sev_rank.get(prev_sev, 0):
            return f"something came up and the pressure just went {cur_sev}"
        # New findings
        cur_f, prev_f = cur.get("n_findings", 0), prev.get("n_findings", 0)
        if cur_f > prev_f:
            return f"a new finding just landed; {cur_f} confirmed now"
        # Phase transition
        if cur.get("phase") != prev.get("phase") and cur.get("phase"):
            return f"it has moved into the {cur['phase']} phase"
        # Floor change
        if cur.get("floor") != prev.get("floor") and cur.get("floor") is not None:
            return f"now working on floor {cur['floor']}"
        # Blocker appeared
        if cur.get("n_blockers", 0) > prev.get("n_blockers", 0):
            return f"there is a fresh blocker on the desk; {cur.get('n_blockers')} open now"
        # Final verdict
        if cur.get("final") and cur.get("final") != prev.get("final"):
            return f"it has come back with a final {cur['final']}"
        return None

    def _budget_ok(self) -> bool:
        now = time.time()
        with self._lock:
            if now - self._min_reset > 60:
                self._min_reset = now
                self._this_min = 0
            if now - self._last_time < MIN_INTERVAL:
                return False
            if self._this_min >= MAX_PER_MIN:
                return False
            return True

    def _tap(self) -> None:
        try:
            cur = cortex.snapshot(force=False)
            prev = self._last or cur
            # Guardian path: templated critical lines bypass the LLM entirely.
            guard = self._guardian_line(cur, prev)
            if guard:
                self._last = cur
                with self._lock:
                    if time.time() - self._last_time < MIN_INTERVAL:
                        return
                    self._last_time = time.time()
                    self._this_min += 1
                with self._emit_lock:
                    self.sink(guard)
                return
            event = self._describe_event(cur, prev)
            if not event:
                self._last = cur
                return
            if not self._budget_ok():
                self._last = cur
                return
            self._last = cur
            prompt = (
                f"Recent event: {event}\n"
                f"Live state: phase={cur.get('phase')}, floor={cur.get('floor')}, "
                f"{cur.get('worker_calls')} worker calls, severity={cur.get('severity')}, "
                f"{cur.get('n_findings')} findings, {cur.get('n_blockers')} blockers, "
                f"{cur.get('artifacts_count')} files on disk.\n"
                "Say one short spoken line a human engineer would say out loud right now."
            )
            with self._emit_lock:
                line = _llm(prompt)
                if line:
                    with self._lock:
                        self._last_time = time.time()
                        self._this_min += 1
                    self.sink(line)
        except Exception:
            pass

    def run(self, stop_event: threading.Event) -> None:
        while not stop_event.is_set():
            self._tap()
            stop_event.wait(1.5)
