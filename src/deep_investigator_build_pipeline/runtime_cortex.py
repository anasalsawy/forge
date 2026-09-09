"""Runtime cortex: shared live-state derivation for the beacon and voice layers.

The declarative flow schema does not expose a per-crew step callback, so the
cortex reconstructs pipeline state from three trustworthy, schema-legal sources:

1. Gateway ``.dual_lobe/events.jsonl``   - exact worker-call timeline + Lobe-B
   severity / findings / blockers / credit errors (the "doing" plane).
2. Gateway ``.dual_lobe/state.json``     - evidence ledger, severity, blockers.
3. Workspace artifact scan               - files touched since run start (the
   "what is actually made" plane, used to fill in the visual subject).
4. Run log tail                          - floor / phase boundaries parsed from
   the captured ``crewai run`` output (best-effort).

Everything is best-effort and fail-open: if a source is unavailable the cortex
returns the partial state and the beacon degrades to a procedural render.
"""
from __future__ import annotations

import json
import os
import re
import threading
import time
from pathlib import Path
from typing import Any

# Default locations (override via env).
STATE_DIR = Path(os.getenv("DUAL_LOBE_STATE_DIR", ".dual_lobe"))
ROOT_DIR = Path(os.getenv("RUNTIME_BEACON_ROOT", os.getcwd()))
RUN_LOG = Path(os.getenv("RUNTIME_RUN_LOG", "output/run.log"))

# Workspace paths to ignore when scanning for build artifacts.
_IGNORED_DIRS = {
    ".venv", ".git", ".dual_lobe", ".beacon", "__pycache__", ".crewai",
    ".mypy_cache", ".pytest_cache", "node_modules", ".idea", ".vscode",
    "output", "frames",
}
_IGNORED_SUFFIXES = {".pyc", ".pyo", ".log", ".jsonl"}

_lock = threading.Lock()
_cached: dict[str, Any] = {}
_CACHE_TTL = 3.0

# --- (M, E, K, F) truth-model constants -------------------------------------
# M = materialization, E = evidence confidence, K = conflict, F = freshness.
_STATUS_WEIGHT = {
    "VERIFIED": 1.0,
    "SUPPORTED": 0.75,
    "INFERRED": 0.45,
    "UNVERIFIED": 0.15,
    "CONTRADICTED": 0.0,
}
_CRITICAL_CAP_E = 0.25          # a critical contradiction hard-caps confidence.
_TRANSIENT_WORDS = (
    "respond", "serves", "reachable", "live site", "live lookup",
    "return", "latency", "uptime", "ping", "status endpoint",
)
_SUBJECT_STOP = {
    "the", "a", "an", "that", "this", "it", "its", "we", "our", "us",
    "them", "their", "and", "or", "of", "for", "to", "in", "with", "on",
    "all", "some", "most", "about", "as", "into", "at", "from",
}
_CLAIM_PREFIXES = {
    "verified", "supported", "inferred", "unverified", "contradicted",
    "status", "observation", "inference",
}
_N_VERIFY_TARGET = 8            # evidence-backed claims enough to call research "done"
_N_CALL_TARGET = 40             # worker-call budget proxy for research completion


def _clamp(v: float, lo: float = 0.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, v))


def _claim_subject(claim: str) -> str:
    """Heuristic: the leading noun-ish phrase of a claim becomes its entity key."""
    words = claim.split()
    picked: list[str] = []
    for w in words:
        wl = w.strip('",:;()[]!?').strip("'")
        if not wl:
            continue
        low = wl.lower()
        low_stripped = low.strip("-")
        if low_stripped in _SUBJECT_STOP or low in _CLAIM_PREFIXES:
            continue
        picked.append(wl)
        if len(picked) >= 3:
            break
    if not picked:
        # Fall back to the first 3 whole words, lowercased.
        picked = words[:3] or claim[:24].split()
    return " ".join(picked).lower()[:56] or "untitled"


def _entity_freshness(claims: list[dict[str, Any]], last_event_age: float) -> float:
    """F decays for time-sensitive (transient) evidence since the last event."""
    if not claims or last_event_age is None:
        return 1.0
    text = " ".join(str(c.get("claim", "")) for c in claims).lower()
    transient = any(tw in text for tw in _TRANSIENT_WORDS)
    if not transient:
        return 1.0
    return _clamp(1.0 - 0.02 * int(last_event_age / 60))


def derive_entities(claims: list[dict[str, Any]],
                    artifact_tops: list[str],
                    phase: str,
                    last_event_age: float,
                    artifacts_count: int = 0) -> list[dict[str, Any]]:
    """Reduce the Lobe-B evidence ledger + artifact scan into (M,E,K,F) entities.

    Claim-backed entities carry the evidence plane; during the build phase, an
    entity that also matches a real top-level workspace path is *materialized*
    (M raised) but still hazes/blurs according to its E and K — a file on disk
    that is contradicted or unverified reads as present-but-unsafe.
    """
    groups: dict[str, list[dict[str, Any]]] = {}
    seen: set[str] = set()

    def _normalize_top(top: str) -> str:
        t = top.rstrip("/").lower()
        return t if t in _SUBJECT_STOP else t

    for c in claims:
        if not isinstance(c, dict):
            continue
        text = str(c.get("claim", "") or "").strip()
        if not text:
            continue
        subj = _claim_subject(text)
        groups.setdefault(subj, []).append({
            "status": str(c.get("status", "UNVERIFIED")).upper(),
            "claim": text[:120],
            "reason": str(c.get("reason", "") or "")[:120],
        })

    entities: list[dict[str, Any]] = []
    for subj, cls in sorted(groups.items()):
        sts = [cl["status"] for cl in cls]
        weights = [_STATUS_WEIGHT.get(s, 0.0) for s in sts]
        e = sum(weights) / len(weights) if weights else 0.0
        contrad = any(s == "CONTRADICTED" for s in sts)
        if contrad:
            e = min(e, _CRITICAL_CAP_E)
        entities.append({
            "name": subj,
            "kind": "claim",
            "M": 0.0,                        # research outline / not built yet
            "E": round(e, 3),
            "K": 1.0 if contrad else 0.0,
            "F": round(_entity_freshness(cls, last_event_age), 3),
            "contradicted": contrad,
            "status": ",".join(sorted(set(sts))),
        })
        seen.add(subj)

    # Materialization plane: real artifact tops become entities, raised to M=1.
    for top in artifact_tops:
        norm = _normalize_top(top)
        existing = next((x for x in entities if x["name"] == norm), None)
        if existing is not None:
            existing["M"] = 1.0
            # A materialized thing with no supporting evidence is unverified.
            if existing["E"] == 0.0:
                existing["E"] = _STATUS_WEIGHT["UNVERIFIED"]
        else:
            entities.append({
                "name": norm, "kind": "artifact", "M": 1.0,
                "E": _STATUS_WEIGHT["UNVERIFIED"], "K": 0.0, "F": 1.0,
                "contradicted": False, "status": "UNVERIFIED",
            })
    return entities


def compute_metrics(out: dict[str, Any], claims: list[dict[str, Any]],
                    entities: list[dict[str, Any]]) -> dict[str, Any]:
    """Global truth metrics with hard caps (completion can never outrun truth)."""
    n = max(len(claims), 1)
    contrad = sum(1 for c in claims if isinstance(c, dict)
                  and str(c.get("status", "")).upper() == "CONTRADICTED")
    integrity_conflict = _clamp(contrad / n)

    # Evidence confidence = mean E over entity claims, capped by any contradiction.
    claim_entities = [e for e in entities if e.get("kind") == "claim"]
    ev_conf = (sum(e["E"] for e in claim_entities) / len(claim_entities)
               if claim_entities else 0.0)
    if contrad:
        ev_conf = min(ev_conf, _CRITICAL_CAP_E)

    verified = sum(1 for c in claims if isinstance(c, dict)
                   and str(c.get("status", "")).upper() in ("VERIFIED", "SUPPORTED"))
    calls = out.get("worker_calls", 0)
    research_completeness = _clamp(
        0.6 * min(1.0, verified / _N_VERIFY_TARGET) + 0.4 * min(1.0, calls / _N_CALL_TARGET))

    artifacts = out.get("artifacts_count", 0)
    impl = _clamp(min(1.0, artifacts / 20.0))
    if integrity_conflict > 0:
        impl = min(impl, 1.0 - integrity_conflict)

    # Critical contradictions that strike at completion itself block the gate.
    critical_contradicted = any(
        c for c in claims
        if isinstance(c, dict) and str(c.get("status", "")).upper() == "CONTRADICTED")
    completion_blocked = bool(critical_contradicted) or bool(out.get("credit_blocked"))

    return {
        "research_completeness": round(research_completeness, 3),
        "implementation_completeness": round(impl, 3),
        "evidence_confidence": round(ev_conf, 3),
        "integrity_conflict": round(integrity_conflict, 3),
        "completion_blocked": completion_blocked,
        "n_conflicts": len([e for e in entities if e.get("K", 0) > 0]),
    }


# --- Semantic diff engine ----------------------------------------------------

_REVISION_KEYS = ("revision", "phase", "floor", "final", "credit_blocked",
                  "evidence_confidence", "integrity_conflict", "n_blockers")


def make_revision(out: dict[str, Any]) -> dict[str, Any]:
    """Normalized, comparable digest of the current truth state."""
    entities = out.get("entities", [])
    return {
        "t": out.get("_t", 0.0),
        "revision": out.get("shadow_revision", 0),
        "phase": out.get("phase"),
        "floor": out.get("floor"),
        "final": out.get("final"),
        "credit_blocked": out.get("credit_blocked", False),
        "evidence_confidence": out.get("evidence_confidence"),
        "integrity_conflict": out.get("integrity_conflict"),
        "implementation_completeness": out.get("implementation_completeness"),
        "n_blockers": out.get("n_blockers"),
        "entities": [(e["name"], e["K"]) for e in entities],
        "artifact_tops": out.get("artifacts_tops", []),
    }


def diff_revisions(a: dict[str, Any] | None, b: dict[str, Any]) -> dict[str, Any]:
    """Semantic diff between two truth revisions (name/conflict-level)."""
    if not a:
        return {
            "delta": 1.0, "new": True,
            "added": [n for n, _ in b.get("entities", [])],
            "removed": [], "conflict_changed": [],
            "gate_events": [k for k in _REVISION_KEYS if b.get(k) is not None],
        }
    ea = dict(a.get("entities", []))
    eb = dict(b.get("entities", []))
    added = [n for n in eb if n not in ea]
    removed = [n for n in ea if n not in eb]
    conflict_changed = [n for n in eb if n in ea and ea[n] != eb[n]]
    gate_events = [k for k in _REVISION_KEYS if a.get(k) != b.get(k)]
    changed = len(set(added) | set(removed) | set(conflict_changed))
    # Delta score D_Δ in [0,1] used by the beacon to decide a generative frame.
    delta = _clamp(0.6 * min(1.0, changed / 4.0) + 0.4 * min(1.0, len(gate_events) / 3.0))
    return {
        "delta": round(delta, 3), "new": False,
        "added": added, "removed": removed,
        "conflict_changed": conflict_changed, "gate_events": gate_events,
    }


def _rev_digest(rev: dict[str, Any]) -> str:
    core = {k: v for k, v in rev.items() if k != "t"}
    return json.dumps(core, sort_keys=True, default=str)


def save_revision(rev_dir: Path, rev: dict[str, Any]) -> None:
    try:
        rev_dir.mkdir(parents=True, exist_ok=True)
        last = read_revisions(rev_dir, 1)
        if last and _rev_digest(last[0]) == _rev_digest(rev):
            return
        name = f"rev_{int(rev.get('revision', time.time() * 1000))}_{int(rev.get('t', 0.0))}.json"
        (rev_dir / name).write_text(json.dumps(rev, ensure_ascii=False), encoding="utf-8")
    except Exception:
        pass


def read_revisions(rev_dir: Path, limit: int = 20) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    try:
        if not rev_dir.exists():
            return out
        for fn in sorted(rev_dir.glob("rev_*.json"))[-limit:]:
            try:
                out.append(json.loads(fn.read_text(encoding="utf-8")))
            except Exception:
                continue
    except Exception:
        pass
    return out


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    out = []
    try:
        if not path.exists():
            return out
        with path.open("r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    out.append(json.loads(line))
                except Exception:
                    continue
    except Exception:
        pass
    return out


def _read_state(path: Path) -> dict[str, Any]:
    try:
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        pass
    return {}


def _scan_artifacts(since: float) -> list[dict[str, Any]]:
    """Return files under the workspace tree modified after ``since`` (best-effort)."""
    found = []
    start = time.time()
    for base in (ROOT_DIR,):
        if not base.exists():
            continue
        try:
            for root, dirs, files in os.walk(base):
                dirs[:] = [d for d in dirs if d not in _IGNORED_DIRS and not d.startswith(".")]
                for fn in files:
                    if fn.startswith(".") or fn.endswith(tuple(_IGNORED_SUFFIXES)):
                        continue
                    p = Path(root) / fn
                    try:
                        if p.stat().st_mtime >= since:
                            found.append(
                                {"path": str(p.relative_to(base)), "mtime": p.stat().st_mtime}
                            )
                    except Exception:
                        continue
                if time.time() - start > 2.0:  # safety cap on deep trees
                    break
        except Exception:
            continue
    found.sort(key=lambda x: x["mtime"])
    return found


# --- Run-log stage classifier ----------------------------------------------

_PHASE_RE = re.compile(r"(research|build)\s*_?\s*floor\s*_?\s*(\d+)", re.I)
_TASK_RE = re.compile(r"Starting (?:the )?task(?: `| name=)?[\"']?([a-z_0-9]+)", re.I)


def _parse_run_log() -> dict[str, Any]:
    """Best-effort phase/floor/final-stage extraction from the run log tail."""
    info: dict[str, Any] = {"phase": None, "floor": None, "final": None, "ran": False}
    try:
        if not RUN_LOG.exists():
            return info
        # Read the last 2000 lines to keep it cheap.
        with RUN_LOG.open("r", encoding="utf-8", errors="ignore") as fh:
            lines = fh.readlines()[-2000:]
        info["ran"] = True
        built = 0
        for ln in lines:
            low = ln.lower()
            m = _PHASE_RE.search(low)
            if m:
                info["phase"] = m.group(1).lower()
                try:
                    info["floor"] = int(m.group(2))
                except Exception:
                    pass
            if "final status: pass" in low:
                info["final"] = "PASS"
            elif "final status: fail" in low:
                info["final"] = "FAIL"
            elif "correction required" in low and built:
                pass
    except Exception:
        pass
    return info


def snapshot(force: bool = False) -> dict[str, Any]:
    """Compute the current live pipeline snapshot (thread-safe, ttl-cached)."""
    global _cached
    now = time.time()
    with _lock:
        if not force and _cached and (now - _cached.get("_t", 0)) < _CACHE_TTL:
            return _cached

    # Paths resolve against the *current* STATE_DIR so callers that reassign it
    # after import (beacon / voice / rt_mirror) read the right location.
    events = _read_jsonl(STATE_DIR / "events.jsonl")
    state = _read_state(STATE_DIR / "state.json")
    log = _parse_run_log()

    worker_calls = [e for e in events if e.get("kind") == "worker_call"]
    errors = [e for e in events if e.get("kind") == "worker_error"]

    # Credit / hard errors currently affecting the run.
    recent_errors = [e.get("error", "") for e in errors]
    credit_blocked = any("insufficient credits" in (e.get("error") or "").lower()
                         or "insufficient credits" in str(e.get("error", "")).lower()
                         for e in errors)

    started = min((e.get("ts", now) for e in events), default=now)

    # Artifact fill: files touched since the run started.
    artifacts = _scan_artifacts(since=started - 1)

    # Lobe-B severity & findings.
    severity = str(state.get("severity", "LOW")).upper()
    findings = state.get("evidence_ledger", []) or []
    blockers = state.get("blocker_insights", []) or []
    injections = state.get("injection", []) or []
    n_calls = len(worker_calls)
    revision = int(state.get("revision", 0))

    filled = []
    if artifacts:
        # Coverage proxy: fraction of unique top-level workspace paths touched.
        tops = {a["path"].split("/", 1)[0] for a in artifacts}
        filled = sorted(tops)

    last_ts = max((e.get("ts", 0) for e in events), default=0) or started
    last_event_age = max(0.0, now - last_ts)
    phase = log.get("phase") or ("build" if artifacts else "research")
    entities = derive_entities(findings, filled, phase, last_event_age,
                               artifacts_count=len(artifacts))
    metrics = compute_metrics({
        "worker_calls": n_calls, "artifacts_count": len(artifacts),
        "credit_blocked": credit_blocked,
    }, findings, entities)

    out = {
        "_t": now,
        "started": started,
        "uptime_s": max(0, now - started),
        "worker_calls": n_calls,
        "shadow_revision": revision,
        "severity": severity,
        "n_findings": len(findings),
        "n_blockers": len(blockers),
        "n_injections": len(injections),
        "credit_blocked": credit_blocked,
        "recent_errors": recent_errors[-5:],
        "phase": phase,
        "floor": log.get("floor"),
        "final": log.get("final"),
        "artifacts_count": len(artifacts),
        "artifacts_tops": filled,
        "latest_worker": worker_calls[-1] if worker_calls else None,
        "claims": findings,
        "entities": entities,
        "last_event_age": last_event_age,
        **metrics,
    }
    with _lock:
        _cached = out
    return out


def describe(out: dict[str, Any]) -> str:
    """Short human-readable status line used by voice + caption."""
    phase = out.get("phase") or "research"
    floor = out.get("floor") or "?"
    sev = out.get("severity") or "LOW"
    parts = [f"phase {phase} floor {floor}", f"{out.get('worker_calls',0)} worker calls",
             f"severity {sev}", f"{out.get('n_findings',0)} findings"]
    m_impl = int(round((out.get("implementation_completeness") or 0.0) * 100))
    m_ev = int(round((out.get("evidence_confidence") or 0.0) * 100))
    parts.append(f"implementation {m_impl}% · evidence {m_ev}%")
    if out.get("integrity_conflict"):
        parts.append(f"conflict {int(round(out.get('integrity_conflict',0)*100))}%")
    if out.get("credit_blocked"):
        parts.append("credit-blocked")
    if out.get("completion_blocked") and not out.get("credit_blocked"):
        parts.append("gate-blocked")
    if out.get("final"):
        parts.append(f"final {out['final']}")
    return "; ".join(parts)
