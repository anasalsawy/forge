"""rt_mirror — the Project Mirror visible from the shell.

Three sibling surfaces (also see the browser beacon on :8766):

- ``rt_mirror``              one-shot truth snapshot: stage + (M,E,K,F) matrix
- ``rt_mirror --watch``      live ANSI dashboard that redraws every N seconds
- ``rt_mirror --diff``       semantic diff vs the previously recorded revision

Everything reads the same reducer (runtime_cortex) that drives the beacon and
voice, so the terminal, the browser, and the narrator can never disagree.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path
from typing import Any

from deep_investigator_build_pipeline import runtime_cortex as cortex

REVISIONS_DIR: Path | None = None

GREEN = "\033[32m"
YELLOW = "\033[33m"
RED = "\033[31m"
CYAN = "\033[36m"
MAGENTA = "\033[35m"
BOLD = "\033[1m"
DIM = "\033[2m"
RESET = "\033[0m"
CLEAR = "\033[2J\033[H"


def _glyph(e: dict[str, Any]) -> tuple[str, str]:
    if e.get("K", 0) > 0 or e.get("E", 1.0) <= 0.15:
        return "✗", RED
    if e.get("E", 0.0) >= 0.75 and e.get("F", 1.0) >= 0.8:
        return "✓", GREEN
    if e.get("M", 0.0) == 0.0:
        return "·", CYAN
    return "~", YELLOW


def _fmt_state(out: dict[str, Any]) -> str:
    lines: list[str] = []
    phase = (out.get("phase") or "research").upper()
    floor = out.get("floor")
    head = f"{BOLD}PIPELINE MIRROR — truth reflector{RESET}"
    if floor is not None:
        head += f"   {CYAN}phase {phase} · floor {floor}{RESET}"
    elif phase:
        head += f"   {CYAN}phase {phase}{RESET}"
    lines.append(head)
    bar = "─" * 64
    lines.append(bar)

    m_impl = int(round((out.get("implementation_completeness") or 0.0) * 100))
    m_ev = int(round((out.get("evidence_confidence") or 0.0) * 100))
    m_rc = int(round((out.get("research_completeness") or 0.0) * 100))
    m_ic = int(round((out.get("integrity_conflict") or 0.0) * 100))
    impl_s = f"{GREEN}{m_impl}%{RESET}" if m_impl >= 60 else f"{YELLOW}{m_impl}%{RESET}"
    ev_s = f"{GREEN}{m_ev}%{RESET}" if m_ev >= 60 else f"{YELLOW}{m_ev}%{RESET}"
    lines.append(f" {BOLD}Implementation{RESET} {impl_s} · "
                 f"{BOLD}Evidence{RESET} {ev_s} · "
                 f"{BOLD}Research{RESET} {m_rc}% · "
                 f"{BOLD}Conflict{RESET} {m_ic}%")
    extras = [f"{out.get('worker_calls',0)} worker calls",
              f"{out.get('n_findings',0)} findings",
              f"{out.get('artifacts_count',0)} files"]
    if out.get("n_blockers"):
        extras.append(f"{RED}{out.get('n_blockers')} blockers{RESET}")
    sev = (out.get("severity") or "LOW")
    extras.append(f"severity {sev}")
    lines.append("   " + " · ".join(extras))
    if out.get("credit_blocked"):
        lines.append(f"   {BOLD}{RED}⚠ CREDITS BLOCKED{RESET}")
    if out.get("completion_blocked") and not out.get("credit_blocked"):
        lines.append(f"   {BOLD}{YELLOW}⚠ COMPLETION GATE BLOCKED (contradiction){RESET}")
    if out.get("final"):
        fcol = GREEN if out["final"] == "PASS" else RED
        lines.append(f"   {BOLD}FINAL {fcol}{out['final']}{RESET}")
    lines.append(bar)

    # Entity matrix — worst first so the problems surface immediately.
    ents = sorted(out.get("entities", []), key=lambda e: (-e.get("K", 0), e.get("E", 1.0)))
    if ents:
        lines.append(f" {BOLD}{'ENTITY':<40}{'M':>5} {'E':>5} {'K':>4} {'F':>5}{RESET}")
        for e in ents:
            g, col = _glyph(e)
            name = e["name"][:40]
            lines.append(f" {col}{g}{RESET} {name:<39} "
                         f"{e['M']:>5.2f} {e['E']:>5.2f} "
                         f"{e['K']:>4.1f} {e['F']:>5.2f}")
        lines.append(bar)

    claims = out.get("claims") or []
    if claims:
        shown = claims[-8:] if len(claims) > 8 else claims
        lines.append(f" {BOLD}EVIDENCE (last {len(shown)} of {len(claims)}){RESET}")
        for c in shown:
            st = str(c.get("status", "?")).upper()
            scol = GREEN if st in ("VERIFIED", "SUPPORTED") else (YELLOW if st == "INFERRED" else RED)
            txt = str(c.get("claim", ""))[:60]
            lines.append(f"   {scol}[{st:<12}]{RESET} {txt}")
        lines.append(bar)

    if out.get("credit_blocked"):
        errs = out.get("recent_errors") or []
        if errs:
            lines.append(f" {RED}{BOLD}RECENT ERRORS{RESET}")
            for er in errs[-3:]:
                lines.append(f"   {DIM}{str(er)[:80]}{RESET}")

    latest = out.get("latest_worker") or {}
    if latest:
        ch = latest.get("rerun") or latest.get("model") or latest.get("topic_hint")
        tc = f"{str(ch)[:60]}" if ch else ""
        if tc:
            lines.append(f" {DIM}latest worker: {tc}{RESET}")
    return "\n".join(lines) + "\n"


def _fmt_diff(prev: dict[str, Any] | None, cur: dict[str, Any]) -> str:
    d = cortex.diff_revisions(prev, cortex.make_revision(cur))
    lines = [f"{BOLD}SEMANTIC DIFF{RESET} (D_Δ {d['delta']:.2f})"]
    if d.get("new"):
        lines.append("   (first snapshot — baseline established)")
    if d.get("added"):
        lines.append(f"   + added: {', '.join(GREEN + a + RESET for a in d['added'])}")
    if d.get("removed"):
        lines.append(f"   − removed: {', '.join(RED + a + RESET for a in d['removed'])}")
    if d.get("conflict_changed"):
        lines.append(f"   ! conflict changed: {', '.join(YELLOW + a + RESET for a in d['conflict_changed'])}")
    if d.get("gate_events"):
        g = ", ".join(str(k) for k in d["gate_events"])
        lines.append(f"   ⚡ gate events: {CYAN}{g}{RESET}")
    if not (d.get("added") or d.get("removed") or d.get("conflict_changed") or d.get("gate_events")):
        lines.append("   (no meaningful change)")
    return "\n".join(lines) + "\n"


def _render(out: dict[str, Any], show_diff: bool) -> str:
    if not os.getenv("NO_COLOR"):
        pass
    parts = [_fmt_state(out)]
    if show_diff:
        revs = cortex.read_revisions(REVISIONS_DIR, 2)
        prev = revs[-2] if len(revs) >= 2 else (revs[-1] if revs else None)
        parts.append(_fmt_diff(prev, out))
    return "\n".join(parts)


def _run_once(args: argparse.Namespace) -> int:
    if args.json:
        out = cortex.snapshot(force=True)
        sys.stdout.write(json.dumps(out, ensure_ascii=False, indent=2) + "\n")
        return 0
    out = cortex.snapshot(force=True)
    if args.diff:
        cortex.save_revision(REVISIONS_DIR, cortex.make_revision(out))
    sys.stdout.write(_render(out, show_diff=args.diff))
    return 0


def _watch(args: argparse.Namespace) -> None:
    interval = args.watch if isinstance(args.watch, (int, float)) else 5.0
    try:
        while True:
            out = cortex.snapshot(force=True)
            if not os.getenv("NO_COLOR"):
                sys.stdout.write(CLEAR)
            if args.diff:
                cortex.save_revision(REVISIONS_DIR, cortex.make_revision(out))
            sys.stdout.write(f"{DIM}{time.strftime('%H:%M:%S')} updated{RESET}\n")
            sys.stdout.write(_render(out, show_diff=args.diff))
            sys.stdout.flush()
            time.sleep(interval)
    except KeyboardInterrupt:
        pass


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default=os.getcwd())
    ap.add_argument("--json", action="store_true", help="dump the raw snapshot as JSON")
    ap.add_argument("--diff", nargs="?", const=True, default=False,
                    help="show semantic diff vs prior recorded revision")
    ap.add_argument("--watch", nargs="?", const=5.0, type=float, default=None,
                    help="live ANSI dashboard (optional refresh seconds)")
    args = ap.parse_args()

    root = Path(args.root)
    if root != Path.cwd():
        os.chdir(root)
    cortex.ROOT_DIR = root
    cortex.STATE_DIR = Path(os.getenv("DUAL_LOBE_STATE_DIR", root / ".dual_lobe"))
    global REVISIONS_DIR
    env = os.getenv("RUNTIME_BEACON_REVISIONS")
    REVISIONS_DIR = Path(env) if env else root / ".beacon" / "revisions"

    if args.watch is not None:
        _watch(args)
    else:
        sys.exit(_run_once(args))


if __name__ == "__main__":
    main()