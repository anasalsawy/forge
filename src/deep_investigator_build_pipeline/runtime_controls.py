"""Runtime controls: operator control bridge for the dual-lobe pipeline.

All operator actions funnel here. Two kinds:

- Read-only status (never gated): snapshot, describe, force beacon frame.
- Destructive (require a prior verbal confirm): halt / restart / edit prompt /
  reverse to a natural checkpoint / per-file rollback.
- Directive injection (NOT destructive): a voiced instruction for the NEXT
  floor. It rides the flow's natural handoff via the ``## OPERATOR DIRECTIVE``
  lane persisted in ``output/control.json``. Newest directive replaces any
  pending one (single slot).

The run wrapper (run_dual_lobe.py) polls ``control.json`` to react to
halt/restart/reverse, and the flow's next-floor task description consumes the
``directive`` text as an injected crew input.
"""
from __future__ import annotations

import json
import os
import signal
import subprocess
import threading
import time
from pathlib import Path
from typing import Any

from deep_investigator_build_pipeline import runtime_cortex as cortex

ROOT = Path(os.getenv("RUNTIME_BEACON_ROOT", os.getcwd()))
CONTROL_FILE = ROOT / "output" / "control.json"
SNAPSHOT_DIR = ROOT / "output" / "provenance"

_lock = threading.Lock()
_control: dict[str, Any] = {"directive": None, "command": None, "updated_at": 0.0}
CONFIRM_WINDOW = float(os.getenv("RUNTIME_CONTROL_CONFIRM_SECONDS", "30"))


def _ensure() -> None:
    CONTROL_FILE.parent.mkdir(parents=True, exist_ok=True)
    SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)


def _read() -> dict[str, Any]:
    _ensure()
    try:
        if CONTROL_FILE.exists():
            return json.loads(CONTROL_FILE.read_text(encoding="utf-8"))
    except Exception:
        pass
    return {"directive": None, "command": None, "updated_at": 0.0}


def _write() -> None:
    _ensure()
    try:
        CONTROL_FILE.write_text(json.dumps(_control, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        pass


def read_control() -> dict[str, Any]:
    with _lock:
        _control.update(_read())
        return dict(_control)


def set_directive(text: str, floor: str | None = None) -> dict[str, Any]:
    """Queue a SINGLE operator directive for the next floor. Newest wins."""
    with _lock:
        _control["directive"] = {"text": text, "floor": floor, "ts": time.time()}
        _control["command"] = None
        _write()
        return dict(_control)


def consume_directive() -> dict[str, Any] | None:
    """Called by the flow header to pick up and clear the pending directive."""
    with _lock:
        _control.update(_read())
        d = _control.get("directive")
        if d and isinstance(d, dict) and d.get("text"):
            _control["directive"] = None
            _write()
            return d
        return None


def _status() -> dict[str, Any]:
    out = cortex.snapshot(force=False)
    return {
        "type": "status",
        "text": cortex.describe(out),
        "snapshot": {k: v for k, v in out.items() if k not in ("_t", "latest_worker")},
    }


def force_beacon_frame() -> dict[str, Any]:
    return {"type": "ok", "text": "Beacon frame refresh requested."}


# --- Destructive / control commands -----------------------------------------

def command_halt() -> dict[str, Any]:
    """Immediate halt: SIGINT/SIGTERM to the crewai worker tree."""
    try:
        pid = int(os.getenv("RUNTIME_RUN_PID", "0") or "0")
        if pid:
            os.killpg(pid, signal.SIGTERM)
        else:
            subprocess.Popen(["pkill", "-SIGTERM", "-f", "crewai run"])
        return {"type": "ok", "text": "Run halted."}
    except Exception as e:
        return {"type": "error", "text": f"halt failed: {e}"}


def command_restart() -> dict[str, Any]:
    """Full teardown + relaunch (re-run run_dual_lobe.py fresh)."""
    try:
        subprocess.Popen(["setsid", "bash", "-c",
                          "cd %s && .venv/bin/python run_dual_lobe.py %s > output/relaunch.log 2>&1 < /dev/null & disown"
                          % (ROOT, os.getenv("RUNTIME_RUN_ARGS", ""))])
        return {"type": "ok", "text": "Pipeline restarting."}
    except Exception as e:
        return {"type": "error", "text": f"restart failed: {e}"}


def command_reverse(to_floor: str | None = None) -> dict[str, Any]:
    """Reverse: relaunch, requesting a resume from the given floor (best-effort)."""
    try:
        with _lock:
            _control["reverse_to_floor"] = to_floor
            _write()
        msg = command_restart()
        if to_floor:
            msg = {**msg, "text": f"Pipeline restarting from floor {to_floor} (best-effort)."}
        return msg
    except Exception as e:
        return {"type": "error", "text": f"reverse failed: {e}"}


def command_edit_prompt(new_prompt: str) -> dict[str, Any]:
    with _lock:
        _control["prompt_override"] = new_prompt
        _write()
    return {"type": "ok", "text": "Prompt updated for next kickoff."}


# --- Per-file rollback (Tier-1 reversal primitive) --------------------------

PER_FLOOR_SNAPSHOT = SNAPSHOT_DIR / "per_floor.jsonl"


def record_file_snapshots(floor: str, files: list[dict[str, Any]]) -> None:
    """Persist which files were touched during a floor (path, size, mtime)."""
    _ensure()
    if not files:
        return
    rec = {"floor": floor, "ts": time.time(), "files": files}
    try:
        with PER_FLOOR_SNAPSHOT.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
    except Exception:
        pass


def rollback_file(relpath: str, to_floor: str | int | None = None) -> dict[str, Any]:
    """Undo a single file to its state as of a given floor (or before any floor)."""
    _ensure()
    try:
        if not PER_FLOOR_SNAPSHOT.exists():
            return {"type": "error", "text": "No provenance snapshots available."}
        snapshots = []
        with PER_FLOOR_SNAPSHOT.open("r", encoding="utf-8") as fh:
            for ln in fh:
                ln = ln.strip()
                if not ln:
                    continue
                try:
                    snapshots.append(json.loads(ln))
                except Exception:
                    continue
        # Collect each floor's recorded entry for this file.
        versions = []
        for snap in snapshots:
            for f in snap.get("files", []):
                if f.get("path") == relpath or relpath in f.get("path", ""):
                    versions.append({"floor": snap["floor"], "ts": snap["ts"],
                                     "size": f.get("size"), "mtime": f.get("mtime")})
        if not versions:
            return {"type": "error", "text": f"No provenance for {relpath}."}
        # Determine cutoff: only undo changes from floors >= to_floor (default: all).
        target = ROOT / relpath
        if not target.exists():
            return {"type": "error", "text": f"{relpath} not present on disk."}
        # v1: restore from the earliest recorded version of the file present.
        first = versions[0]
        earliest = ROOT / "output" / "provenance" / "originals" / relpath
        if not earliest.exists():
            return {"type": "note", "text": f"Original of {relpath} not stored; cannot roll back. Nothing changed."}
        import shutil
        shutil.copy2(earliest, target)
        return {"type": "ok", "text": f"Rolled back {relpath} to its original pre-{first['floor']} state."}
    except Exception as e:
        return {"type": "error", "text": f"rollback failed: {e}"}
