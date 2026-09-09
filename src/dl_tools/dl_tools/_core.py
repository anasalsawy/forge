"""Core implementations for the dl_tools package.

Execution model: synchronous subprocess with a hard timeout and bounded output.
Destructive/irreversible operations are allowed (unrestricted by design).
"""

from __future__ import annotations

import os
import re
import shutil
import subprocess
import time
import uuid
from pathlib import Path

from crewai.tools import BaseTool

_SCRIPT_RE = re.compile(r"[^\x09\x0a\x0d\x20-\x7e]")
MAX_OUT = 32_000


def _now() -> float:
    return time.time()


def _attempt_ledger(kind: str, payload: str, *, when: float | None = None,
                    duration: float = 0.0) -> None:
    """Best-effort evidence record into the dual-lobe ledger.

    Purely additive and failure-tolerant: the build must never break because
    the ledger is missing or busy.
    """
    try:
        from deep_investigator_build_pipeline import dual_lobe_persistence as dlp
        run_id = os.environ.get("DL_RUN_ID", "")
        floor_id = os.environ.get("DL_FLOOR_ID", "")
        attempt = int(os.environ.get("DL_ATTEMPT", "0") or 0)
        role = os.environ.get("DL_AGENT_ROLE", "builder")
        db = dlp.Persistence()
        db.append_event(
            str(uuid.uuid4()),
            kind,
            when or _now(),
            run_id=run_id,
            floor_id=floor_id,
            attempt_id=attempt,
            agent_role=role,
            payload={"cmd_duration_s": round(duration, 3), "out_tail": payload[-4000:]},
        )
    except Exception:
        pass


def _run_proc(cmd: list[str], *, cwd: str | None, timeout: int,
              env_extra: dict[str, str] | None = None) -> str:
    env = os.environ.copy()
    if env_extra:
        env.update(env_extra)
    proc = subprocess.run(
        cmd,
        cwd=cwd,
        capture_output=True,
        text=True,
        timeout=timeout,
        env=env,
    )
    out = (proc.stdout or "") + ("\n[STDERR]\n" + proc.stderr if proc.stderr else "")
    if not out.strip():
        out = f"(exit {proc.returncode}; no output)"
    if len(out) > MAX_OUT:
        out = out[:MAX_OUT] + f"\n...[truncated {len(out)} chars]"
    return out.strip()


class BashTool(BaseTool):
    name: str = "bash_exec"
    description: str = (
        "Execute an arbitrary shell command on this machine (unrestricted, no "
        "sandbox). Use for building, running, testing, installing, inspecting, "
        "and any OS operation. Provide a single shell command string. Use "
        "newlines or '&&' to chain. The command runs with the caller's env."
    )

    def _run(self, command: str, cwd: str = "", timeout: int = 180) -> str:
        started = _now()
        try:
            cwd = cwd or os.getcwd()
            out = _run_proc(["bash", "-lc", command], cwd=cwd, timeout=max(1, timeout))
            return out
        finally:
            _attempt_ledger("builder_tool", f"bash: {command[:300]}",
                            duration=time.time() - started)


class FileWriteTool(BaseTool):
    name: str = "file_write"
    description: str = (
        "Write or replace (or append when append=true) UTF-8 text content in a "
        "file at an arbitrary absolute or cwd-relative path. Creates parent "
        "directories. No path restrictions. Returns the path and byte count."
    )

    def _run(self, path: str, content: str = "", append: bool = False) -> str:
        started = _now()
        p = Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        mode = "a" if append else "w"
        with p.open(mode, encoding="utf-8", newline="\n") as fh:
            n = fh.write(content)
        _attempt_ledger("builder_tool", f"write {p} ({n} bytes)",
                        duration=time.time() - started)
        return f"wrote {len(content)} chars to {p}"


class GitTool(BaseTool):
    name: str = "git_exec"
    description: str = (
        "Run a git command in a repository. For example 'git add -A', 'git "
        "commit -m msg', 'git status --short', 'git log --oneline -5'. Provide "
        "the exact git argument list as a single string (the leading 'git' is "
        "optional). Works in repo_dir (defaults to the current working "
        "directory). Uses the caller's git identity and credentials."
    )

    def _run(self, command: str, repo_dir: str = "", timeout: int = 120) -> str:
        started = _now()
        repo = repo_dir or os.getcwd()
        parts = command.strip().split()
        if parts and parts[0] == "git":
            parts = parts[1:]
        if not parts:
            return "no git command supplied"
        try:
            return _run_proc(["git", *parts], cwd=repo, timeout=max(1, timeout))
        finally:
            _attempt_ledger("builder_tool", f"git: {command[:200]}",
                            duration=time.time() - started)


class ValidateTool(BaseTool):
    name: str = "run_validation"
    description: str = (
        "Validate a Python project directory: byte-compiles every .py file and "
        "runs the unit tests (pytest if present, else unittest discover in "
        "tests/). Returns PASS/FAIL with the real command output. Use this to "
        "produce genuine validation evidence for code you write."
    )

    def _run(self, project_dir: str, timeout: int = 300) -> str:
        started = _now()
        root = Path(project_dir or os.getcwd())
        lines: list[str] = []
        py_files = [str(p) for p in root.rglob("*.py") if ".venv" not in str(p)
                    and "site-packages" not in str(p)]
        for chunk_start in range(0, len(py_files), 200):
            chunk = py_files[chunk_start:chunk_start + 200]
            try:
                out = _run_proc(["python", "-m", "py_compile", *chunk],
                                cwd=str(root), timeout=timeout)
                lines.append(f"py_compile[{len(chunk)} files]: OK")
                if "Error" in out or "Traceback" in out:
                    lines.append(out)
            except subprocess.TimeoutExpired:
                lines.append("py_compile: TIMEOUT")
        missing = []
        for marker in ("pytest.ini", "pyproject.toml", "setup.cfg"):
            if (root / marker).exists():
                missing.append(marker)
        try:
            if missing and shutil.which("pytest"):
                out = _run_proc(["python", "-m", "pytest", "-q", "-x"],
                                cwd=str(root), timeout=timeout)
                lines.append("pytest: " + out)
            else:
                tdir = root / "tests"
                if tdir.exists():
                    out = _run_proc(["python", "-m", "unittest", "discover", "-s", "tests", "-v"],
                                    cwd=str(root), timeout=timeout)
                    lines.append("unittest: " + out)
                else:
                    lines.append("no tests dir found; pytest markers absent")
        except subprocess.TimeoutExpired:
            lines.append("tests: TIMEOUT")
        report = "\n".join(lines)
        status = "PASS" if ("FAILED" not in report and "Error" not in report
                            and "Traceback" not in report and "TIMEOUT" not in report) else "FAIL"
        final = f"VALIDATION {status} for {root}\n{report}"
        _attempt_ledger("builder_tool", f"validate {root} -> {status}",
                        duration=time.time() - started)
        return final