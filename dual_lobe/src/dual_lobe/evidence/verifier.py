"""Integrity Sentinel / verifier.

Lobe B surfaces claims and challenges but never writes verdicts. Only this
module settles a claim, through ``set_claim_status``, grounded in artifacts
(file/git/command) rather than the asserting model's own text. Lobe A therefore
can never mark its own claim VERIFIED.
"""
from __future__ import annotations

import asyncio
import hashlib
import os
from pathlib import Path
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from ..core import idgen, models
from ..state import repositories as repo


def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8", "ignore")).hexdigest()[:16]


async def _check_file_exists(session: AsyncSession, tenant_id: int, run_id: str | None, path: str) -> models.Evidence:
    ok = Path(path).expanduser().is_file()
    ev = await repo.append_evidence(
        session, tenant_id, "file", "PASS" if ok else "FAIL",
        run_id=run_id, source="verifier", artifact_ref=path, tool_name="file_exists",
    )
    return ev


async def _check_dir_exists(session: AsyncSession, tenant_id: int, run_id: str | None, path: str) -> models.Evidence:
    ok = Path(path).expanduser().is_dir()
    ev = await repo.append_evidence(
        session, tenant_id, "dir", "PASS" if ok else "FAIL",
        run_id=run_id, source="verifier", artifact_ref=path, tool_name="dir_exists",
    )
    return ev


async def _check_file_contains(session: AsyncSession, tenant_id: int, run_id: str | None, path: str, needle: str) -> models.Evidence:
    ok = False
    h = ""
    try:
        text = Path(path).expanduser().read_text(encoding="utf-8", errors="ignore")
        ok = needle in text
        h = _sha256(text)
    except Exception:
        pass
    ev = await repo.append_evidence(
        session, tenant_id, "file", "PASS" if ok else "FAIL",
        run_id=str(run_id) if run_id else None, source="verifier",
        artifact_ref=path, artifact_hash=h, tool_name="file_contains",
    )
    return ev


async def _check_git_commit(session: AsyncSession, tenant_id: int, run_id: str | None, repo_path: str, sha: str) -> models.Evidence:
    try:
        proc = await asyncio.create_subprocess_exec(
            "git", "-C", str(repo_path), "cat-file", "-e", f"{sha}^{{commit}}",
            stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL,
        )
        _rc = await asyncio.wait_for(proc.wait(), timeout=20)
        ok = proc.returncode == 0
    except Exception:
        ok = False
    ev = await repo.append_evidence(
        session, tenant_id, "git", "PASS" if ok else "FAIL",
        run_id=str(run_id) if run_id else None, source="verifier",
        artifact_ref=str(repo_path), tool_name="git_commit_exists",
    )
    return ev


async def verify_artifact(
    session: AsyncSession,
    tenant_id: int,
    check: str,
    artifact: str,
    *,
    run_id: str | None = None,
    needle: str = "",
    cwd: str = ".",
) -> tuple[bool, models.Evidence]:
    path = artifact if os.path.isabs(artifact) else str(Path(cwd).expanduser() / artifact)
    if check == "file_exists":
        ev = await _check_file_exists(session, tenant_id, run_id, path)
    elif check == "dir_exists":
        ev = await _check_dir_exists(session, tenant_id, run_id, path)
    elif check == "file_contains":
        ev = await _check_file_contains(session, tenant_id, run_id, path, needle)
    elif check == "git_commit_exists":
        sha, repo_path = artifact.rsplit(":", 1) if ":" in artifact else (artifact, cwd)
        ev = await _check_git_commit(session, tenant_id, run_id, repo_path, sha)
    else:
        ev = await repo.append_evidence(
            session, tenant_id, check, "PENDING",
            run_id=str(run_id) if run_id else None, source="verifier",
            artifact_ref=path, tool_name="unknown_check",
        )
    return ev.validation_status == "PASS", ev


async def verify_claim(
    session: AsyncSession,
    tenant_id: int,
    claim_id: str,
    check: str,
    artifact: str,
    *,
    needle: str = "",
    cwd: str = ".",
    run_id: str | None = None,
) -> str:
    ok, ev = await verify_artifact(
        session, tenant_id, check, artifact, run_id=run_id or claim_id, needle=needle, cwd=cwd
    )
    verdict = "verified" if ok else "contradicted"
    await repo.set_claim_status(
        session, claim_id, verdict,
        verified_at=models.utcnow(),
        evidence_ids=[str(ev.id)],
    )
    await session.commit()
    return verdict