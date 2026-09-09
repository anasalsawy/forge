"""Integrity Sentinel / verifier for the dual-lobe runtime.

Lobe B and the Oversight channel surface *claims* and *challenges* but never write
verdicts. Only this verifier settles a claim, and only through the single
``update_claim_status`` write path in the append-only ledger. Verdicts must be
grounded in artifacts (file existence, git commit, command output, HTTP status),
never in the asserting model's own text.

Claim lifecycle: claimed -> (PENDING verification) -> VERIFIED | SUPPORTED |
INFERRED | UNVERIFIED | CONTRADICTED.
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import subprocess
import time
import uuid
from pathlib import Path
from typing import Any

from . import dual_lobe_persistence as persist

LOG = logging.getLogger("dual_lobe.verifier")

# Only these are terminal/verdict statuses this sentinel may write.
VERDICT_STATUSES = {"VERIFIED", "SUPPORTED", "INFERRED", "UNVERIFIED", "CONTRADICTED"}

# A claim phrased only as an assertion/opinion is not independently verifiable
# against an artifact; it stays INFERRED rather than being rubber-stamped.
_OPINION_HINTS = re.compile(
    r"\b(i (think|believe|assume|suggest)|probably|maybe|arguably|could be|likely|seems to)\b",
    re.I,
)


def _new_id() -> str:
    return str(uuid.uuid4())


def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8", "ignore")).hexdigest()[:16]


class VerifiableArtifact:
    """A concrete fact we can ground a claim against: file / git / command / http."""

    def __init__(self, db: persist.Persistence, run_id: str, event_id: str, source: str) -> None:
        self.db = db
        self.run_id = run_id
        self.event_id = event_id
        self.source = source
        self._now = time.time()

    def _record(self, kind: str, validation_status: str, *, artifact_ref: str = "",
                artifact_hash: str = "", tool_name: str = "", commit_sha: str = "") -> str:
        eid = _new_id()
        self.db.append_evidence(
            eid, self.run_id, self.event_id, kind, self._now,
            source=self.source, artifact_ref=artifact_ref, artifact_hash=artifact_hash,
            tool_name=tool_name, commit_sha=commit_sha, validation_status=validation_status,
        )
        return eid

    # -- grounded checks ------------------------------------------------------
    def file_exists(self, path: str) -> tuple[bool, str]:
        ok = Path(path).expanduser().is_file()
        eid = self._record("file", "PASS" if ok else "FAIL", artifact_ref=path,
                           tool_name="file_exists")
        return ok, eid

    def dir_exists(self, path: str) -> tuple[bool, str]:
        ok = Path(path).expanduser().is_dir()
        eid = self._record("dir", "PASS" if ok else "FAIL", artifact_ref=path,
                           tool_name="dir_exists")
        return ok, eid

    def git_commit_exists(self, repo: str | Path, sha: str) -> tuple[bool, str]:
        try:
            r = subprocess.run(
                ["git", "-C", str(repo), "cat-file", "-e", f"{sha}^{{commit}}"],
                capture_output=True, timeout=20, check=True,
            )
            ok = True if r.returncode == 0 else False
        except Exception:
            ok = False
        eid = self._record("git", "PASS" if ok else "FAIL", artifact_ref=str(repo),
                           commit_sha=sha, tool_name="git_commit_exists")
        return ok, eid

    def file_contains(self, path: str, needle: str) -> tuple[bool, str]:
        try:
            text = Path(path).expanduser().read_text(encoding="utf-8", errors="ignore")
            ok = needle in text
            h = _sha256(text)
        except Exception:
            ok, h = False, ""
        eid = self._record("file", "PASS" if ok else "FAIL", artifact_ref=path,
                           artifact_hash=h, tool_name="file_contains")
        return ok, eid

    def command_succeeds(self, argv: list[str], timeout: int = 60) -> tuple[bool, str, str]:
        try:
            r = subprocess.run(argv, capture_output=True, text=True, timeout=timeout)
            ok = r.returncode == 0
            eid = self._record("command", "PASS" if ok else "FAIL",
                               artifact_ref=" ".join(argv), tool_name="command")
            return ok, eid, (r.stdout or "")[:200]
        except Exception as e:
            eid = self._record("command", "FAIL", artifact_ref=" ".join(argv),
                               tool_name="command", artifact_hash=f"{type(e).__name__}: {e}")
            return False, eid, f"{type(e).__name__}: {e}"


def _extract_file_claims(text: str, cwd: Path) -> list[dict[str, Any]]:
    """Heuristic claim extraction: detect statements that name a local artifact
    (file/dir path or bare-name) and phrase them as verifiable file claims."""
    claims: list[dict[str, Any]] = []
    seen: set[str] = set()
    for raw in re.finditer(r"(?:created|wrote|patched|edited|added|removed|exists|present)\s+[`'\"]?([a-zA-Z0-9_./\-]+\.(?:py|js|ts|json|md|yaml|yml|toml|cfg|ini|txt|sh|sql|csv|env))[`'\"]?", text, re.I):
        ref = raw.group(1).strip("`'\"")
        if ref in seen:
            continue
        seen.add(ref)
        claims.append({
            "claim": f"File {ref!r} exists as stated",
            "claim_type": "file",
            "artifact": ref,
            "check": "file_exists",
        })
    return claims


def ingest_claims_for_workflow(
    db: persist.Persistence,
    run_id: str,
    sources: list[dict[str, Any]],
    cwd: str = ".",
) -> int:
    """Create claim rows from workflow/oversight sources (events or B post-pass),
    then immediately attempt grounded verification for artifact-typed claims."""
    cwd_p = Path(cwd).expanduser()
    verifier = VerifiableArtifact(db, run_id, source_event_or_none(sources), "verifier")
    created = 0
    for src in sources:
        # Each source may carry its own event_id for provenance.
        verifier.event_id = str(src.get("event_id") or verifier.event_id)
        verifier.source = str(src.get("source") or src.get("kind") or "workflow")
        for claim in src.get("claims") or []:
            if not claim.get("claim"):
                continue
            claim_id = _new_id()
            claim_type = str(claim.get("claim_type") or "generic")
            db.create_claim(
                claim_id, run_id, verifier.event_id,
                str(claim["claim"]), time.time(),
                claim_type=claim_type,
                severity=str(claim.get("severity") or "MEDIUM"),
            )
            created += 1
            if claim_type == "file":
                path = str(claim.get("artifact") or "")
                if not path:
                    continue
                ok, evidence_id = verifier.file_exists(path if os.path.isabs(path) else str(cwd_p / path))
                verdict = "VERIFIED" if ok else "CONTRADICTED"
                db.update_claim_status(claim_id, verdict, verified_at=time.time(),
                                       evidence_ids=[evidence_id])
    return created


def source_event_or_none(sources: list[dict[str, Any]]) -> str:
    for s in sources:
        if s.get("event_id"):
            return str(s["event_id"])
    return ""


def verify_single_claim(db: persist.Persistence, claim_id: str, check: str,
                        artifact: str, needle: str = "", cwd: str = ".",
                        run_id: str = "", event_id: str = "") -> str:
    """Run one named check against an artifact and settle the claim. Returns the
    verdict written. Gate for future claim-level verification RPCs."""
    cwd_p = Path(cwd).expanduser()
    verifier = VerifiableArtifact(db, run_id, event_id, "verifier")
    path = artifact if os.path.isabs(artifact) else str(cwd_p / artifact)
    if check == "file_exists":
        ok, eid = verifier.file_exists(path)
    elif check == "dir_exists":
        ok, eid = verifier.dir_exists(path)
    elif check == "file_contains":
        ok, eid = verifier.file_contains(path, needle)
    else:
        return "UNVERIFIED"
    verdict = "VERIFIED" if ok else "CONTRADICTED"
    db.update_claim_status(claim_id, verdict, verified_at=time.time(), evidence_ids=[eid])
    return verdict
