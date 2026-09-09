"""Claim/evidence extraction from Lobe-B structured output.

B's evidence ledger labels are *dispositions*, never verdicts. This module only
creates claim rows (status ``claimed``) and records them as ledger evidence; the
verifier is the sole writer of ``verified``/``contradicted``.
"""
from __future__ import annotations

import re
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from ..core import idgen
from ..state import repositories as repo

SEVERITY_WEIGHT = {
    "CRITICAL": 5,
    "HIGH": 3,
    "MEDIUM": 2,
    "LOW": 1,
}

DISPOSITIONS = {"VERIFIED", "SUPPORTED", "INFERRED", "UNVERIFIED", "CONTRADICTED"}

_FILE_RE = re.compile(
    r"(?:created|wrote|patched|edited|added|removed|exists|present)\s+[`'\"]?"
    r"([a-zA-Z0-9_./\-]+\.(?:py|js|ts|json|md|yaml|yml|toml|cfg|ini|txt|sh|sql|csv|env))[`'\"]?",
    re.I,
)


def _weight(severity: str | None) -> int:
    return SEVERITY_WEIGHT.get((severity or "LOW").upper(), 0)


async def ingest_ledger(
    session: AsyncSession,
    tenant_id: int,
    run_id: str,
    source_event_id: str | None,
    ledger: list[dict[str, Any]],
    created_by_role: str = "integrity_sentinel",
    cwd: str = ".",
) -> tuple[int, int]:
    """Create claims from B's evidence ledger. Returns (claims_created, claims_immediately_verified).

    Artifact-typed file claims are phrased and immediately grounded by the
    verifier; generic claims stay ``claimed`` until explicitly verified.
    """
    from . import verifier

    created = 0
    verified = 0
    for entry in ledger or []:
        claim_text = str(entry.get("claim") or "").strip()
        if not claim_text:
            continue
        disposition = str(entry.get("status") or "UNVERIFIED").upper()
        if disposition not in DISPOSITIONS:
            disposition = "UNVERIFIED"
        claim_type = "generic"
        artifact = ""
        m = _FILE_RE.search(claim_text)
        if m:
            claim_type = "file"
            artifact = m.group(1).strip("`'\"")
        claim = await repo.create_claim(
            session,
            tenant_id,
            run_id,
            source_event_id,
            claim_text if claim_type == "generic" else f"File {artifact!r} exists as stated",
            claim_type=claim_type,
            severity_weight=_weight(str(entry.get("severity") or "")),
            created_by_role=created_by_role,
        )
        await repo.append_evidence(
            session,
            tenant_id,
            "ledger",
            "PENDING",
            run_id=run_id,
            claim_id=str(claim.id),
            source="integrity_sentinel",
            tool_name="ledger_disposition",
            idempotency_key=f"{str(claim.id)}:disposition",
        )
        created += 1
        if claim_type == "file":
            from pathlib import Path

            path = artifact if artifact.startswith("/") else str(Path(cwd).expanduser() / artifact)
            ok, ev = await verifier.verify_artifact(
                session, tenant_id, "file_exists", path, run_id=run_id
            )
            verdict = "verified" if ok else "contradicted"
            await repo.set_claim_status(
                session, str(claim.id), verdict,
                verified_at=models_utcnow(), evidence_ids=[str(ev.id)],
            )
            verified += 1 if ok else 0
    return created, verified


def models_utcnow():
    from ..core.models import utcnow

    return utcnow()


def challenge_action(severity: str, mode: str) -> str:
    """Map a challenge's severity + rollout mode to an action for Lobe A."""
    weight = _weight(severity)
    if mode == "block" and weight >= 5:
        return "stop"
    if mode == "precondition" and weight >= 3:
        return "verify"
    if mode == "warn" and weight >= 3:
        return "hold"
    return "note"


def new_claim_id() -> str:
    return idgen.new_id()