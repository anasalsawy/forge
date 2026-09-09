"""Lobe-B job-key builder and outbox helpers (transactional outbox producer)."""
from __future__ import annotations

from typing import Any

from ..core.idgen import sha256_short


def shadow_job_key(
    tenant_id: int,
    run_id: str,
    call_seq: int,
    context_text: str,
    response_text: str,
) -> str:
    """Deterministic job key: retries and redeploys never double-enqueue a job."""
    content = f"{tenant_id}:{run_id}:context_shadow:{call_seq}:{sha256_short(context_text)}:{sha256_short(response_text)}"
    return f"context_shadow:{sha256_short(content)}"


def shadow_payload(
    tenant_id: int,
    run_id: str,
    external_run_id: str,
    call_seq: int,
    context_text: str,
    response_text: str,
    floor_id: str = "",
    attempt_id: int = 1,
    stage: str = "observation",
    max_attempts: int = 5,
) -> dict[str, Any]:
    return {
        "kind": "context_shadow",
        "tenant_id": tenant_id,
        "run_id": run_id,
        "external_run_id": external_run_id,
        "call_seq": call_seq,
        "floor_id": floor_id,
        "attempt_id": attempt_id,
        "context_text": context_text,
        "response_text": response_text,
        "stage": stage,
        "max_attempts": max_attempts,
    }