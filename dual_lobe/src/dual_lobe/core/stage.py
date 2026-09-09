"""Staged rollout semantics for the dual-lobe proxy.

Each stage is cumulative:

- observation         A only; B records evidence, no injection.
- context             B distilled context is injected into later A calls.
- integrity-observe   B challenges recorded and surfaced as warnings only.
- integrity-intervene HIGH/CRITICAL challenges become verification preconditions.
- enforcement         CRITICAL findings block propagation of false completion.
"""
from __future__ import annotations


STAGES = ("observation", "context", "integrity-observe", "integrity-intervene", "enforcement")


def stage_index(stage: str) -> int:
    stage = (stage or "observation").strip().lower()
    if stage not in STAGES:
        raise ValueError(f"unknown rollout stage: {stage!r}")
    return STAGES.index(stage)


def at_least(current: str, required: str) -> bool:
    return stage_index(current) >= stage_index(required)


def injection_enabled(stage: str) -> bool:
    return at_least(stage, "context")


def challenge_mode(stage: str) -> str:
    """How B challenges surface to Lobe A. One of none|note|warn|precondition|block."""
    if at_least(stage, "enforcement"):
        return "block"
    if at_least(stage, "integrity-intervene"):
        return "precondition"
    if at_least(stage, "integrity-observe"):
        return "warn"
    if at_least(stage, "context"):
        return "note"
    return "none"