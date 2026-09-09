"""X-DL-* correlation headers -> structured correlation dict."""
from __future__ import annotations

from typing import Any

DL_HEADERS = {
    "run": "X-DL-Run-ID",
    "floor": "X-DL-Floor-ID",
    "attempt": "X-DL-Attempt",
    "worker": "X-DL-Worker-ID",
    "role": "X-DL-Agent-Role",
    "task": "X-DL-Task-ID",
    "call_seq": "X-DL-Call-Seq",
}

BYPASS_ROLES = {"analyst", "auditor", "lobe-b", "b"}


def parse_headers(headers) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for key, hdr in DL_HEADERS.items():
        val = headers.get(hdr)
        if isinstance(val, str) and val.strip():
            out[key] = val.strip()
    mode = headers.get("X-Dual-Lobe-Mode")
    if isinstance(mode, str):
        out["mode"] = mode.strip().lower()
    try:
        if "attempt" in out:
            out["attempt"] = int(out["attempt"])
    except (TypeError, ValueError):
        out.pop("attempt", None)
    try:
        if "call_seq" in out:
            out["call_seq"] = int(out["call_seq"])
    except (TypeError, ValueError):
        out.pop("call_seq", None)
    return out


def is_bypass(corr: dict[str, Any]) -> bool:
    if corr.get("mode") == "bypass":
        return True
    return str(corr.get("role", "")).lower() in BYPASS_ROLES