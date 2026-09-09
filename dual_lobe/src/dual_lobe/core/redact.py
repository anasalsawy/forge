"""Secret redaction used before anything is written to the event ledger."""
from __future__ import annotations

import os
from copy import deepcopy
from typing import Any

from .settings import get_settings


def _secret_candidates() -> list[str]:
    s = get_settings()
    out = [v for v in (s.a_api_key, s.resolved_b_api_key) if v]
    out += [v for k, v in os.environ.items() if k.endswith(("API_KEY", "TOKEN", "SECRET")) and v]
    return out


_SECRETS: list[str] | None = None


def _secrets() -> list[str]:
    global _SECRETS
    if _SECRETS is None:
        _SECRETS = [v for v in _secret_candidates() if len(v) >= 8]
    return _SECRETS


def redact_text(text: str) -> str:
    out = text
    for secret in _secrets():
        out = out.replace(secret, "[REDACTED]")
    return out


def _redact_node(node: Any) -> Any:
    if isinstance(node, str):
        return redact_text(node)
    if isinstance(node, list):
        return [_redact_node(x) for x in node]
    if isinstance(node, dict):
        return {k: _redact_node(v) for k, v in node.items()}
    return node


def redact_payload(payload: Any) -> Any:
    return _redact_node(deepcopy(payload))