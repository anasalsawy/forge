"""Provider adapters for the dual-lobe gateway.

The gateway never assumes every upstream accepts ``/v1/chat/completions``. A
``ProviderAdapter`` turns a normalized conversation/tool request into the dialect
an upstream speaks. Today we implement the OpenAI-compatible Chat Completions
adapter (Featherless and any OpenAI-compatible endpoint) and a guarded stub for
the OpenCode Zen Responses dialect, which is not yet validated in this project.

The gateway's Worker-facing endpoint always presents the normal Chat Completions
shape; only the upstream leg differs by adapter.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Any

from litellm import completion

# Registered adapter names (dialects).
CHAT_COMPLETIONS = "chat_completions"
RESPONSES = "responses"


@dataclass
class UpstreamConfig:
    """Resolved upstream connection for one lobe."""

    base_url: str
    api_key: str
    model: str
    dialect: str = CHAT_COMPLETIONS
    extra: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_env(cls, prefix: str, default_model: str, default_base: str, default_key: str) -> "UpstreamConfig":
        def _env(name: str, default: str = "") -> str:
            return os.getenv(name, default) or default

        return cls(
            base_url=_env(f"{prefix}_BASE_URL", default_base),
            api_key=_env(f"{prefix}_API_KEY", default_key),
            model=_env(f"{prefix}_MODEL", default_model),
            dialect=_env(f"{prefix}_DIALECT", CHAT_COMPLETIONS),
        )


@dataclass
class NormalizedRequest:
    """Provider-independent conversation/tool request."""

    messages: list[dict[str, Any]]
    temperature: float | None = None
    max_tokens: int | None = None
    top_p: float | None = None
    stop: Any = None
    tools: list[dict[str, Any]] | None = None
    tool_choice: Any = None
    stream: bool = False
    response_format: Any = None
    seed: Any = None
    timeout: float | None = None
    reasoning_effort: Any = None

    def to_kwargs(self) -> dict[str, Any]:
        out: dict[str, Any] = {"messages": self.messages}
        for k in ("temperature", "max_tokens", "top_p", "stop", "tools", "tool_choice",
                  "response_format", "seed", "timeout", "reasoning_effort"):
            v = getattr(self, k)
            if v is not None:
                out[k] = v
        return out


BLOCKED_UNIFIED_KWARGS = {"api_base", "api_key", "base_url", "custom_llm_provider"}


class ChatCompletionsAdapter:
    """Routes to any OpenAI-compatible completion endpoint (e.g. Featherless)."""

    dialect = CHAT_COMPLETIONS

    def __init__(self, cfg: UpstreamConfig) -> None:
        self.cfg = cfg

    def complete(self, req: NormalizedRequest) -> Any:
        kwargs = req.to_kwargs()
        # openai/ prefix + explicit api_base/api_key keeps litellm on the
        # OpenAI-compatible path regardless of the bare model id's provider hint.
        return completion(
            model=f"openai/{self.cfg.model}",
            api_base=self.cfg.base_url,
            api_key=self.cfg.api_key,
            **kwargs,
        )


class ResponsesAdapter:
    """Guarded stub for the OpenCode Zen Responses dialect (not yet validated).

    Present so provider routing is data-driven (``dialect``), but raises loudly
    rather than guessing until a real OpenCode Zen connection is configured and
    the translation is verified end-to-end.
    """

    dialect = RESPONSES

    def __init__(self, cfg: UpstreamConfig) -> None:
        self.cfg = cfg

    def complete(self, req: NormalizedRequest) -> Any:
        raise NotImplementedError(
            "OpenCode Zen Responses adapter is not validated in this project yet. "
            "Refusing to guess. Configure an OpenAI-compatible dialect, or add and "
            "verify the /zen/v1/responses translation first."
        )


def make_adapter(cfg: UpstreamConfig):
    if cfg.dialect == RESPONSES:
        return ResponsesAdapter(cfg)
    return ChatCompletionsAdapter(cfg)


def resolve_request(payload: dict[str, Any]) -> NormalizedRequest:
    """Extract a normalized request from the Worker-facing Chat Completions body."""
    allowed = {
        "temperature", "top_p", "max_tokens", "stop", "presence_penalty",
        "frequency_penalty", "tools", "tool_choice", "response_format", "seed",
        "timeout", "reasoning_effort",
    }
    kwargs = {k: payload[k] for k in allowed if payload.get(k) is not None}
    return NormalizedRequest(
        messages=payload.get("messages") or [],
        stream=bool(payload.get("stream", False)),
        **kwargs,
    )
