"""Provider adapters. The gateway never assumes every upstream speaks
``/v1/chat/completions``; an adapter turns a normalized request into the
upstream dialect and back into an OpenAI-compatible shape.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, AsyncIterator

from litellm import completion

CHAT_COMPLETIONS = "chat_completions"
RESPONSES = "responses"

BLOCKED_UNIFIED_KWARGS = {"api_base", "api_key", "base_url", "custom_llm_provider"}


@dataclass
class ProviderTarget:
    """Resolved upstream connection for one lobe / logical alias."""

    alias: str
    base_url: str
    api_key: str
    model: str
    kind: str = CHAT_COMPLETIONS
    capabilities: dict[str, Any] = field(default_factory=dict)
    enabled: bool = True

    def supports(self, feature: str) -> bool:
        if not self.enabled:
            return False
        if feature not in self.capabilities:
            return True
        return bool(self.capabilities[feature])


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
        for k in (
            "temperature",
            "max_tokens",
            "top_p",
            "stop",
            "tools",
            "tool_choice",
            "response_format",
            "seed",
            "timeout",
            "reasoning_effort",
        ):
            v = getattr(self, k)
            if v is not None:
                out[k] = v
        return out


class ChatCompletionsAdapter:
    dialect = CHAT_COMPLETIONS

    def __init__(self, target: ProviderTarget) -> None:
        self.target = target

    def _base_kwargs(self, req: NormalizedRequest) -> dict[str, Any]:
        return {
            "model": f"openai/{self.target.model}",
            "api_base": self.target.base_url,
            "api_key": self.target.api_key,
            **req.to_kwargs(),
        }

    async def buffered(self, req: NormalizedRequest):
        import asyncio

        return await asyncio.to_thread(completion, **self._base_kwargs(req))

    async def stream(self, req: NormalizedRequest) -> AsyncIterator[dict[str, Any]]:
        """Yield normalized delta chunks from a litellm streaming completion.

        Each yield is {"delta": {...}, "finish_reason": str|None}. If the upstream
        collapses to a buffered response, a single terminal delta is produced.
        """
        import asyncio

        kwargs = {**self._base_kwargs(req), "stream": True}
        gen = await asyncio.to_thread(completion, **kwargs)
        finish = None
        for chunk in gen:
            try:
                choices = chunk.choices or []
                if not choices:
                    continue
                choice = choices[0]
                delta = getattr(choice, "delta", None)
                f = getattr(choice, "finish_reason", None)
                d: dict[str, Any] = {}
                content = getattr(delta, "content", None)
                if content:
                    d["content"] = content
                tc = getattr(delta, "tool_calls", None)
                if tc:
                    d["tool_calls"] = [tc[0].__dict__ if not isinstance(tc[0], dict) else tc[0]]
                if delta and (getattr(delta, "role", None) or d):
                    if d or getattr(delta, "role", None):
                        yield {"delta": d, "finish_reason": f}
                finish = f
            except Exception:
                continue
        yield {"delta": {}, "finish_reason": finish or "stop"}


class ResponsesAdapter:
    """Guarded stub for the OpenCode Zen Responses dialect (not yet validated)."""

    dialect = RESPONSES

    def __init__(self, target: ProviderTarget) -> None:
        self.target = target

    async def buffered(self, req: NormalizedRequest):
        raise NotImplementedError(
            "OpenCode Zen Responses adapter is not validated in this project yet. "
            "Refusing to guess. Configure an OpenAI-compatible dialect, or add and "
            "verify the /zen/v1/responses translation first."
        )

    async def stream(self, req: NormalizedRequest) -> AsyncIterator[dict[str, Any]]:
        raise NotImplementedError(
            "OpenCode Zen Responses adapter is not validated in this project yet."
        )


def make_adapter(target: ProviderTarget):
    if target.kind == RESPONSES:
        return ResponsesAdapter(target)
    return ChatCompletionsAdapter(target)


def resolve_request(payload: dict[str, Any]) -> NormalizedRequest:
    """Extract a normalized request from the Worker-facing Chat Completions body."""
    allowed = {
        "temperature",
        "top_p",
        "max_tokens",
        "stop",
        "presence_penalty",
        "frequency_penalty",
        "tools",
        "tool_choice",
        "response_format",
        "seed",
        "timeout",
        "reasoning_effort",
    }
    kwargs = {k: payload[k] for k in allowed if payload.get(k) is not None}
    return NormalizedRequest(
        messages=payload.get("messages") or [],
        stream=bool(payload.get("stream", False)),
        **kwargs,
    )