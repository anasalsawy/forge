#!/usr/bin/env python
"""Deployable CrewAI entry point for the Deep Investigator build pipeline.

This module is the ``main.py`` entry point expected by `crewai deploy`. It
wraps the declarative CrewAI flow in `flow_direct.json` — the cloud-viable
variant that drives workers straight at their upstream providers instead of
the local dual-lobe gateway (which only exists on the operator's box).

API keys are injected at kickoff time from environment variables, never
baked into the flow definition:
  - workers (Gemini upstream): DUAL_LOBE_A_API_KEY (fallback GOOGLE_API_KEY)
  - analysts/auditors (GLM upstream): OPENAI_API_KEY
"""

from __future__ import annotations

import json
import os
from pathlib import Path

from crewai.flow.flow import Flow

HERE = Path(__file__).resolve().parent
FLOW_FILE = HERE / "flow_direct.json"

DEFAULT_PROMPT = (
    "Build a real-time voice + text conversational computer-use agent for "
    "Windows, robust and low-latency, ready to use out of the box."
)


def _inject_api_keys(data: dict) -> dict:
    """Fill llm.api_key placeholders from the environment for cloud deploy."""
    for method in data.get("methods", {}).values():
        agents = method.get("do", {}).get("with", {}).get("agents", {})
        for agent in agents.values():
            llm = agent.get("llm")
            if not isinstance(llm, dict):
                continue
            base = llm.get("base_url") or ""
            if "generativelanguage" in base:
                llm["api_key"] = (
                    os.getenv("DUAL_LOBE_A_API_KEY")
                    or os.getenv("GOOGLE_API_KEY")
                    or llm.get("api_key")
                    or ""
                )
            elif "featherless" in base:
                llm["api_key"] = (
                    os.getenv("OPENAI_API_KEY")
                    or llm.get("api_key")
                    or ""
                )
    return data


def _load_definition() -> dict:
    with Path(FLOW_FILE).open(encoding="utf-8") as fh:
        return _inject_api_keys(json.load(fh))


class DeepInvestigatorBuildPipeline(Flow):
    """The Deep Investigator flow, built from the declarative definition."""

    def __new__(cls, *args, **kwargs):  # noqa: ARG003
        return Flow.from_declaration(contents=_load_definition())


def _resolve_inputs(payload: dict | None) -> dict:
    p = payload or {}
    inputs: dict = {"prompt": "", "directive": ""}
    for key in ("prompt", "directive"):
        value = p.get(key)
        if not value:
            value = os.getenv(f"RUNTIME_RUN_{key.upper()}", "")
        inputs[key] = value
    if not inputs["prompt"]:
        inputs["prompt"] = DEFAULT_PROMPT
    return {k: v for k, v in inputs.items() if v}


def kickoff(payload: dict | None = None):
    """Run the flow from a trigger payload (or environment)."""
    flow = DeepInvestigatorBuildPipeline()
    flow.kickoff(inputs=_resolve_inputs(payload))


def plot():
    flow = DeepInvestigatorBuildPipeline()
    flow.plot("DeepInvestigatorBuildPipeline")


if __name__ == "__main__":
    kickoff()