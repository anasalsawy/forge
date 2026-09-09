# Dual-Lobe Runtime

This project keeps the original six research floors, human checkpoint, six build floors, analysts, compiler, final auditor, and final router.

Only **Worker LLM calls** are rerouted through a local OpenAI-compatible gateway:

- **Lobe A** — the existing CrewAI Worker. Its upstream model defaults to `sambanova/Meta-Llama-3.3-70B-Instruct`.
- **Lobe B** — an independent shadow LLM call running outside the CrewAI graph.
- **Shared state** — `.dual_lobe/state.json` maintained by the gateway.
- **Injector** — inserts only distilled facts/constraints/questions into a later Worker LLM call. It never identifies another lobe or reviewer.

## Runtime behavior

For each Worker model call:

1. The gateway loads the current distilled shared-state injection.
2. It starts Lobe B's **pre-pass concurrently** with Lobe A. The pre-pass broadens context, asks unasked questions, identifies assumptions/blockers, and optionally searches via Firecrawl.
3. Lobe A receives its normal response without waiting for the post-audit.
4. A background **post-pass** evaluates A's output for evidence integrity, blockers, unsupported completion, and state corrections.
5. The post-pass updates shared state. A later Worker call receives only the relevant distilled context.

Analysts and the final auditor still use their original direct model connection and remain explicit sequential quality gates.

## Run

Use the bundled launcher so the gateway and CrewAI flow run together:

Windows:

```bat
run_dual_lobe.bat
```

Linux/macOS:

```sh
./run_dual_lobe.sh
```

Or run the pieces manually:

```sh
python -m deep_investigator_build_pipeline.dual_lobe_gateway
crewai run
```

## Environment

The provider key required by the upstream model must still be set normally (for the default A/B model, the SambaNova key expected by LiteLLM).

Optional controls:

```text
DUAL_LOBE_A_MODEL=sambanova/Meta-Llama-3.3-70B-Instruct
DUAL_LOBE_B_MODEL=sambanova/Meta-Llama-3.3-70B-Instruct
DUAL_LOBE_PORT=8765
DUAL_LOBE_SEARCH=1
DUAL_LOBE_MAX_SEARCH_QUERIES=3
DUAL_LOBE_MAX_INJECTION_CHARS=7000
DUAL_LOBE_PERSIST=0
DUAL_LOBE_LOG_LEVEL=INFO
```

If `FIRECRAWL_API_KEY` is present, B can execute its own compact search pass for context broadening. If it is absent, B still performs independent model-based broadening and integrity analysis.

Set `DUAL_LOBE_B_MODEL` to a different provider/model if you want stronger epistemic independence from Lobe A.

## Important state semantics

B distinguishes the active Worker's claim from the system's effective evidence state. A claimed success can therefore generate a later injected precondition such as:

> Deployment remains pending until a live endpoint response establishes success.

The Worker is not told that this came from another agent. The context is inserted as ordinary execution state.

## Interaction with cumulative floors

Lobe B augments the accumulating handoff chain; it does not replace it. Each active Worker receives the previous floor's explicit cumulative handoff through CrewAI plus selectively injected shadow context through the dual-lobe gateway. The Worker is always instructed to solve the whole task, not a floor-specific slice.

