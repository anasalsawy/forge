# Deep Investigator Build Pipeline

This project defines a CrewAI declarative Flow in `src/deep_investigator_build_pipeline/flow_direct.json`.

## Architecture

- 6 research floors (Worker + Floor Analyst)
- Research Master Record compiler on Research Floor 6
- Human APPROVE / REJECT / RERUN checkpoint
- 6 build floors (Worker + Floor Analyst)
- Final Auditor on Build Floor 6
- Deterministic PASS / FAIL router to completion or remediation

## Floor-to-floor gates

Every floor is separated by a deterministic router gate. A floor only advances
when its analyst verdict passes:

- Research: `research_floor_N -> research_gate_N -> research_pass_N -> research_floor_{N+1}`; analyst must emit `FLOOR PASSED` or `PASSED WITH NOTES`.
- Human checkpoint: only `APPROVE` proceeds to the build phase.
- Build: `build_floor_N -> build_gate_N -> build_pass_N -> build_floor_{N+1}`.
- Any gate failure routes to `remediation_notice` and stops the pipeline.

Workers and the research compiler bind to Gemini direct; analysts and the final
auditor bind to GLM (featherless). API keys are injected from environment
variables at kickoff time (see `main.py`), never baked into the definition.

## Install

```sh
crewai install
```

## Run

```sh
crewai run
```

## Cumulative Full-Scope Floor Semantics

The six research floors and six build floors are an accumulating escalation chain, not six parallel partial attempts.

- Floor 1 establishes the first complete working state.
- Floor N (2-6) explicitly receives the full raw output of Floor N-1 as `previous_handoff`.
- Every floor owns the entire problem from all relevant aspects; there are no floor-specific research or implementation scopes.
- A downstream worker must continue from inherited work, independently verify it, preserve supported work, correct defects, close gaps, and return a new cumulative handoff representing the best complete state of the whole job.
- Workers are explicitly forbidden from assuming a later floor will patch omissions.
- Analysts judge the complete cumulative state, not merely the current floor's delta.

Research chain: `R1 -> [gate] R2 -> [gate] R3 -> ... -> R6 -> [gate] Master Record -> [human] Build`.

Build chain: `B1 -> [gate] B2 -> [gate] B3 -> ... -> B6 -> Final Auditor -> PASS/FAIL`.