# dual_lobe — Inference Proxy with a Deep Verification "B-Lobe"

`dual_lobe` is the inference gateway that powers the deployed Forge project
(a multi-floor research/builder pipeline). It exposes a single
OpenAI-compatible API in front of upstream model providers (Gemini via the
Google OpenAI-compatible endpoint, Featherless, etc.), and pairs every call
with a **durable, asynchronous verification pipeline** so that no high-stakes
output runs "unanchored".

The name comes from the two-lobe architecture:

- **A-Lobe (hot path):** serve the request fast, stream or return the model
  response, hand the run off.
- **B-Lobe (verification path):** after the fact, capture a *context shadow*
  of what the model was given (the observation floor at that decision point),
  run deep faithfulness / evidential checks against the response, classify the
  outcome with a disposition + severity, and persist a versioned verdict the
  caller can inspect.

Nothing Blies-blocking: failures on the B path are **fail-open** by design.
The A-path never waits on verification; it commits its work transactionally and
moves on.

---

## What it does, exactly

### 1. OpenAI-compatible inference API

- `GET  /v1/models` — list configured models.
- `POST /v1/chat/completions` — non-streaming and streaming (SSE) chat, requests
  run against the configured upstream provider via per-tenant model routing.
- Every run gets a **run id** (`X-Dual-Lobe-Run-Id` header), an idempotency gate,
  and a durable trail of events: `run_started`, `context_ingested`,
  `provider_attempt` (with per-attempt status and latency), `worker_error`.

### 2. Context Shadow + Integrity Sentinel (the B-Lobe)

For a run that matters (flagged call sequences), the worker:

1. **Reads the shadow record** — the exact context handed to the model at that
   decision floor (`observation`), plus the model's response and per-floor state.
2. **Builds a verification prompt** that asks a stronger/grounding model to check
   the response against the evidence, flags fabricated URLs, distinguishes
   observation from inference, and reports contradictions.
3. **Writes a verifiable claim** (`b_claim`) that is either grounded (file-backed,
   so a file-exists verifier can prove it) or left as an explicit `claimed` item.
4. **Classifies the outcome** into a disposition with a severity weight:
   corrected handoffs (`corrected_handoff`), suppressed writes
   (`write_suppression`), mitigated outputs (`mitigation`), or a
   `shadow_degraded` marker when the verifier could not run (fail-open).
5. **Versions its verdict** in `b_state.rev` + `verdict_status`, so callers can
   read `status_code == REVIEWABLE` and the exact disposition.

### 3. Durable, correct execution

- **Transactional outbox:** every workflow trigger enqueues an outbox row inside
  the **same transaction** as the A-path commit. A durable job is *never* lost
  to a crash between "model returned" and "verification enqueued".
- **Deterministic job keys + `b_job` dedup:** the same event enqueued twice
  produces one job (`unique(event_key)`), so redelivery is idempotent.
- **Claim-free job steering:** workers claim only jobs they can complete; a claim
  stays open while work proceeds and is closed when `b_state` is written.
- **Versioned state:** each B verdict bumps `rev`; the pipeline can see
  "revision 1 = still claimed, revision 2 = corrected handoff" and reconcile.

### 4. Multi-tenant with Row-Level Security

- Tenants authenticate with scoped API keys (`inference:invoke`,
  `events:write`, `state:read`, etc.). Keys are stored **hashed** (SHA-256).
- Postgres is the source of truth. The API connects through two roles:
  - `DATABASE_URL` — privileged role used by workers/migrations/auth.
  - `RLS_DATABASE_URL` — `dual_lobe_rls` role; every request sets
    `SET app.tenant_id = <id>`, and **every row is invisible unless your
    tenant header matches** (RLS policies + `WITH CHECK` on inserts).
- Reading another tenant's state returns zero rows; writing to another
  tenant's rows is rejected by the database itself — never by application code.

### 5. Observability

- `/healthz` (liveness) and `/readyz` (DB+Redis readiness) for orchestration.
- Structlog correlation IDs across gateway and worker.
- `POST /v1/dual-lobe/events` for ingest of client-observed events.
- `GET  /v1/dual-lobe/state/{run_id}` — full event trail + B-verdicts.
- `POST /v1/verify` — on-demand evidence verification against a caller-supplied
  artifact path.

---

## Architecture

```
 client
   │  OpenAI-compatible calls (Bearer <scoped api key>)
   ▼
┌──────────────────────────────────────────────────────────────┐
│ gateway (FastAPI)                                           │
│  auth(api_keys) → push to provider (A-lobe)                 │
│  → outbox row in SAME tx → 200/stream to client             │
└──────────────┬───────────────────────────────────────────────┘
               │ transactional outbox (Postgres)
               ▼
┌──────────────────────────────────────────────────────────────┐
│ b-worker                                                      │
│  claim job → context shadow → integrity checks → classify    │
│  → write b_state (verdict + disposition + severity)          │
│  fail-open: shadow_degraded if verification can't run        │
└───────────────────────────────────────────────────────────────┘
```

- **gateway** — FastAPI app, auth, limits (Redis-backed), routing, SSE.
- **b-worker** — async loop polling the outbox, deduping via `event_key`,
  running the B-pipeline.
- **db** — Postgres 18; source of truth for runs, events, outbox, jobs,
  claims, evidence, b_state, tenants, api_keys. RLS enforced via
  `dual_lobe_rls` role.
- **redis** — rate-limit / coordination state (valkey image).

---

## Running it

```bash
cp .env.example .env        # fill DUAL_LOBE_A_API_KEY, GEMINI_API_KEY, etc.
docker compose up -d --build gateway b-worker
curl localhost:8811/healthz
```

Secrets needed per provider you route to (`DUAL_LOBE_A_API_KEY`,
`GEMINI_API_KEY`, Featherless/`OPENAI_API_KEY`). The bootstrap seeds a default
tenant + key; override with `DUAL_LOBE_BOOTSTRAP_KEYS="<key>|<scope1,scope2>|<slug>"`.

Database is migrated automatically on start (Alembic `0001_initial` creates all
tables, the `dual_lobe_rls` role, grants, and RLS policies).

---

## Tests

Integration tests run against a real Postgres 18 (Testcontainers), no mocks for
the storage layer:

```bash
uv run pytest   # 20 tests: schema+RLS isolation, auth/scopes,
                # chat (stream/non-stream/INCOMPLETE/502), events+state,
                # outbox idempotency, worker cycle, fail-open, verifier
```

---

## Repository layout

```
alembic/            migrations (schema, roles, RLS policies)
docker/             Dockerfile + entrypoint (migrate → seed → run)
src/dual_lobe/
  api/              FastAPI app: chat, events, state, health, verify, auth
  b/                B-lobe: prompts, outbox, context_shadow, worker
  core/             settings, engine (admin+RLS sessions), models, bootstrap
  evidence/         verifier (file-grounding checks), classifier (dispositions)
  provider/         upstream adapters (buffered + streaming) + registry
  state/            async repositories over the Postgres schema
  obs/              structlog + tracer setup
tests/              conftest (Testcontainers PG18) + integration suites
```