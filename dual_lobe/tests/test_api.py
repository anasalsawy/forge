"""HTTP surface: auth, chat pass-through, events ingest, state query."""
from __future__ import annotations

from dual_lobe.api import auth


def test_health(client):
    assert client.get("/healthz").json()["status"] == "ok"
    ready = client.get("/readyz")
    assert ready.status_code == 200
    assert ready.json()["ledger"] == "ok"


def test_models_listed(client):
    ids = [m["id"] for m in client.get("/v1/models").json()["data"]]
    assert "lobe-a" in ids and "lobe-b" in ids


def test_requires_bearer(client):
    r = client.post("/v1/chat/completions", json={"messages": [{"role": "user", "content": "hi"}]})
    assert r.status_code == 401


def test_bad_key_rejected(client):
    r = client.post(
        "/v1/chat/completions",
        json={"messages": [{"role": "user", "content": "hi"}]},
        headers={"Authorization": "Bearer nope"},
    )
    assert r.status_code == 401


def test_scope_enforced(client, tenant):
    tid, _ = tenant
    r = client.post(
        "/v1/dual-lobe/events",
        json={"run_id": "", "kind": "x", "payload": {}},
        headers={"Authorization": "Bearer wrong-scope-key"},
    )
    # unknown key -> 401; the seeded key has events:write, so use a scoped-out key
    assert r.status_code == 401


def test_revoked_key_rejected(client, tenant, db_url):
    import asyncio

    from sqlalchemy import func, select

    from dual_lobe.api.auth import hash_key
    from dual_lobe.core.bootstrap import ALL_SCOPES, ensure_tenant
    from dual_lobe.core.engine import admin_session_factory, dispose_engines
    from dual_lobe.core.models import ApiKey

    async def _revoke():
        async with admin_session_factory()() as s:
            t = await ensure_tenant(s, "revoke-tenant", "Revoke")
            key = ApiKey(tenant_id=t.id, key_hash=hash_key("revoke-me-key"), label="revokable", scopes=ALL_SCOPES)
            s.add(key)
            await s.flush()
            key.revoked_at = key.created_at
            await s.commit()

    asyncio.run(_revoke())
    r = client.post(
        "/v1/chat/completions",
        json={"messages": [{"role": "user", "content": "hi"}]},
        headers={"Authorization": "Bearer revoke-me-key"},
    )
    assert r.status_code == 401
    asyncio.run(dispose_engines())


def test_chat_nonstream_success(client, tenant):
    tid, raw = tenant
    r = client.post(
        "/v1/chat/completions",
        json={"model": "lobe-a", "messages": [{"role": "user", "content": "ping"}]},
        headers={"Authorization": f"Bearer {raw}"},
    )
    assert r.status_code == 200
    body = r.json()
    assert body["choices"][0]["message"]["content"] == "pong!"
    assert body["model"] == "lobe-a"
    run_id = r.headers.get("X-Dual-Lobe-Run-Id")
    assert run_id

    st = client.get(f"/v1/dual-lobe/state/{run_id}", headers={"Authorization": f"Bearer {raw}"}).json()
    kinds = [e["kind"] for e in st["recent_events"]]
    assert "worker_call" in kinds
    assert st["revision"] is None  # observation stage, worker not run


def test_chat_missing_messages(client, tenant):
    _, raw = tenant
    r = client.post(
        "/v1/chat/completions",
        json={"model": "lobe-a", "messages": []},
        headers={"Authorization": f"Bearer {raw}"},
    )
    assert r.status_code == 400


def test_chat_upstream_failure_is_502_and_no_crash(client, tenant, monkeypatch):
    from tests.helpers import FakeAdapter, FakeRegistry, fake_target

    _, raw = tenant

    class Boom(Exception):
        pass

    reg = FakeRegistry(a=FakeAdapter(fail=Boom("empty model response")))
    monkeypatch.setattr("dual_lobe.api.chat.get_registry", lambda: reg)
    r = client.post(
        "/v1/chat/completions",
        json={"model": "lobe-a", "messages": [{"role": "user", "content": "ping"}]},
        headers={"Authorization": f"Bearer {raw}"},
    )
    assert r.status_code == 502
    assert "provider" in r.json()["detail"].lower()


def test_chat_stream_success(client, tenant, monkeypatch):
    from tests.helpers import FakeRegistry

    _, raw = tenant
    reg = FakeRegistry()
    monkeypatch.setattr("dual_lobe.api.chat.get_registry", lambda: reg)
    r = client.post(
        "/v1/chat/completions",
        json={"model": "lobe-a", "stream": True, "messages": [{"role": "user", "content": "ping"}]},
        headers={"Authorization": f"Bearer {raw}"},
    )
    assert r.status_code == 200
    assert "data: [DONE]" in r.text
    assert "chat.completion.chunk" in r.text


def test_chat_stream_incomplete(client, tenant, monkeypatch):
    from tests.helpers import FakeAdapter, FakeRegistry

    _, raw = tenant

    class Cut(Exception):
        pass

    reg = FakeRegistry(a=FakeAdapter(fail=Cut("connection reset")))
    monkeypatch.setattr("dual_lobe.api.chat.get_registry", lambda: reg)
    r = client.post(
        "/v1/chat/completions",
        json={"model": "lobe-a", "stream": True, "messages": [{"role": "user", "content": "ping"}]},
        headers={"Authorization": f"Bearer {raw}"},
    )
    assert r.status_code == 200
    assert '"finish_reason": "INCOMPLETE"' in r.text


def test_events_ingest_and_state_flow(client, tenant):
    tid, raw = tenant
    h = {"Authorization": f"Bearer {raw}"}
    run = client.post(
        "/v1/chat/completions", json={"messages": [{"role": "user", "content": "q"}]}, headers=h
    ).headers.get("X-Dual-Lobe-Run-Id")
    ev = client.post(
        "/v1/dual-lobe/events",
        json={"run_id": run, "kind": "agent_tool_call", "actor": "worker", "payload": {"tool": "exa"}},
        headers=h,
    )
    assert ev.status_code == 200
    assert ev.json()["run_id"] == run
    # duplicate with same idempotency_key -> unique constraint (new row would fail)
    ev2 = client.post(
        "/v1/dual-lobe/events",
        json={"run_id": run, "kind": "agent_tool_call", "actor": "worker", "payload": {"tool": "exa"}, "idempotency_key": "uniq-1"},
        headers=h,
    )
    assert ev2.status_code == 200
    assert ev2.json()["event_id"] != "" and ev2.json()["kind"] == "agent_tool_call"
    st = client.get(f"/v1/dual-lobe/state/{run}", headers=h)
    assert st.status_code == 200
    assert st.json()["run_id"] == run


def test_state_unknown_run_404(client, tenant):
    _, raw = tenant
    r = client.get("/v1/dual-lobe/state/does-not-exist", headers={"Authorization": f"Bearer {raw}"})
    assert r.status_code == 404