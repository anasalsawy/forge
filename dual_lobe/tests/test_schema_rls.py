"""Schema + RLS isolation on a fresh Postgres via Testcontainers."""
from __future__ import annotations

import asyncio

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine


@pytest.fixture(scope="module")
def admin_engine_conn(db_url):
    engine = create_async_engine(db_url)
    yield engine
    asyncio.run(engine.dispose())


async def test_migration_applied_with_tables_and_rls_role(admin_engine_conn):
    async with admin_engine_conn.connect() as conn:
        tables = (await conn.execute(text("SELECT tablename FROM pg_tables WHERE schemaname='public'"))).scalars().all()
        expected = {
            "tenants", "api_keys", "runs", "provider_attempts", "events", "claims",
            "evidence", "b_state", "b_jobs", "outbox", "provider_registry",
        }
        assert expected.issubset(set(tables))
        roles = (await conn.execute(text("SELECT rolname FROM pg_roles WHERE rolname='dual_lobe_rls'"))).scalars().all()
        assert roles == ["dual_lobe_rls"]
        n_policies = (await conn.execute(text("SELECT count(*) FROM pg_policies WHERE tablename IN ('tenants','events','outbox','b_jobs')"))).scalar_one()
        assert n_policies >= 4
        seq = (await conn.execute(text("SELECT last_value FROM event_seq"))).scalar_one()
        assert isinstance(seq, int)


def test_rls_isolates_tenant_data(db_url):
    """As dual_lobe_rls: no GUC -> nothing; GUC matches -> rows; wrong-tenant insert blocked."""
    from sqlalchemy import text
    from sqlalchemy.engine import make_url
    from sqlalchemy.ext.asyncio import create_async_engine

    from dual_lobe.core.bootstrap import ensure_tenant
    from dual_lobe.core.engine import admin_session_factory, dispose_engines
    from dual_lobe.state import repositories as repo

    async def _run():
        async with admin_session_factory()() as s:
            t = await ensure_tenant(s, "iso-tenant", "ISO")
            run = await repo.get_or_create_run(s, t.id, "iso-run-x")
            run_id = str(run.id)
            await repo.append_event(s, "mk", t.id, run_id=run_id, actor="test", payload={"k": 1})
            await s.commit()

        url = make_url(db_url)
        rls_url = (
            f"postgresql+asyncpg://dual_lobe_rls:dual_lobe_rls@{url.host}:{url.port}/{url.database}"
        )
        engine = create_async_engine(rls_url)
        try:
            # 1) RLS session without GUC sees nothing.
            async with engine.connect() as conn:
                n_none = (await conn.execute(text("SELECT count(*) FROM runs"))).scalar_one()
                assert n_none == 0
            # 2) GUC set -> sees own tenant's rows only.
            async with engine.connect() as conn:
                await conn.execute(text(f"SET app.tenant_id = {int(t.id)}"))
                n_own = (await conn.execute(text("SELECT count(*) FROM events"))).scalar_one()
                assert n_own == 1
                # 3) WITH CHECK blocks cross-tenant insert.
                with pytest.raises(Exception):
                    await conn.execute(
                        text("INSERT INTO events (tenant_id, run_id, kind) VALUES (999, NULL, 'x')")
                    )
        finally:
            await engine.dispose()
        await dispose_engines()

    asyncio.run(_run())