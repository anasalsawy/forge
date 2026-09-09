import os

import pytest

# Env must be set before any dual_lobe import (get_settings / engine poolclass are read at import).
os.environ.setdefault("DUAL_LOBE_TESTING", "1")
os.environ.setdefault("REDIS_URL", "redis://localhost:6379/0")
os.environ.setdefault("DUAL_LOBE_A_API_KEY", "test-openai-key")
os.environ.setdefault("DUAL_LOBE_A_MODEL", "fake-model")
os.environ.setdefault("DUAL_LOBE_A_BASE_URL", "https://fake.local/v1")
os.environ.setdefault("DUAL_LOBE_ROLLOUT_STAGE", "observation")
os.environ.setdefault("DUAL_LOBE_PULSE_EVERY", "3")

from testcontainers.postgres import PostgresContainer  # noqa: E402

from dual_lobe.core.settings import get_settings  # noqa: E402


@pytest.fixture(scope="session", autouse=True)
def postgres() -> PostgresContainer:
    with PostgresContainer("postgres:18") as pg:
        host, port = pg.get_container_host_ip(), pg.get_exposed_port(5432)
        user, password, db = pg.username, pg.password, pg.dbname
        os.environ["DATABASE_URL"] = f"postgresql+asyncpg://{user}:{password}@{host}:{port}/{db}"
        get_settings.cache_clear()
        _apply_migrations()
        yield pg


def _apply_migrations() -> None:
    from alembic import command
    from alembic.config import Config

    cfg = Config("alembic.ini")
    cfg.set_main_option("script_location", "alembic")
    command.upgrade(cfg, "head")


@pytest.fixture(scope="session")
def db_url() -> str:
    return os.environ["DATABASE_URL"]


@pytest.fixture
def fake_target_and_registry(monkeypatch):
    from tests.helpers import FakeAdapter, FakeRegistry

    reg = FakeRegistry()
    monkeypatch.setattr("dual_lobe.api.chat.get_registry", lambda: reg)
    return reg


@pytest.fixture
def tenant(db_url):
    """Seeded tenant + raw API key (all scopes). Returns (tenant_slug, raw_key)."""
    from sqlalchemy import func, select

    from dual_lobe.api.auth import hash_key
    from dual_lobe.core.bootstrap import ALL_SCOPES, ensure_tenant
    from dual_lobe.core.engine import admin_session_factory, dispose_engines
    from dual_lobe.core.models import ApiKey

    async def _seed():
        async with admin_session_factory()() as session:
            tenant = await ensure_tenant(session, "koko", "Koko Test")
            raw = "koko-key"
            n = (await session.execute(select(func.count()).select_from(ApiKey).where(ApiKey.key_hash == hash_key(raw)))).scalar_one()
            if not n:
                session.add(ApiKey(tenant_id=tenant.id, key_hash=hash_key(raw), label="test", scopes=ALL_SCOPES))
                await session.commit()
            return tenant.id, raw

    import asyncio

    tid, raw = asyncio.run(_seed())
    yield tid, raw
    asyncio.run(dispose_engines())


@pytest.fixture
def client(tenant, fake_target_and_registry):
    from fastapi.testclient import TestClient

    from dual_lobe.api.app import app

    with TestClient(app) as c:
        yield c