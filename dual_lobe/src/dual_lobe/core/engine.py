"""Async engine factories for the admin (owner) and tenant-scoped (RLS) roles."""
from __future__ import annotations

import contextlib
import os

from sqlalchemy import text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.pool import NullPool

from .settings import get_settings

_admin_engine: AsyncEngine | None = None
_rls_engine: AsyncEngine | None = None
_admin_factory: async_sessionmaker[AsyncSession] | None = None
_rls_factory: async_sessionmaker[AsyncSession] | None = None

# Tests create/destroy engines across independent event loops (asyncio.run per
# test). NullPool keeps connections from being reused across loops there.
if os.environ.get("DUAL_LOBE_TESTING") == "1":
    _POOL: object = NullPool
else:  # pragma: no cover - production path
    _POOL = None


def admin_engine() -> AsyncEngine:
    global _admin_engine
    if _admin_engine is None:
        _admin_engine = create_async_engine(
            get_settings().database_url,
            pool_pre_ping=True,
            poolclass=_POOL,
        )
    return _admin_engine


def rls_engine() -> AsyncEngine:
    global _rls_engine
    if _rls_engine is None:
        _rls_engine = create_async_engine(
            get_settings().rls_url, pool_pre_ping=True, poolclass=_POOL
        )
    return _rls_engine


def admin_session_factory() -> async_sessionmaker[AsyncSession]:
    global _admin_factory
    if _admin_factory is None:
        _admin_factory = async_sessionmaker(admin_engine(), expire_on_commit=False)
    return _admin_factory


def rls_session_factory() -> async_sessionmaker[AsyncSession]:
    global _rls_factory
    if _rls_factory is None:
        _rls_factory = async_sessionmaker(rls_engine(), expire_on_commit=False)
    return _rls_factory


@contextlib.asynccontextmanager
async def tenant_session(tenant_id: int):
    """Yield an RLS-enforced session bound to ``tenant_id`` via the GUC."""
    factory = rls_session_factory()
    async with factory() as session:
        await session.execute(text(f"SET app.tenant_id = {int(tenant_id)}"))
        await session.commit()
        try:
            yield session
        finally:
            pass


async def dispose_engines() -> None:
    global _admin_engine, _rls_engine, _admin_factory, _rls_factory
    for eng in (_admin_engine, _rls_engine):
        if eng is not None:
            await eng.dispose()
    _admin_engine = _rls_engine = None
    _admin_factory = _rls_factory = None