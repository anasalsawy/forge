"""Seed a tenant + scoped API keys from ``DUAL_LOBE_BOOTSTRAP_KEYS``.

Format (``;``-separated): ``<raw-key>|<scope1,scope2>|<tenant-slug>``.
A default ``dual-lobe-local`` admin key for the ``default`` tenant is created
when bootstrap runs and no key exists yet.
"""
from __future__ import annotations

import asyncio
from sqlalchemy import select, func

from .engine import admin_session_factory, dispose_engines
from .models import ApiKey, Tenant
from .settings import get_settings
from ..api.auth import SCOPE_INFERENCE_INVOKE, SCOPE_EVENTS_WRITE, SCOPE_STATE_READ, SCOPE_RUNS_ADMIN, SCOPE_PROVIDERS_ADMIN, SCOPE_STATE_DEBUG, hash_key, new_key

ALL_SCOPES = [SCOPE_INFERENCE_INVOKE, SCOPE_EVENTS_WRITE, SCOPE_STATE_READ, SCOPE_STATE_DEBUG, SCOPE_RUNS_ADMIN, SCOPE_PROVIDERS_ADMIN]


async def ensure_tenant(session, slug: str, name: str) -> Tenant:
    res = await session.execute(select(Tenant).where(Tenant.slug == slug))
    tenant = res.scalar_one_or_none()
    if tenant is None:
        tenant = Tenant(slug=slug, name=name)
        session.add(tenant)
        await session.flush()
    return tenant


async def seed() -> list[str]:
    s = get_settings()
    created: list[str] = []
    async with admin_session_factory()() as session:
        tenant = await ensure_tenant(session, s.seed_tenant_slug, s.seed_tenant_name)
        n_keys = (await session.execute(select(func.count()).select_from(ApiKey))).scalar_one()
        if n_keys == 0 and not s.bootstrap_keys:
            raw = "dual-lobe-local"
            session.add(ApiKey(tenant_id=tenant.id, key_hash=hash_key(raw), label="dev default", scopes=ALL_SCOPES))
            created.append(raw)
            await session.commit()
            return created
        for entry in (p for p in s.bootstrap_keys.split(";") if p.strip()):
            parts = [x.strip() for x in entry.split("|")]
            raw = parts[0]
            scopes = parts[1].split(",") if len(parts) > 1 and parts[1] else ALL_SCOPES
            slug = parts[2] if len(parts) > 2 and parts[2] else s.seed_tenant_slug
            t = await ensure_tenant(session, slug, slug)
            session.add(ApiKey(tenant_id=t.id, key_hash=hash_key(raw), label="bootstrap", scopes=scopes))
            created.append(raw)
        await session.commit()
    return created


def main() -> None:
    async def _run() -> list[str]:
        created = await seed()
        await dispose_engines()
        return created

    created = asyncio.run(_run())
    print("created keys:", created)


if __name__ == "__main__":
    main()