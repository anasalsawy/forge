"""Scoped Bearer auth: an API key maps to a tenant and a set of scopes.

Key lookup runs on the admin engine (the tenant is unknown until the key
resolves); every subsequent data-plane query uses an RLS-bound session.
"""
from __future__ import annotations

import hashlib
import secrets
from dataclasses import dataclass

from fastapi import Depends, HTTPException, Request, status

from ..core.engine import admin_session_factory
from ..state import repositories as repo

SCOPE_INFERENCE_INVOKE = "inference:invoke"
SCOPE_EVENTS_WRITE = "events:write"
SCOPE_STATE_READ = "state:read"
SCOPE_STATE_DEBUG = "state:debug"
SCOPE_RUNS_ADMIN = "runs:admin"
SCOPE_PROVIDERS_ADMIN = "providers:admin"


@dataclass
class Principal:
    tenant_id: int
    tenant_slug: str
    scopes: frozenset[str]

    def has(self, scope: str) -> bool:
        return scope in self.scopes


def hash_key(raw: str) -> str:
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def new_key() -> str:
    return f"dl_{secrets.token_hex(24)}"


async def load_principal(raw_key: str) -> Principal | None:
    if not raw_key:
        return None
    async with admin_session_factory()() as session:
        k = await repo.get_api_key(session, hash_key(raw_key))
        if k is None:
            return None
        from sqlalchemy import select

        res = await session.execute(select(repo.models.Tenant).where(repo.models.Tenant.id == k.tenant_id))
        tenant = res.scalar_one_or_none()
    if tenant is None:
        return None
    return Principal(tenant_id=k.tenant_id, tenant_slug=tenant.slug, scopes=frozenset(k.scopes or []))


async def _extract_token(request: Request) -> str | None:
    raw = request.headers.get("Authorization") or ""
    if raw.lower().startswith("bearer "):
        return raw[len("Bearer "):].strip()
    return None


def require_scope(scope: str):
    async def _dep(request: Request) -> Principal:
        token = await _extract_token(request)
        principal = await load_principal(token) if token else None
        if principal is None:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="invalid or missing API key")
        if not principal.has(scope):
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=f"missing scope {scope}")
        return principal

    return _dep


async def optional_principal(request: Request) -> Principal | None:
    token = await _extract_token(request)
    if not token:
        return None
    return await load_principal(token)