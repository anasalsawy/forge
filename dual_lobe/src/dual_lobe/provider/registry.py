"""Provider registry: logical aliases -> ProviderTarget.

Aliases ``lobe-a``/``lobe-b`` are resolved from the environment contract
(``DUAL_LOBE_A_*`` / ``DUAL_LOBE_B_*``); additional entries come from the
``provider_registry`` table (admin-managed). Providers declare capabilities
(stream/tools/responses/…) so routing is data-driven.
"""
from __future__ import annotations

from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ..core.models import ProviderRegistry
from .adapters import ProviderTarget, make_adapter


def env_targets() -> dict[str, ProviderTarget]:
    from ..core.settings import get_settings

    s = get_settings()
    return {
        "lobe-a": ProviderTarget(
            alias="lobe-a",
            base_url=s.a_base_url,
            api_key=s.a_api_key,
            model=s.a_model,
            kind=s.a_dialect,
            capabilities={"stream": True, "tools": True, "responses": False},
        ),
        "lobe-b": ProviderTarget(
            alias="lobe-b",
            base_url=s.resolved_b_base_url,
            api_key=s.resolved_b_api_key,
            model=s.resolved_b_model,
            kind=s.b_dialect,
            capabilities={"stream": False, "tools": False, "responses": False},
        ),
    }


async def load_db_targets(session: AsyncSession) -> dict[str, ProviderTarget]:
    res = await session.execute(
        select(ProviderRegistry).where(ProviderRegistry.enabled.is_(True)).order_by(ProviderRegistry.alias)
    )
    out: dict[str, ProviderTarget] = {}
    for row in res.scalars().all():
        out[row.alias] = ProviderTarget(
            alias=row.alias,
            base_url=row.base_url,
            api_key="",
            model=row.model,
            kind=row.kind,
            capabilities=row.capabilities or {},
            enabled=row.enabled,
        )
    return out


class Registry:
    def __init__(self) -> None:
        self._targets: dict[str, ProviderTarget] = {}
        self._adapters: dict[str, Any] = {}

    def register(self, target: ProviderTarget) -> None:
        self._targets[target.alias] = target
        self._adapters[target.alias] = make_adapter(target)

    def refresh(self, targets: dict[str, ProviderTarget]) -> None:
        self._targets = {}
        self._adapters = {}
        for target in targets.values():
            self.register(target)

    def target(self, alias: str | None = None) -> ProviderTarget:
        if not alias or alias not in self._targets:
            return self._targets.get("lobe-a") or list(self._targets.values())[0]
        return self._targets[alias]

    def adapter(self, alias: str | None = None):
        target = self.target(alias)
        return self._adapters[target.alias]

    def models(self) -> list[dict[str, Any]]:
        return [
            {
                "id": t.alias,
                "object": "model",
                "owned_by": "local",
                "logical_model": t.model,
                "kind": t.kind,
                "capabilities": t.capabilities,
            }
            for t in self._targets.values()
            if t.enabled
        ]


_registry: Registry | None = None


def get_registry() -> Registry:
    global _registry
    if _registry is None:
        _registry = Registry()
        _registry.refresh(env_targets())
    return _registry


async def refresh_registry_from_db() -> None:
    from ..core.engine import admin_session_factory

    registry = get_registry()
    targets = env_targets()
    async with admin_session_factory()() as session:
        targets.update(await load_db_targets(session))
    registry.refresh(targets)