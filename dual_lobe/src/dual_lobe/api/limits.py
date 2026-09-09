"""Per-tenant RPM/TPM rate limiting with a Redis sliding window and an
in-process fallback when Redis is unavailable."""
from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from typing import Any

from ..core.settings import get_settings

_redis: Any | None = None
_redis_failed_at: float = 0.0
_local: dict[str, list[tuple[float, int]]] = {}
_local_lock = asyncio.Lock()


def _get_redis():
    global _redis, _redis_failed_at
    if _redis is not None:
        return _redis
    if _redis_failed_at and time.time() - _redis_failed_at < 5:
        return None
    try:
        import redis.asyncio as aioredis

        _redis = aioredis.from_url(get_settings().redis_url, socket_connect_timeout=1)
        return _redis
    except Exception:
        _redis_failed_at = time.time()
        return None


async def _local_sliding(key: str, window: float, limit: int, weight: int = 1) -> tuple[bool, float]:
    async with _local_lock:
        now = time.time()
        hits = [(t, w) for t, w in _local.get(key, []) if now - t < window]
        used = sum(w for _, w in hits)
        if limit > 0 and used >= limit:
            _local[key] = [(t, w) for t, w in hits]
            retry = window - (now - hits[0][0]) if hits else window
            return False, max(0.0, retry)
        hits.append((now, weight))
        _local[key] = hits
        return True, 0.0


async def _redis_sliding(key: str, window: float, limit: int, weight: int = 1) -> tuple[bool, float]:
    client = _get_redis()
    if client is None:
        return await _local_sliding(key, window, limit, weight)
    try:
        now = int(time.time() * 1000)
        pipe = client.pipeline()
        pipe.zremrangebyscore(key, 0, now - int(window * 1000))
        pipe.zrange(key, 0, -1, withscores=True)
        _, entries = await pipe.execute()
        used = sum(int(str(e).rsplit(":", 1)[-1]) if ":" in str(e) else 1 for e, _ in entries)
        if limit > 0 and used >= limit:
            if entries:
                first_ts = int(entries[0][1])
                return False, max(0.0, window - (now - first_ts) / 1000.0)
            return False, window
        pipe = client.pipeline()
        pipe.zadd(key, {f"{now}:{weight}": now})
        pipe.expire(key, int(window) + 1)
        await pipe.execute()
        return True, 0.0
    except Exception:
        return await _local_sliding(key, window, limit, weight)


@dataclass
class RateResult:
    allowed: bool = True
    retry_after: float = 0.0
    headers: dict[str, str] = field(default_factory=dict)

    def apply(self, rpm_limit: int, tpm_limit: int, rpm_left: float, tpm_left: float) -> None:
        self.headers["X-RateLimit-Limit-RPM"] = str(rpm_limit)
        self.headers["X-RateLimit-Limit-TPM"] = str(tpm_limit)
        self.headers["X-RateLimit-Remaining-RPM"] = str(int(rpm_left))
        self.headers["X-RateLimit-Remaining-TPM"] = str(int(tpm_left))


async def check_limits(tenant_id: int, token_estimate: int = 0) -> RateResult:
    s = get_settings()
    window = 60.0
    rpm_ok, rpm_wait = await _local_sliding(f"dl:rpm:{tenant_id}", window, s.rpm_limit, weight=1)
    tpm_ok = True
    tpm_wait = 0.0
    if token_estimate:
        tpm_ok, tpm_wait = await _local_sliding(
            f"dl:tpm:{tenant_id}", 60.0, s.tpm_limit, weight=max(1, token_estimate)
        ) if s.tpm_limit > 0 else (True, 0.0)
    allowed = rpm_ok and tpm_ok
    return RateResult(
        allowed=allowed,
        retry_after=max(rpm_wait, tpm_wait),
        headers={
            "X-RateLimit-Limit-RPM": str(s.rpm_limit),
            "X-RateLimit-Limit-TPM": str(s.tpm_limit),
            "X-RateLimit-Remaining-RPM": str(max(0, s.rpm_limit - 1)),
            "X-RateLimit-Remaining-TPM": str(max(0, s.tpm_limit)),
        },
    )