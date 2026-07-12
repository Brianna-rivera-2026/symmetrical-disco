"""Per-user rate limiting (spec §1): fastapi-limiter dependencies keyed by
API-key hash, falling back to client IP for unauthenticated requests."""

from fastapi import Request, Response
from fastapi_limiter import FastAPILimiter
from fastapi_limiter.depends import RateLimiter

from app.users.keys import hash_key


async def user_or_ip_identifier(request: Request) -> str:
    api_key = request.headers.get("X-API-Key")
    if api_key:
        return "u:" + hash_key(api_key)
    host = request.client.host if request.client else "unknown"
    return "ip:" + host


class _GroupRateLimiter(RateLimiter):
    """`RateLimiter` variant that skips fastapi-limiter's built-in route/
    dependency lookup.

    fastapi-limiter 0.1.x disambiguates buckets for multiple `RateLimiter`
    dependencies by scanning `request.app.routes` for the current route and
    indexing into its `dependencies` list. That scan assumes routes included
    via `include_router` are flattened `APIRoute` objects exposing `.path`.
    Current FastAPI instead wraps included routers in a `_IncludedRouter`
    object with no `.path` attribute, so the scan raises `AttributeError` on
    every request — and even patched to not crash, it would never match a
    real route (they're nested inside the wrapper), collapsing every
    dependency to the same `route_index=0, dep_index=0` bucket and letting
    unrelated limit groups (e.g. "submit" and "stats") collide onto one
    shared counter per user. We already fold the group name into `self.group`
    for the Redis key, so the route/dependency lookup is unneeded; this
    override reuses only the stable, non-route-dependent parts of the
    upstream implementation (`_check`, `FastAPILimiter.redis/prefix/identifier
    /http_callback`).
    """

    def __init__(self, group: str, **kwargs):
        super().__init__(**kwargs)
        self.group = group

    async def __call__(self, request: Request, response: Response) -> None:
        if not FastAPILimiter.redis:
            raise RuntimeError(
                "You must call FastAPILimiter.init in startup event of fastapi!"
            )
        identifier = self.identifier or FastAPILimiter.identifier
        callback = self.callback or FastAPILimiter.http_callback
        rate_key = await identifier(request)
        key = f"{FastAPILimiter.prefix}:{self.group}:{rate_key}"
        pexpire = await self._check(key)
        if pexpire != 0:
            await callback(request, response, pexpire)


def rate_limit(group: str):
    """Route dependency for one limit group; no-op when disabled in settings."""

    async def dependency(request: Request, response: Response) -> None:
        settings = request.app.state.settings
        if not settings.rate_limit_enabled:
            return
        times = getattr(settings, f"{group}_rate_limit_per_min")
        await _GroupRateLimiter(group, times=times, seconds=60)(request, response)

    return dependency
