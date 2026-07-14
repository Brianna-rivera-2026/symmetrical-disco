"""Per-user rate limiting (spec §1): fastapi-limiter dependencies keyed by
the validated cluster identity, falling back to client IP for
unauthenticated requests."""

import redis as pyredis
from fastapi import Request, Response
from fastapi_limiter import FastAPILimiter
from fastapi_limiter.depends import RateLimiter


async def user_or_ip_identifier(request: Request) -> str:
    """Bucket by the *validated* user id set by `get_current_user` on
    `request.state.authed_user_id` — never by the raw bearer token value.
    Trusting the raw token would let an attacker rotate a fresh,
    never-seen token on every request to dodge the limiter entirely (garbage
    tokens still hash to a distinct bucket even though they never authenticate).

    Falls back to the client IP when no validated identity is present
    (routes with no `get_current_user` dependency, e.g. the anonymous
    `/stats` endpoint). That IP fallback is only as trustworthy as the
    deployment's `forwarded_allow_ips`/proxy-header trust configuration —
    it authenticates the immediate connection to the trusted proxy, not the
    contents of the forwarded-for chain, so it's best-effort abuse
    mitigation rather than a hard guarantee. `/stats` has no stronger
    identity to fall back on since it's intentionally unauthenticated.
    """
    authed_user_id = getattr(request.state, "authed_user_id", None)
    if authed_user_id is not None:
        return "u:" + str(authed_user_id)
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
        try:
            pexpire = await self._check(key)
        except pyredis.exceptions.NoScriptError:
            # Mirrors upstream RateLimiter.__call__: the Lua script can fall
            # out of Redis's script cache (restart, SCRIPT FLUSH, failover to
            # a replica that never loaded it) — reload it and retry once
            # instead of raising and turning every rate-limited request into
            # a 500 until the process restarts.
            FastAPILimiter.lua_sha = await FastAPILimiter.redis.script_load(
                FastAPILimiter.lua_script
            )
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
