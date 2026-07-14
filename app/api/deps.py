import logging
from collections.abc import AsyncIterator
from uuid import UUID

import redis
from fastapi import HTTPException, Request, Security
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from opentelemetry import trace
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth.cache import TokenCache
from app.auth.identity import AuthedUser, hash_token
from app.auth.tokenreview import TokenReviewer, TokenReviewUnavailable
from app.core import metrics as app_metrics
from app.core.logging import bind_log_context

log = logging.getLogger("app.api")

bearer_scheme = HTTPBearer(auto_error=False)


async def get_db(request: Request) -> AsyncIterator[AsyncSession]:
    factory = request.app.state.session_factory
    async with factory() as session:
        yield session


def get_redis(request: Request) -> redis.Redis:
    return request.app.state.redis


async def get_current_user(
    request: Request,
    credentials: HTTPAuthorizationCredentials | None = Security(bearer_scheme),
) -> AsyncIterator[AuthedUser]:
    """Async yield-dependency: runs in the request's task context, so the
    bound log fields propagate to native async endpoints."""
    if credentials is None:
        app_metrics.auth_validations.add(
            1, {"result": "missing_token", "source": "n/a"}
        )
        log.warning("auth.missing_token")
        raise HTTPException(status_code=401, detail="missing bearer token")

    token_hash = hash_token(credentials.credentials)
    cache: TokenCache = request.app.state.token_cache
    user = cache.get(token_hash)
    source = "cache"
    if user is None:
        source = "tokenreview"
        reviewer: TokenReviewer = request.app.state.token_reviewer
        try:
            reviewed = await reviewer.review(credentials.credentials)
        except TokenReviewUnavailable:
            app_metrics.auth_validations.add(
                1, {"result": "apiserver_error", "source": source}
            )
            raise HTTPException(
                status_code=503, detail="authentication unavailable"
            ) from None
        if reviewed is None:
            app_metrics.auth_validations.add(
                1, {"result": "invalid_token", "source": source}
            )
            log.warning("auth.invalid_token")
            raise HTTPException(status_code=401, detail="invalid token")
        settings = request.app.state.settings
        if settings.auth_required_group not in reviewed.groups:
            app_metrics.auth_validations.add(
                1, {"result": "forbidden_group", "source": source}
            )
            log.warning("auth.forbidden_group", extra={"user_name": reviewed.username})
            raise HTTPException(
                status_code=403, detail="not a member of the required group"
            )
        try:
            uid = UUID(reviewed.uid)
        except ValueError:
            app_metrics.auth_validations.add(
                1, {"result": "invalid_token", "source": source}
            )
            log.warning("auth.non_uuid_uid", extra={"user_name": reviewed.username})
            raise HTTPException(status_code=401, detail="invalid token") from None
        user = AuthedUser(id=uid, name=reviewed.username)
        cache.put(token_hash, user)

    app_metrics.auth_validations.add(1, {"result": "ok", "source": source})
    trace.get_current_span().set_attribute("enduser.id", str(user.id))
    with bind_log_context(user_id=str(user.id), user_name=user.name):
        request.state.authed_user_id = user.id
        request.state.authed_user_name = user.name
        yield user
