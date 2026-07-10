import logging
from collections.abc import AsyncIterator, Iterator

import redis
from fastapi import Depends, HTTPException, Request, Security
from fastapi.security import APIKeyHeader
from opentelemetry import trace
from sqlalchemy.orm import Session

from app import repository as repo
from app.core import metrics as app_metrics
from app.core.logging import bind_log_context
from app.users.keys import AuthedUser, KeyCache, hash_key

log = logging.getLogger("app.api")

api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


def get_db(request: Request) -> Iterator[Session]:
    factory = request.app.state.session_factory
    with factory() as session:
        yield session


def get_redis(request: Request) -> redis.Redis:
    return request.app.state.redis


async def get_current_user(
    request: Request,
    api_key: str | None = Security(api_key_header),
    session: Session = Depends(get_db),
) -> AsyncIterator[AuthedUser]:
    """Async yield-dependency: runs in the request's task context, so the
    bound log fields propagate into the sync endpoint's threadpool copy."""
    if not api_key:
        app_metrics.auth_validations.add(1, {"result": "missing_key", "source": "n/a"})
        log.warning("auth.missing_key")
        raise HTTPException(status_code=401, detail="missing API key")

    key_hash = hash_key(api_key)
    cache: KeyCache = request.app.state.key_cache
    user = cache.get(key_hash)
    source = "cache"
    if user is None:
        source = "db"
        row = repo.get_user_by_key_hash(session, key_hash)
        if row is None:
            app_metrics.auth_validations.add(
                1, {"result": "unknown_key", "source": "db"}
            )
            log.warning("auth.unknown_key")
            raise HTTPException(status_code=401, detail="invalid API key")
        user = AuthedUser(id=row.id, name=row.name)
        cache.put(key_hash, user)

    app_metrics.auth_validations.add(1, {"result": "ok", "source": source})
    trace.get_current_span().set_attribute("enduser.id", str(user.id))
    with bind_log_context(user_id=str(user.id), user_name=user.name):
        yield user
