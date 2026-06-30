from collections.abc import Iterator

import redis
from fastapi import Request
from sqlalchemy.orm import Session


def get_db(request: Request) -> Iterator[Session]:
    factory = request.app.state.session_factory
    with factory() as session:
        yield session


def get_redis(request: Request) -> redis.Redis:
    return request.app.state.redis
