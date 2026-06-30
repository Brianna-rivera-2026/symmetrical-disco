import os
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.api.routes import router
from app.core.config import Settings, get_settings
from app.core.db import make_engine, make_session_factory
from app.core.logging import configure_logging
from app.core.redis import create_redis_client
from app.queue.consumer import ensure_group


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or get_settings()
    configure_logging(settings.log_level)

    engine = make_engine(settings.database_url)
    session_factory = make_session_factory(engine)
    redis_client = create_redis_client(settings.redis_url)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        ensure_group(redis_client, settings.jobs_stream, settings.consumer_group)
        yield
        redis_client.close()
        engine.dispose()

    app = FastAPI(title="Job Processor", lifespan=lifespan)
    app.state.settings = settings
    app.state.session_factory = session_factory
    app.state.redis = redis_client
    app.include_router(router)
    return app


app = create_app() if os.getenv("DATABASE_URL") else None  # type: ignore[assignment]
