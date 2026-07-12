from contextlib import asynccontextmanager

from fastapi import FastAPI
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from pydantic import ValidationError

from app.api.routes import router
from app.core.config import Settings, get_settings
from app.core.db import make_engine, make_session_factory
from app.core.logging import configure_logging
from app.core.redis import create_redis_client
from app.core.telemetry import configure_telemetry, shutdown_telemetry
from app.queue.consumer import ensure_group
from app.users.keys import KeyCache


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or get_settings()
    configure_logging(settings.log_level)
    configure_telemetry(settings, "jobs-api")

    engine = make_engine(
        settings.database_url,
        pool_size=settings.db_pool_size,
        disable_prepared_statements=settings.db_disable_prepared_statements,
    )
    session_factory = make_session_factory(engine)
    redis_client = create_redis_client(settings.redis_url)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        for stream in settings.ordered_streams:
            await ensure_group(redis_client, stream, settings.consumer_group)
        yield
        await redis_client.aclose()
        await engine.dispose()
        shutdown_telemetry()

    app = FastAPI(title="Job Processor", lifespan=lifespan)
    app.state.settings = settings
    app.state.session_factory = session_factory
    app.state.redis = redis_client
    app.state.key_cache = KeyCache(ttl_s=settings.auth_cache_ttl_s)
    app.include_router(router)
    if settings.otel_enabled:
        FastAPIInstrumentor.instrument_app(app)
    return app


try:
    app = create_app()
except ValidationError:
    app = None  # type: ignore[assignment]
