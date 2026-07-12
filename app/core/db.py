from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)


def engine_kwargs(pool_size: int, disable_prepared_statements: bool) -> dict:
    # pool_timeout=5: a saturated pool turns into a fast 503 on /ready
    # instead of a 30s hang (also bounds app-side checkout waits).
    kwargs: dict = {"pool_pre_ping": True, "pool_timeout": 5, "pool_size": pool_size}
    if disable_prepared_statements:
        # PgBouncer transaction pooling: a prepared statement lives on one
        # server connection but later executions may land on another.
        kwargs["connect_args"] = {"prepare_threshold": None}
    return kwargs


def make_engine(
    database_url: str,
    *,
    pool_size: int = 5,
    disable_prepared_statements: bool = False,
) -> AsyncEngine:
    return create_async_engine(
        database_url, **engine_kwargs(pool_size, disable_prepared_statements)
    )


def make_session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(bind=engine, expire_on_commit=False)
