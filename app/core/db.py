from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)


def make_engine(database_url: str) -> AsyncEngine:
    # pool_timeout=5: a saturated pool turns into a fast 503 on /ready
    # instead of a 30s hang (also bounds app-side checkout waits).
    return create_async_engine(database_url, pool_pre_ping=True, pool_timeout=5)


def make_session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(bind=engine, expire_on_commit=False)
