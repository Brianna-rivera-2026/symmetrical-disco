from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker


def make_engine(database_url: str) -> Engine:
    # pool_timeout=5: a saturated pool turns into a fast 503 on /ready
    # instead of a 30s hang (also bounds app-side checkout waits).
    return create_engine(
        database_url, pool_pre_ping=True, pool_timeout=5, future=True
    )


def make_session_factory(engine: Engine) -> sessionmaker[Session]:
    return sessionmaker(bind=engine, expire_on_commit=False, future=True)
