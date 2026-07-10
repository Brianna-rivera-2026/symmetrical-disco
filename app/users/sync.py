"""Declarative user provisioning (init-container pattern).

Reads {"name": "raw key", ...} from a mounted secret file, hashes each key,
and upserts by name in ONE transaction — a mid-run failure rolls back so the
users table is never left mixing key generations. Upsert-only: users absent
from the file are untouched. Raw keys never leave this process; only names
are logged.
"""

import json
import logging
import sys

from opentelemetry import trace
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from app import repository as repo
from app.core.config import Settings, get_settings
from app.core.db import make_engine, make_session_factory
from app.core.logging import configure_logging
from app.core.telemetry import configure_telemetry, shutdown_telemetry
from app.users.keys import hash_key

log = logging.getLogger("app.users.sync")


def load_keys(path: str) -> dict[str, str]:
    with open(path, encoding="utf-8") as f:
        data = json.load(f)  # JSONDecodeError is a ValueError
    if not isinstance(data, dict) or not data:
        raise ValueError("secret file must be a non-empty JSON object")
    for name, raw_key in data.items():
        if not isinstance(raw_key, str) or not raw_key:
            raise ValueError(f"user {name!r} must map to a non-empty key string")
    return data


def sync_users(session: Session, keys: dict[str, str]) -> int:
    """Upsert every entry, then commit once — the whole batch is atomic."""
    for name, raw_key in keys.items():
        repo.upsert_user(session, name, hash_key(raw_key))
    session.commit()
    return len(keys)


def run(settings: Settings) -> int:
    configure_telemetry(settings, "users-sync")
    tracer = trace.get_tracer("app.users.sync")
    engine = make_engine(settings.database_url)
    exit_code = 0
    try:
        with tracer.start_as_current_span("users.sync") as span:
            keys = load_keys(settings.api_user_keys_file)
            session_factory = make_session_factory(engine)
            with session_factory() as session:
                count = sync_users(session, keys)
            span.set_attribute("users.synced_count", count)
            log.info("users.synced", extra={"count": count, "names": sorted(keys)})
    except (OSError, ValueError, SQLAlchemyError) as exc:
        # Deliberately not exc_info=True / str(exc): SQLAlchemyError's default
        # __str__ includes the compiled statement AND its bound parameters,
        # which for repo.upsert_user is the raw key_hash being written. Log
        # only the exception type so operators get a useful diagnostic
        # without a key hash ever reaching the log stream.
        log.error("users.sync_failed", extra={"error_type": type(exc).__name__})
        exit_code = 1
    finally:
        engine.dispose()
        # One-shot process: shutdown flushes the batch processors so the
        # span/logs actually reach the collector before we exit.
        shutdown_telemetry()
    return exit_code


def main() -> int:
    settings = get_settings()
    configure_logging(settings.log_level)
    return run(settings)


if __name__ == "__main__":
    sys.exit(main())
