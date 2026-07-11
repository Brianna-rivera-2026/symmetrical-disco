import json

import pytest
from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError

from app.core.logging import JsonFormatter
from app.models.user import User
from app.users import sync
from app.users.keys import hash_key


async def _names_and_hashes(session) -> dict[str, str]:
    rows = (await session.execute(select(User.name, User.key_hash))).all()
    return {name: key_hash for name, key_hash in rows}


async def test_sync_inserts_new_users(db_session):
    count = await sync.sync_users(db_session, {"alice": "key-a", "bob": "key-b"})
    assert count == 2
    assert await _names_and_hashes(db_session) == {
        "alice": hash_key("key-a"),
        "bob": hash_key("key-b"),
    }


async def test_sync_rotates_changed_key(db_session):
    await sync.sync_users(db_session, {"alice": "old-key"})
    await sync.sync_users(db_session, {"alice": "new-key"})
    assert (await _names_and_hashes(db_session))["alice"] == hash_key("new-key")


async def test_sync_same_input_is_noop(db_session):
    await sync.sync_users(db_session, {"alice": "key-a"})
    before = await _names_and_hashes(db_session)
    await sync.sync_users(db_session, {"alice": "key-a"})
    assert await _names_and_hashes(db_session) == before


async def test_sync_leaves_absent_users_untouched(db_session):
    await sync.sync_users(db_session, {"alice": "key-a"})
    await sync.sync_users(db_session, {"bob": "key-b"})  # alice not in this file
    assert set(await _names_and_hashes(db_session)) == {"alice", "bob"}


async def test_sync_partial_failure_rolls_back(db_session, monkeypatch):
    calls = {"n": 0}
    real = sync.hash_key

    def flaky(raw_key):
        calls["n"] += 1
        if calls["n"] == 2:
            raise RuntimeError("boom")
        return real(raw_key)

    monkeypatch.setattr(sync, "hash_key", flaky)
    with pytest.raises(RuntimeError):
        await sync.sync_users(db_session, {"alice": "key-a", "bob": "key-b"})
    await db_session.rollback()
    assert await _names_and_hashes(db_session) == {}  # nothing committed


def test_run_db_failure_does_not_leak_key_hash_into_logs(
    test_settings, tmp_path, monkeypatch, caplog
):
    """A SQLAlchemyError's default __str__ includes the compiled statement AND
    its bound parameters (see sqlalchemy.exc.StatementError) — for
    repo.upsert_user that's the raw key_hash being written. run() must never
    pass that string (or a traceback containing it) to the logger."""
    secret_hash = hash_key("super-secret-raw-key")

    def boom(session, keys):
        # Mimic what SQLAlchemy raises: a StatementError-like exception whose
        # string representation embeds the bound parameter value.
        raise SQLAlchemyError(
            f"(psycopg.errors.UniqueViolation) duplicate key\n"
            f"[SQL: INSERT INTO users ...]\n"
            f"[parameters: {{'key_hash': '{secret_hash}'}}]"
        )

    monkeypatch.setattr(sync, "sync_users", boom)

    keyfile = tmp_path / "keys.json"
    keyfile.write_text(json.dumps({"alice": "super-secret-raw-key"}))
    settings = test_settings.model_copy(update={"api_user_keys_file": str(keyfile)})

    with caplog.at_level("ERROR", logger="app.users.sync"):
        assert sync.run(settings) == 1

    # Belt-and-suspenders: exc_info=True is what would smuggle the leaked
    # parameter string in via record.exc_text/formatException, bypassing
    # getMessage() entirely. Pin that it was never passed to log.error(...).
    for record in caplog.records:
        assert record.exc_info is None
        assert record.exc_text is None

    # The real assertion: render every captured record through the actual
    # production JsonFormatter (app/core/logging.py) and inspect the exact
    # JSON that would hit stdout, including the "exception" key that
    # formatException populates when exc_info is truthy.
    formatter = JsonFormatter()
    rendered_lines = [formatter.format(r) for r in caplog.records]
    rendered = "\n".join(rendered_lines)
    assert secret_hash not in rendered
    assert "super-secret-raw-key" not in rendered
    assert "SQLAlchemyError" in rendered

    payloads = [json.loads(line) for line in rendered_lines]
    assert not any("exception" in payload for payload in payloads)
    assert any(payload.get("error_type") == "SQLAlchemyError" for payload in payloads)


def test_load_keys_rejects_bad_shapes(tmp_path):
    not_a_dict = tmp_path / "list.json"
    not_a_dict.write_text(json.dumps(["alice"]))
    with pytest.raises(ValueError):
        sync.load_keys(str(not_a_dict))

    empty_value = tmp_path / "empty.json"
    empty_value.write_text(json.dumps({"alice": ""}))
    with pytest.raises(ValueError):
        sync.load_keys(str(empty_value))

    garbage = tmp_path / "garbage.json"
    garbage.write_text("{not json")
    with pytest.raises(ValueError):
        sync.load_keys(str(garbage))


def test_run_missing_file_exits_nonzero(test_settings, tmp_path):
    settings = test_settings.model_copy(
        update={"api_user_keys_file": str(tmp_path / "absent.json")}
    )
    assert sync.run(settings) == 1


def test_run_happy_path_exits_zero(test_settings, pg_engine, tmp_path):
    import asyncio

    from sqlalchemy import text

    keyfile = tmp_path / "keys.json"
    keyfile.write_text(json.dumps({"alice": "key-a"}))
    settings = test_settings.model_copy(update={"api_user_keys_file": str(keyfile)})
    assert sync.run(settings) == 0

    async def _check_and_truncate():
        async with pg_engine.begin() as conn:
            n = (await conn.execute(text("SELECT count(*) FROM users"))).scalar_one()
        async with pg_engine.begin() as conn:
            await conn.execute(text("TRUNCATE TABLE jobs, users"))
        return n

    n = asyncio.run(_check_and_truncate())
    assert n == 1
