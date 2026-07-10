import json

import pytest
from sqlalchemy import select

from app import repository as repo
from app.models.user import User
from app.users import sync
from app.users.keys import hash_key


def _names_and_hashes(session) -> dict[str, str]:
    rows = session.execute(select(User.name, User.key_hash)).all()
    return {name: key_hash for name, key_hash in rows}


def test_sync_inserts_new_users(db_session):
    count = sync.sync_users(db_session, {"alice": "key-a", "bob": "key-b"})
    assert count == 2
    assert _names_and_hashes(db_session) == {
        "alice": hash_key("key-a"),
        "bob": hash_key("key-b"),
    }


def test_sync_rotates_changed_key(db_session):
    sync.sync_users(db_session, {"alice": "old-key"})
    sync.sync_users(db_session, {"alice": "new-key"})
    assert _names_and_hashes(db_session)["alice"] == hash_key("new-key")


def test_sync_same_input_is_noop(db_session):
    sync.sync_users(db_session, {"alice": "key-a"})
    before = _names_and_hashes(db_session)
    sync.sync_users(db_session, {"alice": "key-a"})
    assert _names_and_hashes(db_session) == before


def test_sync_leaves_absent_users_untouched(db_session):
    sync.sync_users(db_session, {"alice": "key-a"})
    sync.sync_users(db_session, {"bob": "key-b"})  # alice not in this file
    assert set(_names_and_hashes(db_session)) == {"alice", "bob"}


def test_sync_partial_failure_rolls_back(db_session, monkeypatch):
    calls = {"n": 0}
    real = sync.hash_key

    def flaky(raw_key):
        calls["n"] += 1
        if calls["n"] == 2:
            raise RuntimeError("boom")
        return real(raw_key)

    monkeypatch.setattr(sync, "hash_key", flaky)
    with pytest.raises(RuntimeError):
        sync.sync_users(db_session, {"alice": "key-a", "bob": "key-b"})
    db_session.rollback()
    assert _names_and_hashes(db_session) == {}  # nothing committed


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
    from sqlalchemy import text

    keyfile = tmp_path / "keys.json"
    keyfile.write_text(json.dumps({"alice": "key-a"}))
    settings = test_settings.model_copy(update={"api_user_keys_file": str(keyfile)})
    assert sync.run(settings) == 0
    with pg_engine.begin() as conn:
        n = conn.execute(text("SELECT count(*) FROM users")).scalar_one()
    assert n == 1
    with pg_engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE jobs, users"))
