import hashlib
import uuid

from app.users.keys import AuthedUser, KeyCache, hash_key


def _user() -> AuthedUser:
    return AuthedUser(id=uuid.uuid4(), name="alice")


def test_hash_key_is_sha256_hex():
    assert hash_key("secret") == hashlib.sha256(b"secret").hexdigest()


def test_cache_miss_returns_none():
    cache = KeyCache(ttl_s=60)
    assert cache.get("nope") is None


def test_cache_put_then_get_returns_user():
    cache = KeyCache(ttl_s=60)
    user = _user()
    cache.put("h1", user)
    assert cache.get("h1") == user


def test_cache_entry_expires_after_ttl():
    clock = {"t": 0.0}
    cache = KeyCache(ttl_s=60, now=lambda: clock["t"])
    cache.put("h1", _user())
    clock["t"] = 59.9
    assert cache.get("h1") is not None
    clock["t"] = 60.0
    assert cache.get("h1") is None


def test_cache_ttl_zero_never_stores():
    cache = KeyCache(ttl_s=0)
    cache.put("h1", _user())
    assert cache.get("h1") is None


def test_cache_is_bounded():
    cache = KeyCache(ttl_s=60, max_entries=2)
    cache.put("h1", _user())
    cache.put("h2", _user())
    cache.put("h3", _user())
    stored = [h for h in ("h1", "h2", "h3") if cache.get(h) is not None]
    assert len(stored) == 2
    assert cache.get("h3") is not None  # newest entry always survives
