"""API-key identity primitives: hashing and the per-process TTL cache."""

import hashlib
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass


def hash_key(raw_key: str) -> str:
    """SHA-256 hex of a raw API key. Keys are high-entropy random strings,
    not passwords, so a fast unsalted hash is the right trade-off."""
    return hashlib.sha256(raw_key.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class AuthedUser:
    id: uuid.UUID
    name: str


class KeyCache:
    """Bounded TTL cache: key_hash -> AuthedUser. Only successful lookups are
    stored, so revocation propagates within one TTL and unknown keys can never
    validate from cache. Races between threadpool requests are benign (worst
    case: a duplicate DB lookup)."""

    def __init__(
        self,
        ttl_s: float,
        max_entries: int = 1024,
        now: Callable[[], float] = time.monotonic,
    ) -> None:
        self._ttl_s = ttl_s
        self._max = max_entries
        self._now = now
        self._entries: dict[str, tuple[float, AuthedUser]] = {}

    def get(self, key_hash: str) -> AuthedUser | None:
        entry = self._entries.get(key_hash)
        if entry is None:
            return None
        expires_at, user = entry
        if self._now() >= expires_at:
            self._entries.pop(key_hash, None)
            return None
        return user

    def put(self, key_hash: str, user: AuthedUser) -> None:
        if self._ttl_s <= 0:
            return
        if key_hash not in self._entries and len(self._entries) >= self._max:
            self._evict()
        self._entries[key_hash] = (self._now() + self._ttl_s, user)

    def _evict(self) -> None:
        now = self._now()
        for key in [k for k, (exp, _) in self._entries.items() if exp <= now]:
            del self._entries[key]
        while len(self._entries) >= self._max:
            del self._entries[next(iter(self._entries))]  # oldest insertion
