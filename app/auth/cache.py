"""Bounded TTL cache for validated bearer tokens."""

import time
from collections.abc import Callable

from app.auth.identity import AuthedUser


class TokenCache:
    """Bounded TTL cache: token_hash -> AuthedUser. Only successful
    validations are stored, so revocation propagates within one TTL and
    unknown tokens can never validate from cache. Races between requests
    are benign (worst case: a duplicate TokenReview call)."""

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

    def get(self, token_hash: str) -> AuthedUser | None:
        entry = self._entries.get(token_hash)
        if entry is None:
            return None
        expires_at, user = entry
        if self._now() >= expires_at:
            self._entries.pop(token_hash, None)
            return None
        return user

    def put(self, token_hash: str, user: AuthedUser) -> None:
        if self._ttl_s <= 0:
            return
        if token_hash not in self._entries and len(self._entries) >= self._max:
            self._evict()
        self._entries[token_hash] = (self._now() + self._ttl_s, user)

    def _evict(self) -> None:
        now = self._now()
        for key in [k for k, (exp, _) in self._entries.items() if exp <= now]:
            del self._entries[key]
        while len(self._entries) >= self._max:
            del self._entries[next(iter(self._entries))]  # oldest insertion
