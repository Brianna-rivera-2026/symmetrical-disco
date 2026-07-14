"""Identity primitives shared by the TokenReview auth flow."""

import hashlib
import uuid
from dataclasses import dataclass


def hash_token(raw: str) -> str:
    """SHA-256 hex of a raw bearer token — cache-key only, never a stored
    credential. Tokens are high-entropy opaque strings, so a fast unsalted
    hash is the right trade-off."""
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class AuthedUser:
    id: uuid.UUID
    name: str
