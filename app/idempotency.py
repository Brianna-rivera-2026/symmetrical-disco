import hashlib
import json

from app.schemas.enums import JobType


def canonical_hash(job_type: JobType, payload: dict) -> str:
    """Stable SHA-256 of a *submitted* payload (pre-JSONB), for idempotency reuse
    detection. `default=str` is defensive only — `payload` is a JSON-parsed dict
    and already contains no non-serializable objects."""
    blob = json.dumps(
        {"type": job_type.value, "payload": payload},
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode()
    return hashlib.sha256(blob).hexdigest()
