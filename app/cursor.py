import base64
import json
from datetime import datetime
from uuid import UUID


def encode_cursor(created_at: datetime, job_id: UUID) -> str:
    raw = json.dumps({"c": created_at.isoformat(), "i": str(job_id)}).encode()
    return base64.urlsafe_b64encode(raw).decode()


def decode_cursor(cursor: str) -> tuple[datetime, UUID]:
    try:
        raw = base64.urlsafe_b64decode(cursor.encode())
        data = json.loads(raw)
        return datetime.fromisoformat(data["c"]), UUID(data["i"])
    except (ValueError, KeyError, json.JSONDecodeError) as exc:
        raise ValueError("invalid cursor") from exc
