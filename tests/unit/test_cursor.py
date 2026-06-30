import uuid
from datetime import datetime, timezone

import pytest

from app.cursor import decode_cursor, encode_cursor


def test_cursor_round_trip():
    created = datetime(2026, 6, 30, 12, 0, 0, tzinfo=timezone.utc)
    jid = uuid.uuid4()
    token = encode_cursor(created, jid)
    got_created, got_id = decode_cursor(token)
    assert got_created == created
    assert got_id == jid


def test_decode_rejects_garbage():
    with pytest.raises(ValueError):
        decode_cursor("not-a-real-cursor")
