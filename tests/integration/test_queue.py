from app.queue.consumer import ack, ensure_group, read_one
from app.queue.producer import enqueue

STREAM = "jobs:stream"
GROUP = "workers"


def test_ensure_group_is_idempotent(redis_client):
    ensure_group(redis_client, STREAM, GROUP)
    ensure_group(redis_client, STREAM, GROUP)  # must not raise


def test_enqueue_read_ack_cycle(redis_client):
    ensure_group(redis_client, STREAM, GROUP)
    enqueue(redis_client, STREAM, "job-123")

    msg = read_one(redis_client, STREAM, GROUP, "consumer-a", block_ms=1000)
    assert msg is not None
    message_id, fields = msg
    assert fields["job_id"] == "job-123"

    # Still pending until acked.
    pending = redis_client.xpending(STREAM, GROUP)
    assert pending["pending"] == 1

    ack(redis_client, STREAM, GROUP, message_id)
    pending_after = redis_client.xpending(STREAM, GROUP)
    assert pending_after["pending"] == 0


def test_read_returns_none_when_empty(redis_client):
    ensure_group(redis_client, STREAM, GROUP)
    assert read_one(redis_client, STREAM, GROUP, "consumer-a", block_ms=100) is None
