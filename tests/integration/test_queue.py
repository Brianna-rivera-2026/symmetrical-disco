from app.queue.consumer import ack, ensure_group, read_one, read_priority
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


PRIO_STREAMS = ["s:high", "s:normal", "s:low"]


def _ensure_prio_groups(redis_client):
    for s in PRIO_STREAMS:
        ensure_group(redis_client, s, GROUP)


def test_read_priority_prefers_higher_stream(redis_client):
    _ensure_prio_groups(redis_client)
    enqueue(redis_client, "s:low", "low-1")
    enqueue(redis_client, "s:high", "high-1")

    batch = read_priority(redis_client, PRIO_STREAMS, GROUP, "c1", block_ms=100)

    assert [(s, f["job_id"]) for s, _mid, f in batch] == [("s:high", "high-1")]
    # Strict: the low stream was never read, so its entry is still undelivered.
    assert redis_client.xlen("s:low") == 1
    assert redis_client.xpending("s:low", GROUP)["pending"] == 0


def test_read_priority_falls_through_to_lower(redis_client):
    _ensure_prio_groups(redis_client)
    enqueue(redis_client, "s:low", "low-1")

    batch = read_priority(redis_client, PRIO_STREAMS, GROUP, "c1", block_ms=100)

    assert [(s, f["job_id"]) for s, _mid, f in batch] == [("s:low", "low-1")]


def test_read_priority_empty_returns_empty_list(redis_client):
    _ensure_prio_groups(redis_client)
    assert read_priority(redis_client, PRIO_STREAMS, GROUP, "c1", block_ms=50) == []
