from app.queue.consumer import ack, ensure_group, read_priority
from app.queue.producer import enqueue

STREAM = "jobs:stream"
GROUP = "workers"


async def test_ensure_group_is_idempotent(redis_client):
    await ensure_group(redis_client, STREAM, GROUP)
    await ensure_group(redis_client, STREAM, GROUP)  # must not raise


async def test_enqueue_read_ack_cycle(redis_client):
    await ensure_group(redis_client, STREAM, GROUP)
    await enqueue(redis_client, STREAM, "job-123")

    batch = await read_priority(redis_client, [STREAM], GROUP, "consumer-a", block_ms=1000)
    assert len(batch) == 1
    stream, message_id, fields = batch[0]
    assert fields["job_id"] == "job-123"

    # Still pending until acked.
    assert (await redis_client.xpending(STREAM, GROUP))["pending"] == 1

    await ack(redis_client, stream, GROUP, message_id)
    assert (await redis_client.xpending(STREAM, GROUP))["pending"] == 0


PRIO_STREAMS = ["s:high", "s:normal", "s:low"]


async def _ensure_prio_groups(redis_client):
    for s in PRIO_STREAMS:
        await ensure_group(redis_client, s, GROUP)


async def test_read_priority_prefers_higher_stream(redis_client):
    await _ensure_prio_groups(redis_client)
    await enqueue(redis_client, "s:low", "low-1")
    await enqueue(redis_client, "s:high", "high-1")

    batch = await read_priority(redis_client, PRIO_STREAMS, GROUP, "c1", block_ms=100)

    assert [(s, f["job_id"]) for s, _mid, f in batch] == [("s:high", "high-1")]
    # Strict: the low stream was never read, so its entry is still undelivered.
    assert await redis_client.xlen("s:low") == 1
    assert (await redis_client.xpending("s:low", GROUP))["pending"] == 0


async def test_read_priority_falls_through_to_lower(redis_client):
    await _ensure_prio_groups(redis_client)
    await enqueue(redis_client, "s:low", "low-1")

    batch = await read_priority(redis_client, PRIO_STREAMS, GROUP, "c1", block_ms=100)

    assert [(s, f["job_id"]) for s, _mid, f in batch] == [("s:low", "low-1")]


async def test_read_priority_empty_returns_empty_list(redis_client):
    await _ensure_prio_groups(redis_client)
    assert await read_priority(redis_client, PRIO_STREAMS, GROUP, "c1", block_ms=50) == []
