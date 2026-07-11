import time

from app.queue import delayed

ZSET = "jobs:delayed"
STREAM = "jobs:stream"


async def test_schedule_and_due_filtering(redis_client):
    await delayed.schedule(redis_client, ZSET, "past", time.time() - 10)
    await delayed.schedule(redis_client, ZSET, "future", time.time() + 1000)
    due = await delayed.due_job_ids(redis_client, ZSET, time.time(), limit=100)
    assert due == ["past"]


async def test_due_respects_limit(redis_client):
    for i in range(5):
        await delayed.schedule(redis_client, ZSET, f"j{i}", time.time() - i - 1)
    due = await delayed.due_job_ids(redis_client, ZSET, time.time(), limit=2)
    assert len(due) == 2


async def test_promote_moves_ids_to_stream_and_removes(redis_client):
    await delayed.schedule(redis_client, ZSET, "a", time.time() - 1)
    await delayed.schedule(redis_client, ZSET, "b", time.time() - 1)
    await delayed.promote(
        redis_client,
        ZSET,
        [(STREAM, {"job_id": "a"}), (STREAM, {"job_id": "b"})],
        ["a", "b"],
    )
    assert await redis_client.xlen(STREAM) == 2
    assert await redis_client.zcard(ZSET) == 0


async def test_promote_routes_to_multiple_streams(redis_client):
    await delayed.schedule(redis_client, ZSET, "a", time.time() - 1)
    await delayed.schedule(redis_client, ZSET, "b", time.time() - 1)
    await delayed.promote(
        redis_client,
        ZSET,
        [("s:high", {"job_id": "a"}), ("s:low", {"job_id": "b"})],
        ["a", "b"],
    )
    assert await redis_client.xlen("s:high") == 1
    assert await redis_client.xlen("s:low") == 1
    assert await redis_client.zcard(ZSET) == 0


async def test_promote_empty_is_noop(redis_client):
    await delayed.promote(redis_client, ZSET, [], [])
    assert await redis_client.xlen(STREAM) == 0
