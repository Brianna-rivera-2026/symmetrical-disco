import time

from app.queue import delayed

ZSET = "jobs:delayed"
STREAM = "jobs:stream"


def test_schedule_and_due_filtering(redis_client):
    delayed.schedule(redis_client, ZSET, "past", time.time() - 10)
    delayed.schedule(redis_client, ZSET, "future", time.time() + 1000)
    due = delayed.due_job_ids(redis_client, ZSET, time.time(), limit=100)
    assert due == ["past"]


def test_due_respects_limit(redis_client):
    for i in range(5):
        delayed.schedule(redis_client, ZSET, f"j{i}", time.time() - i - 1)
    due = delayed.due_job_ids(redis_client, ZSET, time.time(), limit=2)
    assert len(due) == 2


def test_promote_moves_ids_to_stream_and_removes(redis_client):
    delayed.schedule(redis_client, ZSET, "a", time.time() - 1)
    delayed.schedule(redis_client, ZSET, "b", time.time() - 1)
    delayed.promote(redis_client, STREAM, ZSET, ["a", "b"])
    assert redis_client.xlen(STREAM) == 2
    assert redis_client.zcard(ZSET) == 0


def test_promote_empty_is_noop(redis_client):
    delayed.promote(redis_client, STREAM, ZSET, [])
    assert redis_client.xlen(STREAM) == 0
