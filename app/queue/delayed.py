import redis


def schedule(client: redis.Redis, zset: str, job_id: str, score: float) -> None:
    client.zadd(zset, {job_id: score})


def due_job_ids(
    client: redis.Redis, zset: str, now_epoch: float, limit: int
) -> list[str]:
    return client.zrangebyscore(zset, min=0, max=now_epoch, start=0, num=limit)


def promote(client: redis.Redis, stream: str, zset: str, job_ids: list[str]) -> None:
    if not job_ids:
        return
    # XADD every id to the stream BEFORE removing any from the ZSET, so a crash
    # mid-promotion leaves the ids in the ZSET to be retried next tick. Duplicate
    # stream entries are absorbed by the worker's idempotent claim guard.
    pipe = client.pipeline(transaction=False)
    for job_id in job_ids:
        pipe.xadd(stream, {"job_id": job_id})
    pipe.execute()
    client.zrem(zset, *job_ids)
