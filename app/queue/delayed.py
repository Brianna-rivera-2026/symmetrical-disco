import redis


def schedule(client: redis.Redis, zset: str, job_id: str, score: float) -> None:
    client.zadd(zset, {job_id: score})


def due_job_ids(
    client: redis.Redis, zset: str, now_epoch: float, limit: int
) -> list[str]:
    return client.zrangebyscore(zset, min=0, max=now_epoch, start=0, num=limit)


def promote(
    client: redis.Redis,
    zset: str,
    routed: list[tuple[str, str]],
    all_ids: list[str],
) -> None:
    if not all_ids:
        return
    # XADD every routed id to its target stream BEFORE removing any from the ZSET,
    # so a crash mid-promotion leaves the ids in the ZSET to be retried next tick.
    # Duplicate stream entries are absorbed by the worker's idempotent claim guard.
    pipe = client.pipeline(transaction=False)
    for stream, job_id in routed:
        pipe.xadd(stream, {"job_id": job_id})
    pipe.execute()
    client.zrem(zset, *all_ids)
