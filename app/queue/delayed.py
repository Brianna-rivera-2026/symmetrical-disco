import redis.asyncio as redis


async def schedule(client: redis.Redis, zset: str, job_id: str, score: float) -> None:
    await client.zadd(zset, {job_id: score})


async def due_job_ids(
    client: redis.Redis, zset: str, now_epoch: float, limit: int
) -> list[str]:
    return await client.zrangebyscore(zset, min=0, max=now_epoch, start=0, num=limit)


async def promote(
    client: redis.Redis,
    zset: str,
    routed: list[tuple[str, dict]],
    all_ids: list[str],
) -> None:
    if not all_ids:
        return
    # XADD every routed message to its target stream BEFORE removing any id
    # from the ZSET, so a crash mid-promotion leaves the ids in the ZSET to be
    # retried next tick. Duplicate stream entries are absorbed by the worker's
    # idempotent claim guard.
    async with client.pipeline(transaction=False) as pipe:
        for stream, fields in routed:
            pipe.xadd(stream, fields)
        await pipe.execute()
    await client.zrem(zset, *all_ids)
