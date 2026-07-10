import os
import uuid

import redis.asyncio as redis

CONSUMER_NAME = f"worker_{os.getenv('HOSTNAME', 'local')}_{uuid.uuid4().hex[:6]}"
REAPER_NAME = "reaper"


async def ensure_group(client: redis.Redis, stream: str, group: str) -> None:
    try:
        await client.xgroup_create(name=stream, groupname=group, id="$", mkstream=True)
    except redis.ResponseError as exc:
        if "BUSYGROUP" not in str(exc):
            raise


async def ack(client: redis.Redis, stream: str, group: str, message_id: str) -> None:
    await client.xack(stream, group, message_id)


def _flatten(resp) -> list[tuple[str, str, dict]]:
    out: list[tuple[str, str, dict]] = []
    if not resp:
        return out
    for stream, messages in resp:
        for message_id, fields in messages:
            out.append((stream, message_id, fields))
    return out


async def read_priority(
    client: redis.Redis,
    streams: list[str],
    group: str,
    consumer: str,
    block_ms: int,
    count: int = 1,
) -> list[tuple[str, str, dict]]:
    # Strict priority: probe each stream highest-first, non-blocking. The first
    # non-empty stream's messages are returned immediately, so a higher-priority
    # backlog is fully drained before a lower stream is even checked.
    for stream in streams:
        msgs = _flatten(
            await client.xreadgroup(
                groupname=group,
                consumername=consumer,
                streams={stream: ">"},
                count=count,
                block=None,
            )
        )
        if msgs:
            return msgs
    # All empty: block across every stream at once (priority order preserved in
    # the reply). Reached only when idle, so it cannot reorder a real backlog.
    return _flatten(
        await client.xreadgroup(
            groupname=group,
            consumername=consumer,
            streams={stream: ">" for stream in streams},
            count=count,
            block=block_ms,
        )
    )
