import os
import uuid

import redis

CONSUMER_NAME = f"worker_{os.getenv('HOSTNAME', 'local')}_{uuid.uuid4().hex[:6]}"


def ensure_group(client: redis.Redis, stream: str, group: str) -> None:
    try:
        client.xgroup_create(name=stream, groupname=group, id="$", mkstream=True)
    except redis.ResponseError as exc:
        if "BUSYGROUP" not in str(exc):
            raise


def read_one(
    client: redis.Redis, stream: str, group: str, consumer: str, block_ms: int
) -> tuple[str, dict] | None:
    resp = client.xreadgroup(
        groupname=group,
        consumername=consumer,
        streams={stream: ">"},
        count=1,
        block=block_ms,
    )
    if not resp:
        return None
    _stream, messages = resp[0]
    message_id, fields = messages[0]
    return message_id, fields


def ack(client: redis.Redis, stream: str, group: str, message_id: str) -> None:
    client.xack(stream, group, message_id)
