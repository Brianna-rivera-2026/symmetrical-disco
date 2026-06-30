import redis


def enqueue(client: redis.Redis, stream: str, job_id: str) -> str:
    return client.xadd(stream, {"job_id": job_id})
