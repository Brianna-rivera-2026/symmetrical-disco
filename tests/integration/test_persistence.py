import time
import uuid
from pathlib import Path

import docker
import pytest
import yaml
from testcontainers.redis import RedisContainer

from app.core.redis import create_redis_client

COMPOSE_PATH = Path(__file__).resolve().parents[2] / "docker-compose.yml"


def _load_compose_services() -> dict:
    compose = yaml.safe_load(COMPOSE_PATH.read_text())
    return compose["services"]


def test_compose_services_declare_persistent_volumes():
    services = _load_compose_services()

    postgres_volumes = services["postgres"].get("volumes", [])
    assert any(v.endswith(":/var/lib/postgresql/data") for v in postgres_volumes), (
        "postgres service must mount a named volume at /var/lib/postgresql/data"
    )

    redis_service = services["redis"]
    redis_volumes = redis_service.get("volumes", [])
    assert any(v.endswith(":/data") for v in redis_volumes), (
        "redis service must mount a named volume at /data for persistence"
    )
    command = redis_service.get("command", "")
    assert "--appendonly yes" in command, "redis must run with AOF enabled"


def test_redis_state_survives_container_recreation_with_compose_config():
    redis_command = _load_compose_services()["redis"]["command"]
    docker_client = docker.from_env()
    volume_name = f"test-redis-persist-{uuid.uuid4().hex[:8]}"
    docker_client.volumes.create(name=volume_name)
    try:
        first = RedisContainer("redis:7").with_volume_mapping(
            volume_name, "/data", mode="rw"
        )
        first.with_command(redis_command)
        first.start()
        try:
            url = (
                f"redis://{first.get_container_host_ip()}:"
                f"{first.get_exposed_port(6379)}/0"
            )
            client = create_redis_client(url)
            client.xadd("teststream", {"job_id": "abc"})
            client.zadd("jobs:delayed", {"job-2": 123456.0})
            # appendfsync everysec flushes on a ~1s cycle; wait past it before
            # we pull the container out from under the writes.
            time.sleep(1.5)
            client.close()
        finally:
            first.stop()  # stops + removes the container; the named volume is untouched

        second = RedisContainer("redis:7").with_volume_mapping(
            volume_name, "/data", mode="rw"
        )
        second.with_command(redis_command)
        second.start()
        try:
            url = (
                f"redis://{second.get_container_host_ip()}:"
                f"{second.get_exposed_port(6379)}/0"
            )
            client = create_redis_client(url)
            entries = client.xrange("teststream", "-", "+")
            assert len(entries) == 1
            assert entries[0][1] == {"job_id": "abc"}
            assert client.zscore("jobs:delayed", "job-2") == 123456.0
            client.close()
        finally:
            second.stop()
    finally:
        docker_client.volumes.get(volume_name).remove(force=True)
