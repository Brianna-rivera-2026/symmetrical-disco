import httpx
import pytest
import redis as redis_lib

from app.core.healthcheck import Heartbeat, HealthServer


@pytest.fixture
def health_server(pg_engine, redis_client):
    heartbeat = Heartbeat()
    server = HealthServer(
        port=0,
        heartbeat=heartbeat,
        max_heartbeat_age_s=30.0,
        engine=pg_engine,
        redis_client=redis_client,
    )
    server.start()
    yield server, heartbeat
    server.stop()


def _get(server: HealthServer, path: str) -> httpx.Response:
    return httpx.get(f"http://127.0.0.1:{server.port}{path}", timeout=5.0)


def test_health_ok_when_heartbeat_fresh(health_server):
    server, heartbeat = health_server
    heartbeat.beat()
    response = _get(server, "/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok", "checks": {"loop": "ok"}}


def test_health_503_when_heartbeat_stale(pg_engine, redis_client):
    server = HealthServer(
        port=0,
        heartbeat=Heartbeat(),
        max_heartbeat_age_s=0.0,  # everything is instantly stale
        engine=pg_engine,
        redis_client=redis_client,
    )
    server.start()
    try:
        response = _get(server, "/health")
        assert response.status_code == 503
        assert response.json()["status"] == "unavailable"
    finally:
        server.stop()


def test_ready_ok_with_live_dependencies(health_server):
    server, _ = health_server
    response = _get(server, "/ready")
    assert response.status_code == 200
    assert response.json()["checks"] == {"postgres": "ok", "redis": "ok"}


def test_ready_503_when_redis_down(pg_engine):
    dead = redis_lib.Redis(host="127.0.0.1", port=1, socket_connect_timeout=0.2)
    server = HealthServer(
        port=0,
        heartbeat=Heartbeat(),
        max_heartbeat_age_s=30.0,
        engine=pg_engine,
        redis_client=dead,
    )
    server.start()
    try:
        response = _get(server, "/ready")
        assert response.status_code == 503
        assert response.json()["checks"]["redis"] == "error"
        assert response.json()["checks"]["postgres"] == "ok"
    finally:
        server.stop()


def test_unknown_path_404(health_server):
    server, _ = health_server
    assert _get(server, "/nope").status_code == 404
