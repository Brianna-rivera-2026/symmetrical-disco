import signal

import httpx
import pytest
import pytest_asyncio
import redis.asyncio as redis_lib

from app.core.healthcheck import Heartbeat, HealthServer


@pytest_asyncio.fixture(loop_scope="function")
async def health_server(pg_engine, redis_client):
    heartbeat = Heartbeat()
    server = HealthServer(
        port=0,
        heartbeat=heartbeat,
        max_heartbeat_age_s=30.0,
        engine=pg_engine,
        redis_client=redis_client,
    )
    await server.start()
    yield server, heartbeat
    await server.stop()


async def _get(server: HealthServer, path: str) -> httpx.Response:
    async with httpx.AsyncClient() as client:
        return await client.get(f"http://127.0.0.1:{server.port}{path}", timeout=5.0)


async def test_health_ok_when_heartbeat_fresh(health_server):
    server, heartbeat = health_server
    heartbeat.beat()
    response = await _get(server, "/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok", "checks": {"loop": "ok"}}


async def test_health_503_when_heartbeat_stale(pg_engine, redis_client):
    server = HealthServer(
        port=0,
        heartbeat=Heartbeat(),
        max_heartbeat_age_s=0.0,  # everything is instantly stale
        engine=pg_engine,
        redis_client=redis_client,
    )
    await server.start()
    try:
        response = await _get(server, "/health")
        assert response.status_code == 503
        assert response.json()["status"] == "unavailable"
    finally:
        await server.stop()


async def test_ready_ok_with_live_dependencies(health_server):
    server, _ = health_server
    response = await _get(server, "/ready")
    assert response.status_code == 200
    assert response.json()["checks"] == {"postgres": "ok", "redis": "ok"}


async def test_ready_503_when_redis_down(pg_engine):
    dead = redis_lib.Redis(host="127.0.0.1", port=1, socket_connect_timeout=0.2)
    server = HealthServer(
        port=0,
        heartbeat=Heartbeat(),
        max_heartbeat_age_s=30.0,
        engine=pg_engine,
        redis_client=dead,
    )
    await server.start()
    try:
        response = await _get(server, "/ready")
        assert response.status_code == 503
        assert response.json()["checks"]["redis"] == "error"
        assert response.json()["checks"]["postgres"] == "ok"
    finally:
        await server.stop()


async def test_ready_returns_503_while_draining(pg_engine, redis_client):
    server = HealthServer(
        port=0,
        heartbeat=Heartbeat(),
        max_heartbeat_age_s=30.0,
        engine=pg_engine,
        redis_client=redis_client,
        draining=lambda: True,
    )
    await server.start()
    try:
        response = await _get(server, "/ready")
        assert response.status_code == 503
        assert response.json() == {
            "status": "draining",
            "checks": {"draining": "true"},
        }
        health = await _get(server, "/health")
        assert health.status_code == 200  # draining is not dead
    finally:
        await server.stop()


async def test_unknown_path_404(health_server):
    server, _ = health_server
    assert (await _get(server, "/nope")).status_code == 404


async def test_start_raises_cleanly_on_port_conflict(pg_engine, redis_client):
    """Regression: a bind conflict must surface as a normal exception from
    start(), not crash the process. Previously uvicorn's Server.startup()
    caught the OSError itself and called sys.exit(1) inside the task
    coroutine; asyncio re-raises that SystemExit immediately instead of
    storing it as a task result, tearing through the event loop instead of
    giving the caller anything catchable."""
    first = HealthServer(
        port=0,
        heartbeat=Heartbeat(),
        max_heartbeat_age_s=30.0,
        engine=pg_engine,
        redis_client=redis_client,
    )
    await first.start()
    used_port = first.port

    second = HealthServer(
        port=used_port,
        heartbeat=Heartbeat(),
        max_heartbeat_age_s=30.0,
        engine=pg_engine,
        redis_client=redis_client,
    )
    try:
        with pytest.raises(OSError):
            await second.start()
    finally:
        await first.stop()


async def test_worker_sigterm_handler_survives_health_server_running(
    pg_engine, redis_client
):
    """Regression: uvicorn.Server.serve() wraps _serve() in
    `with self.capture_signals(): ...`, which unconditionally calls
    `signal.signal(sig, self.handle_exit)` for SIGINT/SIGTERM on the main
    thread for the entire time serve() is running -- i.e. this health
    server's whole lifetime. Left unchecked, this silently overrides the
    worker/ticker's own `signal.signal(signal.SIGTERM, _request_stop)`
    handler installed at startup, so a real SIGTERM sent to the worker would
    hit uvicorn's handle_exit (which only flips server.should_exit) instead
    of the worker's own graceful-shutdown flag. HealthServer must prevent
    uvicorn from touching process signal handlers at all."""

    def sentinel(signum, frame):  # pragma: no cover - never actually invoked
        raise AssertionError("sentinel handler should not be called by uvicorn")

    previous = signal.signal(signal.SIGTERM, sentinel)
    try:
        assert signal.getsignal(signal.SIGTERM) is sentinel

        server = HealthServer(
            port=0,
            heartbeat=Heartbeat(),
            max_heartbeat_age_s=30.0,
            engine=pg_engine,
            redis_client=redis_client,
        )
        await server.start()
        try:
            # While the health server is running, the worker's own SIGTERM
            # handler must still be installed -- uvicorn must not have
            # swapped in its own handle_exit.
            assert signal.getsignal(signal.SIGTERM) is sentinel
        finally:
            await server.stop()

        # And still intact after stop() too.
        assert signal.getsignal(signal.SIGTERM) is sentinel
    finally:
        signal.signal(signal.SIGTERM, previous)
