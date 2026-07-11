import asyncio
import socket
import time

import redis.asyncio as redis
import uvicorn
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from app.core.config import Settings


def worker_heartbeat_threshold_s(settings: Settings) -> float:
    """A worker legitimately blocks on XREADGROUP then awaits a job slot; busy
    is healthy, hung is not."""
    return settings.block_ms / 1000 + settings.job_handler_timeout_s + 10.0


def ticker_heartbeat_threshold_s(settings: Settings) -> float:
    return max(10.0, 5 * settings.ticker_interval_s)


class Heartbeat:
    """Last-beat tracker for the main loop (single event loop: plain attribute)."""

    def __init__(self) -> None:
        self._last = time.monotonic()

    def beat(self) -> None:
        self._last = time.monotonic()

    def age_seconds(self) -> float:
        return time.monotonic() - self._last


_REDIS_PROBE_TIMEOUT_S = (
    2.0  # /ready must fail fast despite the client's generous 5s/10s socket timeouts
)
_LISTEN_BACKLOG = 100  # matches uvicorn's own default backlog


class HealthServer:
    """Uvicorn server task on the shared event loop: /health = liveness (loop
    heartbeat — a blocked loop also simply never answers, so the probe times
    out and the orchestrator restarts the pod), /ready = readiness probing the
    app's own async engine pool and Redis client."""

    def __init__(
        self,
        port: int,
        heartbeat: Heartbeat,
        max_heartbeat_age_s: float,
        engine: AsyncEngine,
        redis_client: redis.Redis,
    ) -> None:
        self._heartbeat = heartbeat
        self._max_age = max_heartbeat_age_s
        self._engine = engine
        self._redis = redis_client
        self._task: asyncio.Task | None = None

        app = FastAPI()

        @app.get("/health")
        async def health() -> JSONResponse:
            age = self._heartbeat.age_seconds()
            if age <= self._max_age:
                return JSONResponse({"status": "ok", "checks": {"loop": "ok"}})
            return JSONResponse(
                {
                    "status": "unavailable",
                    "checks": {"loop": f"stale ({age:.0f}s > {self._max_age:.0f}s)"},
                },
                status_code=503,
            )

        @app.get("/ready")
        async def ready() -> JSONResponse:
            checks: dict[str, str] = {}
            try:
                async with self._engine.connect() as conn:
                    await conn.execute(text("SELECT 1"))
                checks["postgres"] = "ok"
            except Exception:  # noqa: BLE001 — any failure means not ready
                checks["postgres"] = "error"
            try:
                await asyncio.wait_for(self._redis.ping(), _REDIS_PROBE_TIMEOUT_S)
                checks["redis"] = "ok"
            except (redis.RedisError, TimeoutError):
                checks["redis"] = "error"
            ok = all(value == "ok" for value in checks.values())
            return JSONResponse(
                {"status": "ok" if ok else "unavailable", "checks": checks},
                status_code=200 if ok else 503,
            )

        config = uvicorn.Config(
            app, host="0.0.0.0", port=port, log_level="warning", access_log=False
        )
        self._server = uvicorn.Server(config)
        self._server.install_signal_handlers = lambda: None
        self.port = port

    async def start(self) -> None:
        # Bind the listening socket ourselves, synchronously, before handing it
        # to uvicorn. If we let uvicorn's Server.startup() do the bind, it
        # catches OSError internally and calls sys.exit(1) *inside* the task
        # coroutine; asyncio re-raises SystemExit immediately from task-stepping
        # instead of storing it as a normal task result, which tears through
        # the event loop instead of giving the caller a catchable exception.
        # Binding here means a port conflict raises a plain OSError right here.
        # Deliberately do NOT set SO_REUSEADDR: on Windows it lets a second
        # socket bind to a port a live listener already holds (unlike on
        # Linux, where it only affects TIME_WAIT reuse), which would silently
        # defeat the port-conflict detection this fix exists to provide.
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind(("0.0.0.0", self.port))
        sock.listen(_LISTEN_BACKLOG)
        sock.setblocking(False)
        self.port = sock.getsockname()[1]

        self._task = asyncio.create_task(self._server.serve(sockets=[sock]))
        while not self._server.started:
            if self._task.done():
                self._task.result()  # surface any other startup errors
                raise RuntimeError("health server exited before startup")
            await asyncio.sleep(0.01)
        self.port = self._server.servers[0].sockets[0].getsockname()[1]

    async def stop(self) -> None:
        self._server.should_exit = True
        if self._task is not None:
            await self._task
