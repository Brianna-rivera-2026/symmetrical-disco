import asyncio
import contextlib
import socket
import sys
import time
from collections.abc import Callable

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
        *,
        draining: Callable[[], bool] | None = None,
    ) -> None:
        self._heartbeat = heartbeat
        self._max_age = max_heartbeat_age_s
        self._engine = engine
        self._redis = redis_client
        self._draining = draining
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
            if self._draining is not None and self._draining():
                return JSONResponse(
                    {"status": "draining", "checks": {"draining": "true"}},
                    status_code=503,
                )
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
        # uvicorn's Server has no `install_signal_handlers` attribute in this
        # installed version (0.49.0) -- setting one is a silent no-op. The
        # actual mechanism is `Server.serve()` doing
        # `with self.capture_signals(): await self._serve(...)`, which
        # unconditionally calls `signal.signal(sig, self.handle_exit)` for
        # SIGINT/SIGTERM (and SIGBREAK on Windows) on the main thread for the
        # entire duration `serve()` runs -- i.e. for this health server's
        # whole lifetime, silently overriding the worker/ticker's own
        # `signal.signal(signal.SIGTERM, _request_stop)` handler. Overriding
        # the *instance's* `capture_signals` with a no-op context manager
        # prevents uvicorn from ever touching process signal handlers, so the
        # worker's own handler installed at startup remains in effect for the
        # entire time this health server task is running.
        self._server.capture_signals = contextlib.nullcontext
        self.port = port

    async def start(self) -> None:
        # Bind the listening socket ourselves, synchronously, before handing it
        # to uvicorn. If we let uvicorn's Server.startup() do the bind, it
        # catches OSError internally and calls sys.exit(1) *inside* the task
        # coroutine; asyncio re-raises SystemExit immediately from task-stepping
        # instead of storing it as a normal task result, which tears through
        # the event loop instead of giving the caller a catchable exception.
        # Binding here means a port conflict raises a plain OSError right here.
        #
        # SO_REUSEADDR is set on every POSIX platform to match uvicorn/asyncio's
        # own default bind path (loop.create_server without a pre-made sock=
        # sets it automatically) — without it, a socket lingering in TIME_WAIT
        # after an orchestrator-triggered restart (see class docstring) would
        # spuriously raise "Address already in use" on Linux, a regression
        # relative to the pre-cff49a3 behavior this fix is supposed to preserve.
        #
        # It is deliberately skipped on Windows: Windows' SO_REUSEADDR has
        # looser semantics than POSIX (it can let a second socket bind to a
        # port a live listener already holds, not just one in TIME_WAIT),
        # which would silently defeat the port-conflict detection this fix
        # exists to provide, and would break
        # test_start_raises_cleanly_on_port_conflict on this platform.
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            if sys.platform != "win32":
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind(("0.0.0.0", self.port))
        except BaseException:
            sock.close()
            raise
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
