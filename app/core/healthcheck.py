import concurrent.futures
import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import redis
from sqlalchemy import text
from sqlalchemy.engine import Engine

from app.core.config import Settings


def worker_heartbeat_threshold_s(settings: Settings) -> float:
    """A worker legitimately blocks on XREADGROUP then runs a job; busy is
    healthy, hung is not."""
    return settings.block_ms / 1000 + settings.job_handler_timeout_s + 10.0


def ticker_heartbeat_threshold_s(settings: Settings) -> float:
    return max(10.0, 5 * settings.ticker_interval_s)


class Heartbeat:
    """Thread-safe last-beat tracker for a main loop. A bare float write is
    GIL-atomic today; the lock makes the invariant explicit and future-proof."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._last = time.monotonic()

    def beat(self) -> None:
        with self._lock:
            self._last = time.monotonic()

    def age_seconds(self) -> float:
        with self._lock:
            return time.monotonic() - self._last


class HealthServer:
    """Daemon-thread HTTP server: /health = liveness (loop heartbeat),
    /ready = readiness probing the app's own engine pool and Redis client."""

    def __init__(
        self,
        port: int,
        heartbeat: Heartbeat,
        max_heartbeat_age_s: float,
        engine: Engine,
        redis_client: redis.Redis,
    ) -> None:
        self._heartbeat = heartbeat
        self._max_age = max_heartbeat_age_s
        self._engine = engine
        self._redis = redis_client
        self._redis_probe_pool = concurrent.futures.ThreadPoolExecutor(
            max_workers=2, thread_name_prefix="health-redis-probe"
        )
        outer = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:  # noqa: N802 — http.server API
                if self.path == "/health":
                    status, body = outer._liveness()
                elif self.path == "/ready":
                    status, body = outer._readiness()
                else:
                    status, body = 404, {"status": "not found"}
                payload = json.dumps(body).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            def log_message(self, *args: object) -> None:
                """Probe hits are noise; suppress default stderr logging."""

        self._server = ThreadingHTTPServer(("0.0.0.0", port), Handler)
        self.port = self._server.server_address[1]
        self._thread = threading.Thread(
            target=self._server.serve_forever, name="health-server", daemon=True
        )

    def _liveness(self) -> tuple[int, dict]:
        age = self._heartbeat.age_seconds()
        if age <= self._max_age:
            return 200, {"status": "ok", "checks": {"loop": "ok"}}
        return 503, {
            "status": "unavailable",
            "checks": {"loop": f"stale ({age:.0f}s > {self._max_age:.0f}s)"},
        }

    def _readiness(self) -> tuple[int, dict]:
        checks: dict[str, str] = {}
        try:
            with self._engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            checks["postgres"] = "ok"
        except Exception:  # noqa: BLE001 — any failure means not ready
            checks["postgres"] = "error"

        # PING is wrapped in a bounded wait: this project's shared Redis client
        # is deliberately configured with generous 5s/10s socket timeouts for
        # normal job-pipeline traffic (see app/core/redis.py), but a readiness
        # probe must fail fast regardless — same rationale as Postgres's
        # pool_timeout=5 in app/core/db.py. 2s is short enough to keep /ready
        # responsive under repeated polling, long enough not to false-positive
        # on ordinary transient latency.
        _REDIS_PROBE_TIMEOUT_S = 2.0
        try:
            future = self._redis_probe_pool.submit(self._redis.ping)
            future.result(timeout=_REDIS_PROBE_TIMEOUT_S)
            checks["redis"] = "ok"
        except (redis.RedisError, concurrent.futures.TimeoutError):
            checks["redis"] = "error"

        ok = all(value == "ok" for value in checks.values())
        return (200 if ok else 503), {
            "status": "ok" if ok else "unavailable",
            "checks": checks,
        }

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._server.shutdown()
        self._server.server_close()
        self._redis_probe_pool.shutdown(wait=False)
