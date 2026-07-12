"""Memory-threshold self-recycling: when RSS exceeds the configured cap the
worker stops claiming jobs, drains its TaskGroup, and exits 0 so the
orchestrator replaces the pod (long-term stability against slow leaks)."""

import logging
from collections.abc import Callable

import psutil

from app.core import metrics as app_metrics

log = logging.getLogger("app.worker")

_MB = 1024 * 1024


class MemoryRecycler:
    def __init__(
        self,
        max_rss_mb: int | None,
        rss_bytes: Callable[[], int] | None = None,
    ) -> None:
        self._max_bytes = max_rss_mb * _MB if max_rss_mb is not None else None
        self._rss_bytes = rss_bytes or psutil.Process().memory_info
        self._uses_psutil = rss_bytes is None
        self.triggered = False

    def _read_rss(self) -> int:
        raw = self._rss_bytes()
        return raw.rss if self._uses_psutil else raw

    def should_recycle(self) -> bool:
        if self._max_bytes is None or self.triggered:
            return self.triggered
        rss = self._read_rss()
        if rss > self._max_bytes:
            self.triggered = True
            app_metrics.worker_recycles.add(1)
            log.warning(
                "worker.recycling",
                extra={
                    "rss_mb": round(rss / _MB),
                    "max_rss_mb": round(self._max_bytes / _MB),
                },
            )
        return self.triggered
