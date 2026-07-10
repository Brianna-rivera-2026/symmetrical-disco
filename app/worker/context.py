import time
from typing import TYPE_CHECKING, Protocol
from uuid import UUID

from sqlalchemy import text

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker


class JobContext(Protocol):
    def set_progress(self, pct: int) -> None: ...
    async def cancelled(self) -> bool: ...


class PgJobContext:
    """Postgres-backed cancel/progress channel for a running handler.

    Polls at most once per `poll_interval_s` (cached between ticks). On a poll it
    writes progress only when the percent changed (a coalesced UPDATE … RETURNING
    that also reads the cancel flag and confirms the row is still 'processing');
    otherwise it does a flag-only SELECT. Opens its own short-lived async session per
    poll on the worker loop.
    """

    def __init__(
        self,
        job_id: UUID | str,
        session_factory: "async_sessionmaker[AsyncSession] | None",
        poll_interval_s: float,
        now: "Callable[[], float]" = time.monotonic,
    ) -> None:
        self._job_id = job_id
        self._sf = session_factory
        self._interval = poll_interval_s
        self._now = now
        self._pending_pct = 0
        self._last_written_pct: int | None = None
        self._last_poll: float | None = None
        self._cached = False

    def set_progress(self, pct: int) -> None:
        self._pending_pct = pct

    async def cancelled(self) -> bool:
        now = self._now()
        if self._last_poll is None or now - self._last_poll >= self._interval:
            if self._pending_pct != self._last_written_pct:
                alive, requested = await self._write(self._pending_pct)
                self._last_written_pct = self._pending_pct
            else:
                alive, requested = await self._read()
            self._cached = requested or not alive
            self._last_poll = now
        return self._cached

    async def _write(self, pct: int) -> tuple[bool, bool]:
        async with self._sf() as session:
            row = (
                await session.execute(
                    text(
                        "UPDATE jobs SET progress = :pct "
                        "WHERE id = :id AND status = 'processing' "
                        "RETURNING cancel_requested_at"
                    ),
                    {"pct": pct, "id": self._job_id},
                )
            ).first()
            await session.commit()
        if row is None:
            return (False, False)  # no longer processing
        return (True, row[0] is not None)

    async def _read(self) -> tuple[bool, bool]:
        async with self._sf() as session:
            row = (
                await session.execute(
                    text("SELECT cancel_requested_at, status FROM jobs WHERE id = :id"),
                    {"id": self._job_id},
                )
            ).first()
        if row is None or row[1] != "processing":
            return (False, False)
        return (True, row[0] is not None)
