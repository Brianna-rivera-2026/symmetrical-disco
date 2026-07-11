import asyncio
from collections.abc import Awaitable
from typing import TypeVar

T = TypeVar("T")


class HandlerTimeout(Exception):
    """Raised when a job handler exceeds its allotted execution time."""


async def run_with_timeout(coro: Awaitable[T], timeout_s: float) -> T:
    """Await `coro` for up to `timeout_s`, then cancel it.

    Cancellation is native asyncio: the handler task is cancelled and awaited
    to completion, so nothing is orphaned and no process recycle is needed.
    """
    try:
        return await asyncio.wait_for(coro, timeout=timeout_s)
    except TimeoutError as exc:
        raise HandlerTimeout(f"handler exceeded {timeout_s}s") from exc
