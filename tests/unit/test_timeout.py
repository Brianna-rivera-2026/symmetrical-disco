import asyncio

import pytest

from app.worker.timeout import HandlerTimeout, run_with_timeout


async def test_returns_value_when_fast():
    async def fast():
        return 21 * 2

    assert await run_with_timeout(fast(), timeout_s=1.0) == 42


async def test_raises_handler_timeout_and_cancels_when_slow():
    cancelled = asyncio.Event()

    async def slow():
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            cancelled.set()
            raise

    with pytest.raises(HandlerTimeout):
        await run_with_timeout(slow(), timeout_s=0.05)
    assert cancelled.is_set()


async def test_propagates_handler_exception():
    async def boom():
        raise ValueError("nope")

    with pytest.raises(ValueError):
        await run_with_timeout(boom(), timeout_s=1.0)
