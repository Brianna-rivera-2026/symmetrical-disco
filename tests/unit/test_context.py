import pytest

from app.worker.context import PgJobContext


class _FakeCtx(PgJobContext):
    def __init__(self, interval, now_fn):
        super().__init__("jid", None, interval, now=now_fn)
        self.writes = []
        self.reads = 0
        self.alive = True
        self.flag = False

    async def _write(self, pct):
        self.writes.append(pct)
        return (self.alive, self.flag)

    async def _read(self):
        self.reads += 1
        return (self.alive, self.flag)


@pytest.mark.asyncio
async def test_first_call_writes_pending_progress():
    ctx = _FakeCtx(2.0, lambda: 0.0)
    ctx.set_progress(10)
    assert await ctx.cancelled() is False
    assert ctx.writes == [10]


@pytest.mark.asyncio
async def test_skips_poll_within_interval():
    t = [0.0]
    ctx = _FakeCtx(2.0, lambda: t[0])
    ctx.set_progress(10)
    await ctx.cancelled()  # polls at t=0, writes [10]
    ctx.set_progress(20)
    t[0] = 1.0  # < interval -> no poll
    await ctx.cancelled()
    assert ctx.writes == [10]


@pytest.mark.asyncio
async def test_change_only_reads_when_pct_unchanged():
    ctx = _FakeCtx(0.0, lambda: 0.0)  # always past interval
    ctx.set_progress(10)
    await ctx.cancelled()  # writes [10]
    await ctx.cancelled()  # pct unchanged -> read, no write
    assert ctx.writes == [10]
    assert ctx.reads == 1


@pytest.mark.asyncio
async def test_cancel_flag_is_detected():
    ctx = _FakeCtx(0.0, lambda: 0.0)
    ctx.flag = True
    ctx.set_progress(5)
    assert await ctx.cancelled() is True


@pytest.mark.asyncio
async def test_row_gone_stops_the_loop():
    ctx = _FakeCtx(0.0, lambda: 0.0)
    ctx.alive = False
    ctx.set_progress(5)
    assert await ctx.cancelled() is True
