from unittest.mock import AsyncMock

import pytest

from app.jobs import handlers
from app.jobs.handlers import JobCancelled, handle_batch
from app.schemas.payloads import BatchPayload


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch):
    monkeypatch.setattr(handlers.asyncio, "sleep", AsyncMock())


class _FakeCtx:
    def __init__(self, cancel_after=None):
        self.cancel_after = cancel_after
        self.calls = 0
        self.progress = []

    async def cancelled(self) -> bool:
        hit = self.cancel_after is not None and self.calls >= self.cancel_after
        self.calls += 1
        return hit

    def set_progress(self, pct: int) -> None:
        self.progress.append(pct)


@pytest.mark.asyncio
async def test_batch_dispatches_real_handlers_mixed_success_and_failure(monkeypatch):
    monkeypatch.setattr(
        handlers.random, "random", lambda: 0.05
    )  # forces webhook < 0.2 -> fail
    payload = BatchPayload(
        items=[
            {"type": "email", "to": "a@b.com", "subject": "Hi"},
            {"type": "webhook", "url": "https://x.test"},
            {"type": "report", "report_type": "sales"},
        ]
    )
    out = await handle_batch(payload, _FakeCtx())
    assert out["total"] == 3
    assert out["succeeded"] == 2
    assert out["failed"] == 1
    assert [r["index"] for r in out["results"]] == [0, 2]
    assert "message_id" in out["results"][0]["result"]
    assert "file_url" in out["results"][1]["result"]
    assert out["errors"] == [
        {"index": 1, "error": "webhook call to https://x.test/ failed"}
    ]


@pytest.mark.asyncio
async def test_batch_all_fail_still_completes(monkeypatch):
    monkeypatch.setattr(handlers.random, "random", lambda: 0.05)  # every webhook fails
    payload = BatchPayload(
        items=[
            {"type": "webhook", "url": "https://a.test"},
            {"type": "webhook", "url": "https://b.test"},
        ]
    )
    out = await handle_batch(payload, _FakeCtx())
    assert out["succeeded"] == 0
    assert out["failed"] == 2
    assert out["results"] == []
    assert [e["index"] for e in out["errors"]] == [0, 1]


@pytest.mark.asyncio
async def test_batch_raises_jobcancelled_with_partial_summary():
    ctx = _FakeCtx(cancel_after=2)  # first two items processed, then cancel
    payload = BatchPayload(
        items=[
            {"type": "email", "to": "a@b.com", "subject": "1"},
            {"type": "email", "to": "a@b.com", "subject": "2"},
            {"type": "report", "report_type": "sales"},
            {"type": "report", "report_type": "ops"},
        ]
    )
    with pytest.raises(JobCancelled) as exc:
        await handle_batch(payload, ctx)
    assert exc.value.summary["succeeded"] == 2
    assert exc.value.summary["total"] == 4
    assert len(exc.value.summary["results"]) == 2


@pytest.mark.asyncio
async def test_batch_reports_progress_per_item():
    ctx = _FakeCtx()
    payload = BatchPayload(
        items=[{"type": "email", "to": "a@b.com", "subject": str(i)} for i in range(4)]
    )
    await handle_batch(payload, ctx)
    assert ctx.progress == [25, 50, 75, 100]
