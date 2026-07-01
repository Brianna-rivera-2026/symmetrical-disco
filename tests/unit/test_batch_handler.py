import pytest

from app.jobs import handlers
from app.jobs.handlers import JobCancelled, handle_batch
from app.schemas.payloads import BatchPayload


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch):
    monkeypatch.setattr(handlers.time, "sleep", lambda *_: None)


class _FakeCtx:
    def __init__(self, cancel_after=None):
        self.cancel_after = cancel_after
        self.calls = 0
        self.progress = []

    def cancelled(self) -> bool:
        hit = self.cancel_after is not None and self.calls >= self.cancel_after
        self.calls += 1
        return hit

    def set_progress(self, pct: int) -> None:
        self.progress.append(pct)


def test_batch_summarizes_success_and_failure():
    payload = BatchPayload(items=[{"ok": 1}, {"fail": True}, {"ok": 2}])
    out = handle_batch(payload, _FakeCtx())
    assert out["total"] == 3
    assert out["succeeded"] == 2
    assert out["failed"] == 1
    assert out["errors"][0]["index"] == 1
    assert "progress" not in out  # progress lives on the row, not in the summary


def test_batch_completes_even_if_all_fail():
    payload = BatchPayload(items=[{"fail": True}, {"fail": True}])
    out = handle_batch(payload, _FakeCtx())
    assert out == {
        "total": 2,
        "succeeded": 0,
        "failed": 2,
        "errors": [
            {"index": 0, "error": "item failed"},
            {"index": 1, "error": "item failed"},
        ],
    }


def test_batch_raises_jobcancelled_with_partial_summary():
    ctx = _FakeCtx(cancel_after=2)  # first two items processed, then cancel
    payload = BatchPayload(items=[{}, {}, {}, {}])
    with pytest.raises(JobCancelled) as exc:
        handle_batch(payload, ctx)
    assert exc.value.summary["succeeded"] == 2
    assert exc.value.summary["total"] == 4


def test_batch_reports_progress_per_item():
    ctx = _FakeCtx()
    handle_batch(BatchPayload(items=[{}, {}, {}, {}]), ctx)
    assert ctx.progress == [25, 50, 75, 100]
