from datetime import datetime, timedelta, timezone

from app.observability import (
    live_worker_count,
    pending_age_seconds,
    zero_fill_status_counts,
)
from app.schemas.enums import JobStatus


def test_live_worker_count_uses_min_idle_across_streams():
    # Worker "w1" is saturated on high (idle 0) but looks stale on low (idle 99999).
    # It MUST count as live because its minimum idle is under the cutoff.
    high = [{"name": "w1", "idle": 0}]
    normal = []
    low = [{"name": "w1", "idle": 99_999}]
    assert live_worker_count([high, normal, low], cutoff_ms=60_000) == 1


def test_live_worker_count_excludes_stale_and_dedups():
    high = [{"name": "w1", "idle": 500}, {"name": "dead", "idle": 120_000}]
    normal = [{"name": "w1", "idle": 800}]  # same worker seen twice -> one
    low = []
    assert live_worker_count([high, normal, low], cutoff_ms=60_000) == 1


def test_zero_fill_status_counts_fills_all_six():
    rows = [(JobStatus.pending, 3), (JobStatus.completed, 10)]
    counts = zero_fill_status_counts(rows)
    assert set(counts) == {s.value for s in JobStatus}
    assert counts["pending"] == 3
    assert counts["completed"] == 10
    assert counts["failed"] == 0


def test_pending_age_seconds_none_when_no_pending():
    assert pending_age_seconds(None, datetime.now(timezone.utc)) is None


def test_pending_age_seconds_computes_delta():
    now = datetime(2026, 7, 1, 12, 0, 0, tzinfo=timezone.utc)
    created = now - timedelta(seconds=42)
    assert pending_age_seconds(created, now) == 42.0
