from app.worker.recycler import MemoryRecycler


def test_disabled_when_threshold_is_none():
    recycler = MemoryRecycler(max_rss_mb=None, rss_bytes=lambda: 10**12)
    assert recycler.should_recycle() is False


def test_below_threshold_does_not_trigger():
    recycler = MemoryRecycler(max_rss_mb=100, rss_bytes=lambda: 50 * 1024 * 1024)
    assert recycler.should_recycle() is False
    assert recycler.triggered is False


def test_breach_triggers_and_latches():
    rss = {"value": 50 * 1024 * 1024}
    recycler = MemoryRecycler(max_rss_mb=100, rss_bytes=lambda: rss["value"])
    assert recycler.should_recycle() is False
    rss["value"] = 101 * 1024 * 1024
    assert recycler.should_recycle() is True
    rss["value"] = 10  # latched: dropping back below does not un-trigger
    assert recycler.should_recycle() is True
    assert recycler.triggered is True


def test_breach_logs_warning(caplog):
    import logging

    recycler = MemoryRecycler(max_rss_mb=1, rss_bytes=lambda: 2 * 1024 * 1024)
    with caplog.at_level(logging.WARNING, logger="app.worker"):
        assert recycler.should_recycle() is True
    assert any(r.message == "worker.recycling" for r in caplog.records)
