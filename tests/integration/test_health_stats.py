from uuid import uuid4

from app import repository as repo
from app.queue.consumer import ensure_group
from app.schemas.enums import JobType


async def _reset_streams(r, s):
    await r.flushdb()
    for stream in s.ordered_streams:
        await ensure_group(r, stream, s.consumer_group)


async def test_stats_reports_queue_and_job_metrics(client, db_session, redis_client):
    r = redis_client
    s = client.app.state.settings
    await _reset_streams(r, s)

    # high: 3 waiting, none delivered -> depth 3, in_flight 0
    for _ in range(3):
        await r.xadd(s.stream_high, {"job_id": str(uuid4())})
    # normal: 2 added, 1 delivered to consumer "w1" -> depth 1, in_flight 1
    for _ in range(2):
        await r.xadd(s.stream_normal, {"job_id": str(uuid4())})
    await r.xreadgroup(
        groupname=s.consumer_group,
        consumername="w1",
        streams={s.stream_normal: ">"},
        count=1,
    )
    # one delayed (scheduled) member
    await r.zadd(s.delayed_zset, {str(uuid4()): 9999999999})

    # DB rows across statuses
    await repo.create_job(
        db_session, JobType.email, {"to": "a", "subject": "b"}
    )  # pending
    done = await repo.create_job(db_session, JobType.email, {"to": "a", "subject": "b"})
    await repo.claim_job(db_session, done.id)
    await repo.complete_job(db_session, done.id, {"ok": True})  # completed

    body = client.get("/stats").json()

    assert body["queue"]["streams"]["high"] == {"depth": 3, "in_flight": 0}
    assert body["queue"]["streams"]["normal"] == {"depth": 1, "in_flight": 1}
    assert body["queue"]["streams"]["low"] == {"depth": 0, "in_flight": 0}
    assert body["queue"]["scheduled"] == 1
    assert body["queue"]["workers"] == 1
    assert body["jobs"]["by_status"]["pending"] == 1
    assert body["jobs"]["by_status"]["completed"] == 1
    assert body["jobs"]["by_status"]["failed"] == 0
    assert body["jobs"]["oldest_pending_age_seconds"] is not None
    assert body["jobs"]["oldest_pending_age_seconds"] >= 0


async def test_stats_workers_excludes_reaper_consumer(client, db_session, redis_client):
    from app.ticker.runner import reap_stale

    r = redis_client
    s = client.app.state.settings
    await _reset_streams(r, s)

    # One real worker: deliver a message to consumer "w1".
    await r.xadd(s.stream_normal, {"job_id": str(uuid4())})
    await r.xreadgroup(
        groupname=s.consumer_group,
        consumername="w1",
        streams={s.stream_normal: ">"},
        count=1,
    )

    # Run the real reaper sweep with nothing stale to reclaim. XAUTOCLAIM
    # registers its own consumer name in the group as a side effect even when
    # it claims zero messages, so this leaves a "reaper" consumer behind on
    # every stream.
    await reap_stale(db_session, r, s)

    body = client.get("/stats").json()

    assert body["queue"]["workers"] == 1


def test_stats_503_when_redis_down(client):
    from app.core.redis import create_redis_client

    client.app.state.redis = create_redis_client("redis://127.0.0.1:6390/0")
    resp = client.get("/stats")
    assert resp.status_code == 503
    assert resp.json() == {"detail": "stats unavailable"}
