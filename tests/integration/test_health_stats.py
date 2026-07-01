from uuid import uuid4

from app import repository as repo
from app.queue.consumer import ensure_group
from app.schemas.enums import JobType


def _reset_streams(r, s):
    r.flushdb()
    for stream in s.ordered_streams:
        ensure_group(r, stream, s.consumer_group)


def test_stats_reports_queue_and_job_metrics(client, db_session):
    r = client.app.state.redis
    s = client.app.state.settings
    _reset_streams(r, s)

    # high: 3 waiting, none delivered -> depth 3, in_flight 0
    for _ in range(3):
        r.xadd(s.stream_high, {"job_id": str(uuid4())})
    # normal: 2 added, 1 delivered to consumer "w1" -> depth 1, in_flight 1
    for _ in range(2):
        r.xadd(s.stream_normal, {"job_id": str(uuid4())})
    r.xreadgroup(
        groupname=s.consumer_group,
        consumername="w1",
        streams={s.stream_normal: ">"},
        count=1,
    )
    # one delayed (scheduled) member
    r.zadd(s.delayed_zset, {str(uuid4()): 9999999999})

    # DB rows across statuses
    repo.create_job(db_session, JobType.email, {"to": "a", "subject": "b"})  # pending
    done = repo.create_job(db_session, JobType.email, {"to": "a", "subject": "b"})
    repo.claim_job(db_session, done.id)
    repo.complete_job(db_session, done.id, {"ok": True})  # completed

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


def test_stats_503_when_redis_down(client):
    from app.core.redis import create_redis_client

    client.app.state.redis = create_redis_client("redis://127.0.0.1:6390/0")
    resp = client.get("/stats")
    assert resp.status_code == 503
    assert resp.json() == {"detail": "stats unavailable"}
