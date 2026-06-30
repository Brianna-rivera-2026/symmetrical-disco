def test_health(client):
    assert client.get("/health").json() == {"status": "ok"}


def test_submit_creates_job_and_enqueues(client):
    resp = client.post(
        "/jobs", json={"type": "email", "payload": {"to": "a@b.com", "subject": "Hi"}}
    )
    assert resp.status_code == 202
    body = resp.json()
    assert body["status"] == "pending"
    assert body["type"] == "email"

    # Job is fetchable.
    got = client.get(f"/jobs/{body['id']}")
    assert got.status_code == 200
    assert got.json()["payload"]["to"] == "a@b.com"

    # And a message was enqueued on the stream.
    redis_client = client.app.state.redis
    stream = client.app.state.settings.jobs_stream
    assert redis_client.xlen(stream) == 1


def test_submit_rejects_bad_payload(client):
    resp = client.post(
        "/jobs", json={"type": "email", "payload": {"subject": "no recipient"}}
    )
    assert resp.status_code == 422


def test_submit_rejects_unknown_type(client):
    resp = client.post("/jobs", json={"type": "translate", "payload": {}})
    assert resp.status_code == 422


def test_get_missing_returns_404(client):
    import uuid

    assert client.get(f"/jobs/{uuid.uuid4()}").status_code == 404


def test_list_filters_by_type(client):
    client.post(
        "/jobs", json={"type": "email", "payload": {"to": "a@b.com", "subject": "Hi"}}
    )
    client.post("/jobs", json={"type": "report", "payload": {"report_type": "sales"}})
    resp = client.get("/jobs", params={"type": "email"})
    assert resp.status_code == 200
    items = resp.json()["items"]
    assert len(items) == 1
    assert items[0]["type"] == "email"
