import json

from fastapi.testclient import TestClient
from sqlalchemy import text

from app.main import create_app
from app.users import sync

EMAIL_JOB = {"type": "email", "payload": {"to": "a@b.com", "subject": "Hi"}}


def test_sync_then_scoped_access(pg_engine, test_settings, tmp_path):
    keyfile = tmp_path / "api_user_keys.json"
    keyfile.write_text(json.dumps({"alice": "alice-key", "bob": "bob-key"}))
    settings = test_settings.model_copy(update={"api_user_keys_file": str(keyfile)})

    assert sync.run(settings) == 0

    app = create_app(settings)
    try:
        with TestClient(app) as c:
            created = c.post(
                "/jobs", json=EMAIL_JOB, headers={"X-API-Key": "alice-key"}
            )
            assert created.status_code == 202
            job_id = created.json()["id"]

            as_alice = c.get(f"/jobs/{job_id}", headers={"X-API-Key": "alice-key"})
            assert as_alice.status_code == 200

            as_bob = c.get(f"/jobs/{job_id}", headers={"X-API-Key": "bob-key"})
            assert as_bob.status_code == 404

            no_key = c.get(f"/jobs/{job_id}")
            assert no_key.status_code == 401
    finally:
        with pg_engine.begin() as conn:
            conn.execute(text("TRUNCATE TABLE jobs, users"))
