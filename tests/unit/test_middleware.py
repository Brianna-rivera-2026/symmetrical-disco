from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api.middleware import BodySizeLimitMiddleware


def make_client(max_bytes: int = 100) -> TestClient:
    app = FastAPI()

    @app.post("/echo")
    async def echo(payload: dict) -> dict:
        return payload

    app.add_middleware(BodySizeLimitMiddleware, max_bytes=max_bytes)
    return TestClient(app)


def test_under_limit_passes():
    r = make_client().post("/echo", json={"a": 1})
    assert r.status_code == 200


def test_content_length_over_limit_rejected():
    r = make_client().post(
        "/echo", content=b"x" * 200, headers={"content-type": "application/json"}
    )
    assert r.status_code == 413


def test_chunked_body_over_limit_rejected():
    def chunks():
        for _ in range(20):
            yield b"y" * 50  # no Content-Length: httpx sends chunked

    r = make_client().post(
        "/echo", content=chunks(), headers={"content-type": "application/json"}
    )
    assert r.status_code == 413
