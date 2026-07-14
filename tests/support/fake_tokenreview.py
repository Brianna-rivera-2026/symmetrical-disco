"""Fake Kubernetes TokenReview endpoint for tests and docker-compose dev.

Answers POST /apis/authentication.k8s.io/v1/tokenreviews from a static
token map. Runnable standalone as the compose sidecar:

    python -m tests.support.fake_tokenreview

Env (standalone mode): FAKE_TOKENS — JSON object mapping raw token to
{"username": ..., "uid": ..., "groups": [...]}; PORT (default 8443).
"""

import json
import logging
import os

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

log = logging.getLogger("app.dev.fake_tokenreview")

DEFAULT_DEV_TOKENS: dict[str, dict] = {
    "dev-alice": {
        "username": "alice",
        "uid": "aaaaaaaa-0000-4000-8000-000000000001",
        "groups": ["jobprocessor-users"],
    },
    "dev-bob": {
        "username": "bob",
        "uid": "bbbbbbbb-0000-4000-8000-000000000002",
        "groups": ["jobprocessor-users"],
    },
}


def create_fake_tokenreview(
    tokens: dict[str, dict],
    *,
    required_sa_token: str | None = None,
    fail: bool = False,
) -> FastAPI:
    app = FastAPI()

    @app.get("/health")
    async def health() -> dict:
        return {"status": "ok"}

    @app.post("/apis/authentication.k8s.io/v1/tokenreviews")
    async def review(request: Request) -> JSONResponse:
        if fail:
            return JSONResponse(status_code=500, content={"message": "boom"})
        if required_sa_token is not None:
            if request.headers.get("authorization") != f"Bearer {required_sa_token}":
                return JSONResponse(
                    status_code=401, content={"message": "Unauthorized"}
                )
        body = await request.json()
        info = tokens.get(body.get("spec", {}).get("token", ""))
        if info is None:
            status: dict = {"authenticated": False}
        else:
            status = {
                "authenticated": True,
                "user": {
                    "username": info["username"],
                    "uid": info["uid"],
                    "groups": list(info["groups"]),
                },
            }
        return JSONResponse(
            status_code=201,
            content={
                "apiVersion": "authentication.k8s.io/v1",
                "kind": "TokenReview",
                "status": status,
            },
        )

    return app


def main() -> None:
    import uvicorn

    raw = os.environ.get("FAKE_TOKENS", "")
    tokens = json.loads(raw) if raw else DEFAULT_DEV_TOKENS
    log.info("fake_tokenreview.starting", extra={"users": sorted(tokens)})
    uvicorn.run(
        create_fake_tokenreview(tokens),
        host="0.0.0.0",
        port=int(os.environ.get("PORT", "8443")),
    )


if __name__ == "__main__":
    main()
