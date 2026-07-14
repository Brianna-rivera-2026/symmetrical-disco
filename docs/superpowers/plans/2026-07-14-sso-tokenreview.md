# SSO via Kubernetes TokenReview Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace API-key authentication with cluster SSO: clients present OpenShift OAuth bearer tokens (htpasswd IdP) and the API validates them via the Kubernetes TokenReview API.

**Architecture:** A new `app/auth/` package holds the token cache and a plain-httpx TokenReview client. `get_current_user` validates `Authorization: Bearer` tokens against the apiserver (group-gated, ownership by `status.user.uid`), the `users` table is dropped, and a fake TokenReview ASGI app serves tests and docker-compose. Helm gains a dedicated API ServiceAccount bound to `system:auth-delegator`; a cluster-admin script configures the htpasswd IdP.

**Tech Stack:** FastAPI, httpx (new runtime dep), SQLAlchemy 2.0 + Alembic, pytest + testcontainers, Helm, OpenShift OAuth.

**Spec:** `docs/superpowers/specs/2026-07-14-sso-tokenreview-design.md`

## Global Constraints

- Use `uv` for everything: `uv run pytest`, `uv run ruff check --fix`, `uv run ruff format`, `uv add <pkg>`. Never pip/venv/poetry.
- No `print` — stdlib logging via `logging.getLogger("app.<component>")`.
- Helm/RBAC/IdP/compose changes are verified manually (`helm lint`, `helm template`, compose up) — **no pytest for infra config** (project convention).
- Bearer tokens must never be logged or stored; only SHA-256 hashes may be cached, and cache values are `AuthedUser` only.
- Run the full `uv run pytest` suite before declaring any task complete if it touches `app/` or `tests/`.
- Commit after every task (small, focused commits).

---

### Task 1: `app/auth` package — identity primitives and token cache

The existing `app/users/keys.py` primitives move (renamed) into a new `app/auth/` package. `app/users/` is NOT deleted yet — `deps.py`/`main.py` still import it until Task 5 switches auth over.

**Files:**
- Create: `app/auth/__init__.py` (empty)
- Create: `app/auth/identity.py`
- Create: `app/auth/cache.py`
- Test: `tests/unit/test_auth_cache.py`

**Interfaces:**
- Produces: `app.auth.identity.AuthedUser` (frozen dataclass: `id: uuid.UUID`, `name: str`), `app.auth.identity.hash_token(raw: str) -> str`, `app.auth.cache.TokenCache(ttl_s, max_entries=1024, now=time.monotonic)` with `.get(token_hash) -> AuthedUser | None` and `.put(token_hash, user) -> None`.

- [ ] **Step 1: Add httpx as a runtime dependency** (it is currently dev-only; Task 3 needs it in `app/`)

```bash
uv add "httpx>=0.28.1"
```

- [ ] **Step 2: Write the failing tests**

Create `tests/unit/test_auth_cache.py` — a port of `tests/unit/test_keys.py` to the new names (the old file stays until Task 5):

```python
import hashlib
import uuid

from app.auth.cache import TokenCache
from app.auth.identity import AuthedUser, hash_token


def _user() -> AuthedUser:
    return AuthedUser(id=uuid.uuid4(), name="alice")


def test_hash_token_is_sha256_hex():
    assert hash_token("secret") == hashlib.sha256(b"secret").hexdigest()


def test_cache_miss_returns_none():
    cache = TokenCache(ttl_s=60)
    assert cache.get("nope") is None


def test_cache_put_then_get_returns_user():
    cache = TokenCache(ttl_s=60)
    user = _user()
    cache.put("h1", user)
    assert cache.get("h1") == user


def test_cache_entry_expires_after_ttl():
    clock = {"t": 0.0}
    cache = TokenCache(ttl_s=60, now=lambda: clock["t"])
    cache.put("h1", _user())
    clock["t"] = 59.9
    assert cache.get("h1") is not None
    clock["t"] = 60.0
    assert cache.get("h1") is None


def test_cache_ttl_zero_never_stores():
    cache = TokenCache(ttl_s=0)
    cache.put("h1", _user())
    assert cache.get("h1") is None


def test_cache_is_bounded():
    cache = TokenCache(ttl_s=60, max_entries=2)
    cache.put("h1", _user())
    cache.put("h2", _user())
    cache.put("h3", _user())
    stored = [h for h in ("h1", "h2", "h3") if cache.get(h) is not None]
    assert len(stored) == 2
    assert cache.get("h3") is not None  # newest entry always survives
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `uv run pytest tests/unit/test_auth_cache.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'app.auth'`

- [ ] **Step 4: Implement the package**

Create empty `app/auth/__init__.py`.

Create `app/auth/identity.py`:

```python
"""Identity primitives shared by the TokenReview auth flow."""

import hashlib
import uuid
from dataclasses import dataclass


def hash_token(raw: str) -> str:
    """SHA-256 hex of a raw bearer token — cache-key only, never a stored
    credential. Tokens are high-entropy opaque strings, so a fast unsalted
    hash is the right trade-off."""
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class AuthedUser:
    id: uuid.UUID
    name: str
```

Create `app/auth/cache.py` — the `KeyCache` implementation from `app/users/keys.py` verbatim, renamed:

```python
"""Bounded TTL cache for validated bearer tokens."""

import time
from collections.abc import Callable

from app.auth.identity import AuthedUser


class TokenCache:
    """Bounded TTL cache: token_hash -> AuthedUser. Only successful
    validations are stored, so revocation propagates within one TTL and
    unknown tokens can never validate from cache. Races between requests
    are benign (worst case: a duplicate TokenReview call)."""

    def __init__(
        self,
        ttl_s: float,
        max_entries: int = 1024,
        now: Callable[[], float] = time.monotonic,
    ) -> None:
        self._ttl_s = ttl_s
        self._max = max_entries
        self._now = now
        self._entries: dict[str, tuple[float, AuthedUser]] = {}

    def get(self, token_hash: str) -> AuthedUser | None:
        entry = self._entries.get(token_hash)
        if entry is None:
            return None
        expires_at, user = entry
        if self._now() >= expires_at:
            self._entries.pop(token_hash, None)
            return None
        return user

    def put(self, token_hash: str, user: AuthedUser) -> None:
        if self._ttl_s <= 0:
            return
        if token_hash not in self._entries and len(self._entries) >= self._max:
            self._evict()
        self._entries[token_hash] = (self._now() + self._ttl_s, user)

    def _evict(self) -> None:
        now = self._now()
        for key in [k for k, (exp, _) in self._entries.items() if exp <= now]:
            del self._entries[key]
        while len(self._entries) >= self._max:
            del self._entries[next(iter(self._entries))]  # oldest insertion
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `uv run pytest tests/unit/test_auth_cache.py -v`
Expected: 6 PASS

- [ ] **Step 6: Lint and commit**

```bash
uv run ruff check --fix && uv run ruff format
git add app/auth tests/unit/test_auth_cache.py pyproject.toml uv.lock
git commit -m "feat: add app/auth package with token cache and identity primitives"
```

---

### Task 2: Fake TokenReview endpoint (tests + compose sidecar)

One module serves both purposes: importable factory for tests, `python -m tests.support.fake_tokenreview` for the docker-compose sidecar (the Docker image `COPY . .` includes `tests/`).

**Files:**
- Create: `tests/support/__init__.py` (empty)
- Create: `tests/support/fake_tokenreview.py`
- Test: `tests/unit/test_fake_tokenreview.py`

**Interfaces:**
- Produces: `create_fake_tokenreview(tokens: dict[str, dict], *, required_sa_token: str | None = None, fail: bool = False) -> FastAPI`. `tokens` maps a raw token to `{"username": str, "uid": str, "groups": list[str]}`. When `required_sa_token` is set, requests whose `Authorization` header is not `Bearer <required_sa_token>` get **401** (simulates a stale pod SA token). When `fail=True`, every review returns **500**. Also `DEFAULT_DEV_TOKENS` (dev-alice/dev-bob) used by the compose sidecar.

- [ ] **Step 1: Write the failing tests**

Create `tests/unit/test_fake_tokenreview.py`:

```python
import httpx
import pytest

from tests.support.fake_tokenreview import create_fake_tokenreview

TOKENS = {
    "tok-a": {
        "username": "alice",
        "uid": "11111111-1111-4111-8111-111111111111",
        "groups": ["jobprocessor-users"],
    }
}
URL = "http://fake/apis/authentication.k8s.io/v1/tokenreviews"


def _client(app) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://fake")


def _body(token: str) -> dict:
    return {
        "apiVersion": "authentication.k8s.io/v1",
        "kind": "TokenReview",
        "spec": {"token": token},
    }


async def test_known_token_is_authenticated_with_identity():
    async with _client(create_fake_tokenreview(TOKENS)) as c:
        resp = await c.post(URL, json=_body("tok-a"))
    assert resp.status_code == 201
    status = resp.json()["status"]
    assert status["authenticated"] is True
    assert status["user"]["username"] == "alice"
    assert status["user"]["uid"] == "11111111-1111-4111-8111-111111111111"
    assert status["user"]["groups"] == ["jobprocessor-users"]


async def test_unknown_token_is_unauthenticated():
    async with _client(create_fake_tokenreview(TOKENS)) as c:
        resp = await c.post(URL, json=_body("nope"))
    assert resp.status_code == 201
    assert resp.json()["status"] == {"authenticated": False}


async def test_wrong_sa_token_is_401():
    app = create_fake_tokenreview(TOKENS, required_sa_token="good-sa")
    async with _client(app) as c:
        resp = await c.post(
            URL, json=_body("tok-a"), headers={"Authorization": "Bearer stale-sa"}
        )
    assert resp.status_code == 401


@pytest.mark.parametrize("headers", [{"Authorization": "Bearer good-sa"}])
async def test_correct_sa_token_is_accepted(headers):
    app = create_fake_tokenreview(TOKENS, required_sa_token="good-sa")
    async with _client(app) as c:
        resp = await c.post(URL, json=_body("tok-a"), headers=headers)
    assert resp.status_code == 201


async def test_fail_mode_returns_500():
    async with _client(create_fake_tokenreview(TOKENS, fail=True)) as c:
        resp = await c.post(URL, json=_body("tok-a"))
    assert resp.status_code == 500
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/unit/test_fake_tokenreview.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'tests.support'`

- [ ] **Step 3: Implement the fake**

Create empty `tests/support/__init__.py`.

Create `tests/support/fake_tokenreview.py`:

```python
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
                return JSONResponse(status_code=401, content={"message": "Unauthorized"})
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/unit/test_fake_tokenreview.py -v`
Expected: 5 PASS

- [ ] **Step 5: Lint and commit**

```bash
uv run ruff check --fix && uv run ruff format
git add tests/support tests/unit/test_fake_tokenreview.py
git commit -m "test: add fake TokenReview endpoint for tests and compose"
```

---

### Task 3: Settings for TokenReview

Additive only — `api_user_keys_file` is removed in Task 5 when `app/users/sync.py` dies.

**Files:**
- Modify: `app/core/config.py` (after line 37, `auth_cache_ttl_s: float = 60.0`)
- Test: `tests/unit/test_config.py`

**Interfaces:**
- Produces: `Settings.auth_tokenreview_url`, `auth_sa_token_file`, `auth_ca_file`, `auth_required_group`, `auth_timeout_s` (Task 4/5 consume them).

- [ ] **Step 1: Write the failing test** (append to `tests/unit/test_config.py`; keep whatever Settings-construction helper that file already uses — read it first and match its style)

```python
def test_tokenreview_settings_defaults():
    settings = Settings(database_url="postgresql://x/x", redis_url="redis://x")
    assert settings.auth_tokenreview_url == (
        "https://kubernetes.default.svc/apis/authentication.k8s.io/v1/tokenreviews"
    )
    assert settings.auth_sa_token_file == (
        "/var/run/secrets/kubernetes.io/serviceaccount/token"
    )
    assert settings.auth_ca_file == (
        "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
    )
    assert settings.auth_required_group == "jobprocessor-users"
    assert settings.auth_timeout_s == 2.0
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_config.py -v`
Expected: FAIL — `AttributeError` / pydantic missing attribute

- [ ] **Step 3: Add the settings** in `app/core/config.py` directly below `auth_cache_ttl_s`:

```python
    auth_tokenreview_url: str = (
        "https://kubernetes.default.svc/apis/authentication.k8s.io/v1/tokenreviews"
    )
    auth_sa_token_file: str = "/var/run/secrets/kubernetes.io/serviceaccount/token"
    auth_ca_file: str = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
    auth_required_group: str = "jobprocessor-users"
    auth_timeout_s: float = 2.0
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_config.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add app/core/config.py tests/unit/test_config.py
git commit -m "feat: add TokenReview auth settings"
```

---

### Task 4: TokenReviewer client

**Files:**
- Create: `app/auth/tokenreview.py`
- Test: `tests/unit/test_tokenreview.py`

**Interfaces:**
- Consumes: `create_fake_tokenreview` (Task 2), settings fields (Task 3).
- Produces: `app.auth.tokenreview.TokenReviewer(settings, transport: httpx.AsyncBaseTransport | None = None)` with `async review(user_token: str) -> ReviewedUser | None` (None = cluster rejected the token → caller 401s) raising `TokenReviewUnavailable` on infra failure (→ 503), and `async aclose()`. `ReviewedUser` frozen dataclass: `uid: str`, `username: str`, `groups: tuple[str, ...]`.

- [ ] **Step 1: Write the failing tests**

Create `tests/unit/test_tokenreview.py`:

```python
import httpx
import pytest

from app.auth.tokenreview import ReviewedUser, TokenReviewer, TokenReviewUnavailable
from app.core.config import Settings
from tests.support.fake_tokenreview import create_fake_tokenreview

TOKENS = {
    "tok-a": {
        "username": "alice",
        "uid": "11111111-1111-4111-8111-111111111111",
        "groups": ["jobprocessor-users", "other"],
    }
}


def _settings(tmp_path, sa_token: str | None = None) -> Settings:
    token_file = tmp_path / "sa-token"
    if sa_token is not None:
        token_file.write_text(sa_token)
    return Settings(
        database_url="postgresql://x/x",
        redis_url="redis://x",
        auth_tokenreview_url="http://fake/apis/authentication.k8s.io/v1/tokenreviews",
        auth_sa_token_file=str(token_file),
        auth_ca_file=str(tmp_path / "absent-ca.crt"),
    )


def _reviewer(tmp_path, fake_app, sa_token: str | None = None) -> TokenReviewer:
    return TokenReviewer(
        _settings(tmp_path, sa_token), transport=httpx.ASGITransport(app=fake_app)
    )


async def test_valid_token_returns_reviewed_user(tmp_path):
    reviewer = _reviewer(tmp_path, create_fake_tokenreview(TOKENS))
    user = await reviewer.review("tok-a")
    assert user == ReviewedUser(
        uid="11111111-1111-4111-8111-111111111111",
        username="alice",
        groups=("jobprocessor-users", "other"),
    )
    await reviewer.aclose()


async def test_unknown_token_returns_none(tmp_path):
    reviewer = _reviewer(tmp_path, create_fake_tokenreview(TOKENS))
    assert await reviewer.review("nope") is None
    await reviewer.aclose()


async def test_apiserver_500_raises_unavailable(tmp_path):
    reviewer = _reviewer(tmp_path, create_fake_tokenreview(TOKENS, fail=True))
    with pytest.raises(TokenReviewUnavailable):
        await reviewer.review("tok-a")
    await reviewer.aclose()


async def test_connect_error_raises_unavailable(tmp_path):
    class Boom(httpx.AsyncBaseTransport):
        async def handle_async_request(self, request):
            raise httpx.ConnectError("down", request=request)

    reviewer = TokenReviewer(_settings(tmp_path, "sa"), transport=Boom())
    with pytest.raises(TokenReviewUnavailable):
        await reviewer.review("tok-a")
    await reviewer.aclose()


async def test_sa_rotation_rereads_token_and_retries_once(tmp_path):
    # Apiserver only accepts "new-sa"; reviewer starts holding "old-sa".
    fake = create_fake_tokenreview(TOKENS, required_sa_token="new-sa")
    reviewer = _reviewer(tmp_path, fake, sa_token="old-sa")
    # Kubelet rotated the projected file after startup:
    (tmp_path / "sa-token").write_text("new-sa")
    user = await reviewer.review("tok-a")
    assert user is not None and user.username == "alice"
    await reviewer.aclose()


async def test_persistent_401_raises_unavailable(tmp_path):
    fake = create_fake_tokenreview(TOKENS, required_sa_token="right-sa")
    reviewer = _reviewer(tmp_path, fake, sa_token="wrong-sa")
    with pytest.raises(TokenReviewUnavailable):
        await reviewer.review("tok-a")
    await reviewer.aclose()


async def test_missing_sa_token_file_sends_no_auth_header(tmp_path):
    # Outside a cluster there is no projected token; the fake (no
    # required_sa_token) must still be reachable.
    reviewer = _reviewer(tmp_path, create_fake_tokenreview(TOKENS), sa_token=None)
    assert await reviewer.review("tok-a") is not None
    await reviewer.aclose()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/unit/test_tokenreview.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'app.auth.tokenreview'`

- [ ] **Step 3: Implement the client**

Create `app/auth/tokenreview.py`:

```python
"""Kubernetes TokenReview client (delegated authentication).

The pod's ServiceAccount token authenticates *this service* to the
apiserver; the user's bearer token is the payload being reviewed.
"""

import logging
from dataclasses import dataclass
from pathlib import Path

import httpx

from app.core.config import Settings

log = logging.getLogger("app.auth.tokenreview")


class TokenReviewUnavailable(Exception):
    """The apiserver could not be reached, errored, or rejected OUR
    ServiceAccount credentials. Maps to 503 — never the client's fault."""


@dataclass(frozen=True)
class ReviewedUser:
    uid: str
    username: str
    groups: tuple[str, ...]


class TokenReviewer:
    """The SA token is read once at construction and held in memory. An
    HTTP 401 from the apiserver can only mean our credential is stale
    (a bad *user* token still yields 2xx + authenticated: false), so it
    triggers one re-read of the projected token file and a single retry —
    absorbing kubelet rotation without per-request file reads. The rare
    sync re-read on the event loop is fine: the file lives on tmpfs.
    """

    def __init__(
        self,
        settings: Settings,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self._url = settings.auth_tokenreview_url
        self._token_path = Path(settings.auth_sa_token_file)
        verify: bool | str = True
        ca = Path(settings.auth_ca_file)
        if self._url.startswith("https://") and ca.is_file():
            verify = str(ca)
        self._client = httpx.AsyncClient(
            transport=transport, verify=verify, timeout=settings.auth_timeout_s
        )
        self._sa_token = self._read_sa_token()

    async def aclose(self) -> None:
        await self._client.aclose()

    def _read_sa_token(self) -> str | None:
        try:
            return self._token_path.read_text(encoding="utf-8").strip()
        except OSError:
            # Outside a cluster (tests, compose) there is no projected
            # token; the fake endpoint doesn't authenticate callers.
            return None

    async def _post(self, user_token: str) -> httpx.Response:
        headers = {}
        if self._sa_token:
            headers["Authorization"] = f"Bearer {self._sa_token}"
        return await self._client.post(
            self._url,
            json={
                "apiVersion": "authentication.k8s.io/v1",
                "kind": "TokenReview",
                "spec": {"token": user_token},
            },
            headers=headers,
        )

    async def review(self, user_token: str) -> ReviewedUser | None:
        """None means the cluster rejected the token (caller → 401)."""
        try:
            resp = await self._post(user_token)
            if resp.status_code == 401:
                self._sa_token = self._read_sa_token()
                resp = await self._post(user_token)
        except httpx.HTTPError as exc:
            log.error(
                "auth.tokenreview_unreachable",
                extra={"error_type": type(exc).__name__},
            )
            raise TokenReviewUnavailable(type(exc).__name__) from exc
        if resp.status_code == 401:
            log.error("auth.sa_credentials_rejected")
            raise TokenReviewUnavailable("apiserver rejected ServiceAccount token")
        if resp.status_code >= 300:
            log.error("auth.tokenreview_error", extra={"status": resp.status_code})
            raise TokenReviewUnavailable(f"tokenreview status {resp.status_code}")
        status = resp.json().get("status", {})
        if not status.get("authenticated"):
            return None
        user = status.get("user", {})
        return ReviewedUser(
            uid=user.get("uid", ""),
            username=user.get("username", ""),
            groups=tuple(user.get("groups", ())),
        )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/unit/test_tokenreview.py tests/unit/test_fake_tokenreview.py -v`
Expected: all PASS

- [ ] **Step 5: Lint and commit**

```bash
uv run ruff check --fix && uv run ruff format
git add app/auth/tokenreview.py tests/unit/test_tokenreview.py
git commit -m "feat: add TokenReview client with SA-rotation retry"
```

---

### Task 5: Switchover — bearer auth, drop users table, rewrite tests

The big cut-over: everything in this task changes together because auth, the schema, and the test fixtures are interlocked (the old auth path queries `users`; the FK on `jobs.user_id` rejects cluster UIDs until dropped). Tests are only green again at the end of the task.

**Files:**
- Create: `alembic/versions/0009_drop_users_tokenreview_auth.py`
- Modify: `app/models/job.py`, `app/repository.py`, `app/api/deps.py`, `app/api/routes.py`, `app/main.py`, `app/core/metrics.py:32-34`, `app/core/config.py` (remove `api_user_keys_file`)
- Delete: `app/models/user.py`, `app/users/` (whole package), `tests/unit/test_keys.py`, `tests/integration/test_users_sync.py`, `tests/integration/test_auth_e2e.py`
- Modify: `tests/integration/conftest.py`, `tests/integration/test_auth_api.py`
- Check/Modify: any other test importing `app.users` or seeding users (grep in Step 8)

**Interfaces:**
- Consumes: `TokenCache`, `AuthedUser`, `hash_token` (Task 1), `TokenReviewer`/`TokenReviewUnavailable` (Task 4), fake (Task 2), settings (Task 3).
- Produces: `app.state.token_cache: TokenCache`, `app.state.token_reviewer: TokenReviewer`; `repo.create_job(..., user_name: str | None = None)`; `Job.user_name` column; conftest constants `DEFAULT_TEST_TOKEN`, `SECOND_TEST_TOKEN`, `OUTSIDER_TEST_TOKEN`, `DEFAULT_TEST_UID`, `SECOND_TEST_UID` and fixture `fake_tokenreview_url`.

- [ ] **Step 1: Migration 0009**

Create `alembic/versions/0009_drop_users_tokenreview_auth.py`:

```python
"""drop users table; auth moves to cluster TokenReview

Ownership survives as bare UUIDs stamped from TokenReview's
status.user.uid. Existing jobs.user_id values were app-generated and no
longer resolve to anything — old jobs are effectively unowned (accepted;
see docs/superpowers/specs/2026-07-14-sso-tokenreview-design.md).

Revision ID: 0009
Revises: 0008
Create Date: 2026-07-14

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0009"
down_revision = "0008"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("jobs", sa.Column("user_name", sa.Text(), nullable=True))
    op.drop_constraint("fk_jobs_user_id_users", "jobs", type_="foreignkey")
    op.drop_index("uq_users_key_hash", table_name="users")
    op.drop_index("uq_users_name", table_name="users")
    op.drop_table("users")


def downgrade() -> None:
    # Recreates an EMPTY users table — ownership data is not restorable.
    # user_id values reference cluster UIDs that have no users row, so they
    # must be nulled before the FK can be restored.
    op.create_table(
        "users",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("key_hash", sa.Text(), nullable=False),
        sa.Column(
            "created_at",
            sa.TIMESTAMP(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
    )
    op.create_index("uq_users_name", "users", ["name"], unique=True)
    op.create_index("uq_users_key_hash", "users", ["key_hash"], unique=True)
    op.execute("UPDATE jobs SET user_id = NULL")
    op.create_foreign_key(
        "fk_jobs_user_id_users",
        "jobs",
        "users",
        ["user_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.drop_column("jobs", "user_name")
```

- [ ] **Step 2: Model + repository changes**

In `app/models/job.py`: replace the `user_id` column (lines 20-24) with (no FK) and add `user_name` after it:

```python
    # Cluster identity UID from TokenReview (no local users table to FK to).
    user_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), nullable=True
    )
    user_name: Mapped[str | None] = mapped_column(Text, nullable=True)
```

Delete `app/models/user.py`. Remove the `User` import and export wherever registered (check `app/models/__init__.py`).

In `app/repository.py`:
- Remove `from app.models.user import User` and the `upsert_user` / `get_user_by_key_hash` functions (and now-unused `uuid_mod` / `pg_insert` imports if nothing else uses them — ruff will flag).
- `create_job`: add keyword param `user_name: str | None = None` after `user_id` and pass `user_name=user_name` into the `Job(...)` constructor.

- [ ] **Step 3: Rewrite `get_current_user`**

Replace `app/api/deps.py` content:

```python
import logging
from collections.abc import AsyncIterator
from uuid import UUID

import redis
from fastapi import Depends, HTTPException, Request, Security
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from opentelemetry import trace
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth.cache import TokenCache
from app.auth.identity import AuthedUser, hash_token
from app.auth.tokenreview import TokenReviewer, TokenReviewUnavailable
from app.core import metrics as app_metrics
from app.core.logging import bind_log_context

log = logging.getLogger("app.api")

bearer_scheme = HTTPBearer(auto_error=False)


async def get_db(request: Request) -> AsyncIterator[AsyncSession]:
    factory = request.app.state.session_factory
    async with factory() as session:
        yield session


def get_redis(request: Request) -> redis.Redis:
    return request.app.state.redis


async def get_current_user(
    request: Request,
    credentials: HTTPAuthorizationCredentials | None = Security(bearer_scheme),
) -> AsyncIterator[AuthedUser]:
    """Async yield-dependency: runs in the request's task context, so the
    bound log fields propagate to native async endpoints."""
    if credentials is None:
        app_metrics.auth_validations.add(
            1, {"result": "missing_token", "source": "n/a"}
        )
        log.warning("auth.missing_token")
        raise HTTPException(status_code=401, detail="missing bearer token")

    token_hash = hash_token(credentials.credentials)
    cache: TokenCache = request.app.state.token_cache
    user = cache.get(token_hash)
    source = "cache"
    if user is None:
        source = "tokenreview"
        reviewer: TokenReviewer = request.app.state.token_reviewer
        try:
            reviewed = await reviewer.review(credentials.credentials)
        except TokenReviewUnavailable:
            app_metrics.auth_validations.add(
                1, {"result": "apiserver_error", "source": source}
            )
            raise HTTPException(
                status_code=503, detail="authentication unavailable"
            ) from None
        if reviewed is None:
            app_metrics.auth_validations.add(
                1, {"result": "invalid_token", "source": source}
            )
            log.warning("auth.invalid_token")
            raise HTTPException(status_code=401, detail="invalid token")
        settings = request.app.state.settings
        if settings.auth_required_group not in reviewed.groups:
            app_metrics.auth_validations.add(
                1, {"result": "forbidden_group", "source": source}
            )
            log.warning("auth.forbidden_group", extra={"user_name": reviewed.username})
            raise HTTPException(
                status_code=403, detail="not a member of the required group"
            )
        try:
            uid = UUID(reviewed.uid)
        except ValueError:
            app_metrics.auth_validations.add(
                1, {"result": "invalid_token", "source": source}
            )
            log.warning("auth.non_uuid_uid", extra={"user_name": reviewed.username})
            raise HTTPException(status_code=401, detail="invalid token") from None
        user = AuthedUser(id=uid, name=reviewed.username)
        cache.put(token_hash, user)

    app_metrics.auth_validations.add(1, {"result": "ok", "source": source})
    trace.get_current_span().set_attribute("enduser.id", str(user.id))
    with bind_log_context(user_id=str(user.id), user_name=user.name):
        request.state.authed_user_id = user.id
        request.state.authed_user_name = user.name
        yield user
```

(Note `get_current_user` no longer depends on `get_db` — auth does zero DB work now.)

- [ ] **Step 4: Wire `main.py`, stamp `user_name`, update metric description, delete dead code**

`app/main.py`:
- Replace `from app.users.keys import KeyCache` with:

```python
from app.auth.cache import TokenCache
from app.auth.tokenreview import TokenReviewer
```

- Replace `app.state.key_cache = KeyCache(ttl_s=settings.auth_cache_ttl_s)` with:

```python
    app.state.token_cache = TokenCache(ttl_s=settings.auth_cache_ttl_s)
    app.state.token_reviewer = token_reviewer
```

- In `create_app`, before the `lifespan` definition add `token_reviewer = TokenReviewer(settings)`, and in the lifespan shutdown sequence (after `await redis_client.aclose()`) add `await token_reviewer.aclose()`.

`app/api/routes.py`:
- Change import `from app.users.keys import AuthedUser` → `from app.auth.identity import AuthedUser`.
- `_create_and_handoff(session, client, settings, submission, key, req_hash, user_id)` → add trailing param `user_name`; pass `user_name=user_name` to **both** `repo.create_job(...)` calls.
- Both `submit_job` call sites: `_create_and_handoff(session, client, settings, submission, ..., user.id, user.name)`.

`app/core/metrics.py:33`: description → `"Bearer token auth attempts by result and source"`.

`app/core/config.py`: delete the `api_user_keys_file: str = "/run/secrets/api_user_keys"` line.

Delete `app/models/user.py`, the whole `app/users/` directory, `tests/unit/test_keys.py`, `tests/integration/test_users_sync.py`, `tests/integration/test_auth_e2e.py` (its scoped-access coverage lives on in `test_auth_api.py`; the sync flow no longer exists).

- [ ] **Step 5: Rewrite integration conftest auth fixtures**

In `tests/integration/conftest.py`:

Replace the imports of `app.users.keys` / `repo` usage for users, and the `DEFAULT_TEST_KEY`/`SECOND_TEST_KEY` constants, with:

```python
import time
import threading
import uuid

import uvicorn

from tests.support.fake_tokenreview import create_fake_tokenreview

DEFAULT_TEST_TOKEN = "tok-default"
SECOND_TEST_TOKEN = "tok-second"
OUTSIDER_TEST_TOKEN = "tok-outsider"  # authenticated but not in the group
DEFAULT_TEST_UID = "00000000-0000-4000-8000-000000000001"
SECOND_TEST_UID = "00000000-0000-4000-8000-000000000002"

TEST_TOKENS = {
    DEFAULT_TEST_TOKEN: {
        "username": "default-user",
        "uid": DEFAULT_TEST_UID,
        "groups": ["jobprocessor-users"],
    },
    SECOND_TEST_TOKEN: {
        "username": "second-user",
        "uid": SECOND_TEST_UID,
        "groups": ["jobprocessor-users"],
    },
    OUTSIDER_TEST_TOKEN: {
        "username": "outsider",
        "uid": "00000000-0000-4000-8000-000000000003",
        "groups": ["some-other-group"],
    },
}


@pytest.fixture(scope="session")
def fake_tokenreview_url():
    """Real-socket fake TokenReview server (the app reaches it via httpx
    over the network, exactly like the in-cluster apiserver)."""
    config = uvicorn.Config(
        create_fake_tokenreview(TEST_TOKENS),
        host="127.0.0.1",
        port=0,
        log_level="warning",
    )
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    deadline = time.monotonic() + 10
    while not server.started:
        if time.monotonic() > deadline:
            raise RuntimeError("fake tokenreview server failed to start")
        time.sleep(0.05)
    port = server.servers[0].sockets[0].getsockname()[1]
    yield f"http://127.0.0.1:{port}/apis/authentication.k8s.io/v1/tokenreviews"
    server.should_exit = True
    thread.join(timeout=5)
```

Update `test_settings` to depend on `fake_tokenreview_url` and pass `auth_tokenreview_url=fake_tokenreview_url` (keep existing kwargs).

Replace the `client`, `default_user_id`, `unauth_client`, `second_user`, `owner_id` fixtures:

```python
@pytest.fixture
async def client(pg_engine, test_settings):
    app = create_app(test_settings)
    with TestClient(app) as c:
        c.headers.update({"Authorization": f"Bearer {DEFAULT_TEST_TOKEN}"})
        yield c
    async with pg_engine.begin() as conn:
        await conn.execute(text("TRUNCATE TABLE jobs"))
    real_redis = create_redis_client(test_settings.redis_url)
    try:
        await real_redis.flushdb()
    finally:
        await real_redis.aclose()


@pytest.fixture
def default_user_id():
    """UID the `client` fixture's token resolves to, for tests that create
    jobs via repo.create_job(...) owned by the same user."""
    return uuid.UUID(DEFAULT_TEST_UID)


@pytest.fixture
async def unauth_client(pg_engine, test_settings):
    """TestClient with no Authorization header."""
    app = create_app(test_settings)
    with TestClient(app) as c:
        yield c
    async with pg_engine.begin() as conn:
        await conn.execute(text("TRUNCATE TABLE jobs"))


@pytest.fixture
def second_user():
    """Headers for a second, independent user."""
    return {"Authorization": f"Bearer {SECOND_TEST_TOKEN}"}


@pytest.fixture
def owner_id():
    """An owner id for tests that create jobs directly via the repo — any
    UUID works now (ownership is a bare cluster UID, no FK)."""
    return uuid.uuid4()
```

Also change `db_session`'s teardown `TRUNCATE TABLE jobs, users` → `TRUNCATE TABLE jobs`, and remove the now-unused `hash_key` import.

- [ ] **Step 6: Rewrite `tests/integration/test_auth_api.py`**

```python
import uuid

import pytest

from tests.integration.conftest import OUTSIDER_TEST_TOKEN

EMAIL_JOB = {"type": "email", "payload": {"to": "a@b.com", "subject": "Hi"}}


@pytest.mark.parametrize(
    ("method", "path"),
    [
        ("post", "/jobs"),
        ("get", "/jobs"),
        ("get", f"/jobs/{uuid.uuid4()}"),
        ("post", f"/jobs/{uuid.uuid4()}/retry"),
        ("post", f"/jobs/{uuid.uuid4()}/cancel"),
    ],
)
def test_missing_token_is_401(unauth_client, method, path):
    kwargs = {"json": EMAIL_JOB} if (method, path) == ("post", "/jobs") else {}
    resp = getattr(unauth_client, method)(path, **kwargs)
    assert resp.status_code == 401


def test_unknown_token_is_401(unauth_client):
    resp = unauth_client.get(
        "/jobs", headers={"Authorization": "Bearer not-a-real-token"}
    )
    assert resp.status_code == 401


def test_authenticated_but_wrong_group_is_403(unauth_client):
    resp = unauth_client.get(
        "/jobs", headers={"Authorization": f"Bearer {OUTSIDER_TEST_TOKEN}"}
    )
    assert resp.status_code == 403


def test_probes_and_stats_stay_open(unauth_client):
    assert unauth_client.get("/health").status_code == 200
    assert unauth_client.get("/ready").status_code == 200
    assert unauth_client.get("/stats").status_code == 200


async def test_apiserver_down_is_503(pg_engine, test_settings):
    from fastapi.testclient import TestClient

    from app.main import create_app

    settings = test_settings.model_copy(
        update={
            # RFC 5737 TEST-NET, nothing listens there; short timeout.
            "auth_tokenreview_url": "http://192.0.2.1:6443/apis/authentication.k8s.io/v1/tokenreviews",
            "auth_timeout_s": 0.2,
        }
    )
    app = create_app(settings)
    with TestClient(app) as c:
        resp = c.get("/jobs", headers={"Authorization": "Bearer whatever"})
    assert resp.status_code == 503


def test_cross_user_get_retry_cancel_are_404(client, second_user):
    job_id = client.post("/jobs", json=EMAIL_JOB).json()["id"]

    assert client.get(f"/jobs/{job_id}").status_code == 200
    assert client.get(f"/jobs/{job_id}", headers=second_user).status_code == 404
    assert client.post(f"/jobs/{job_id}/retry", headers=second_user).status_code == 404
    assert client.post(f"/jobs/{job_id}/cancel", headers=second_user).status_code == 404


def test_list_returns_only_own_jobs(client, second_user):
    mine = client.post("/jobs", json=EMAIL_JOB).json()["id"]
    client.post("/jobs", json=EMAIL_JOB, headers=second_user)

    items = client.get("/jobs").json()["items"]
    assert [j["id"] for j in items] == [mine]


def test_idempotency_key_scoped_per_user(client, second_user):
    body = {**EMAIL_JOB, "idempotency_key": "shared-key"}

    first = client.post("/jobs", json=body)
    assert first.status_code == 202
    replay = client.post("/jobs", json=body)
    assert replay.status_code == 200
    assert replay.json()["id"] == first.json()["id"]

    other = client.post("/jobs", json=body, headers=second_user)
    assert other.status_code == 202  # fresh job, no cross-user collision
    assert other.json()["id"] != first.json()["id"]


async def test_submitted_job_is_owned_with_display_name(
    client, pg_engine, default_user_id
):
    from sqlalchemy import text

    job_id = client.post("/jobs", json=EMAIL_JOB).json()["id"]
    async with pg_engine.begin() as conn:
        row = (
            await conn.execute(
                text("SELECT user_id, user_name FROM jobs WHERE id = :id"),
                {"id": job_id},
            )
        ).one()
    assert row.user_id == default_user_id
    assert row.user_name == "default-user"
```

The dropped DB-revocation/TTL tests (`test_deleted_user_still_authenticates_within_ttl`, `test_ttl_zero_makes_revocation_immediate`, `test_unknown_key_is_not_negatively_cached`) are covered at unit level now: TTL/negative-caching by `tests/unit/test_auth_cache.py`, invalid-token non-caching by the `TokenCache` "only successes stored" design plus `test_unknown_token_is_401` here.

- [ ] **Step 7: Run the migration-dependent suites**

Run: `uv run pytest tests/unit -v`
Expected: PASS (no `app.users` imports remain)

Run: `uv run pytest tests/integration/test_auth_api.py -v`
Expected: PASS

- [ ] **Step 8: Sweep for stragglers**

```bash
grep -rn "app.users\|X-API-Key\|api_user_keys\|upsert_user\|get_user_by_key_hash\|hash_key\|KeyCache\|key_cache" app tests --include="*.py"
```

Expected: no hits (fix any that appear — likely candidates: `tests/integration/test_ratelimit.py`, `tests/unit/test_ratelimit.py`, `tests/integration/test_migration.py`, `app/models/__init__.py`). Then run the FULL suite:

Run: `uv run pytest`
Expected: all PASS

- [ ] **Step 9: Lint and commit**

```bash
uv run ruff check --fix && uv run ruff format
git add -A app tests alembic
git commit -m "feat!: replace API-key auth with cluster TokenReview SSO"
```

---

### Task 6: Helm chart — API ServiceAccount, auth-delegator RBAC, remove key machinery

**Files:**
- Create: `deploy/chart/jobprocessor/templates/api-serviceaccount.yaml`
- Create: `deploy/chart/jobprocessor/templates/api-tokenreview-rbac.yaml`
- Modify: `deploy/chart/jobprocessor/templates/api-deployment.yaml`, `deploy/chart/jobprocessor/values.yaml`, `deploy/chart/jobprocessor/README.md`
- Delete: `deploy/chart/jobprocessor/templates/users-sync-job.yaml`

- [ ] **Step 1: ServiceAccount**

Create `deploy/chart/jobprocessor/templates/api-serviceaccount.yaml`:

```yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: {{ include "jobprocessor.fullname" . }}-api
  labels:
    {{- include "jobprocessor.labels" . | nindent 4 }}
    app.kubernetes.io/component: api
```

- [ ] **Step 2: ClusterRoleBinding**

Create `deploy/chart/jobprocessor/templates/api-tokenreview-rbac.yaml`:

```yaml
{{- if .Values.auth.rbac.create }}
# Lets ONLY the API pods create TokenReviews (delegated authentication).
# Deliberately not bound to the namespace default SA — workers/hooks must
# not inherit apiserver access. Cluster-scoped: set auth.rbac.create=false
# if bindings are managed out-of-band.
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: {{ include "jobprocessor.fullname" . }}-api-tokenreview
  labels:
    {{- include "jobprocessor.labels" . | nindent 4 }}
    app.kubernetes.io/component: api
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: system:auth-delegator
subjects:
  - kind: ServiceAccount
    name: {{ include "jobprocessor.fullname" . }}-api
    namespace: {{ .Release.Namespace }}
{{- end }}
```

- [ ] **Step 3: Deployment + values**

In `api-deployment.yaml`, add under `spec.template.spec` (before `terminationGracePeriodSeconds: 30`):

```yaml
      serviceAccountName: {{ include "jobprocessor.fullname" . }}-api
```

and add to the api container `env` block:

```yaml
            - name: AUTH_REQUIRED_GROUP
              value: {{ .Values.auth.requiredGroup | quote }}
```

Delete `deploy/chart/jobprocessor/templates/users-sync-job.yaml`.

In `values.yaml`: delete the `secrets:` block containing `apiUserKeysSecret` (lines ~92-94; keep the block if other keys exist under it) and add:

```yaml
auth:
  # Cluster group whose members may use the API (TokenReview group gate).
  requiredGroup: jobprocessor-users
  rbac:
    # ClusterRoleBinding of system:auth-delegator to the API SA.
    create: true
```

Update `deploy/chart/jobprocessor/README.md`: remove init-secrets/API-key instructions; document `oc login` → `oc whoami -t` → `Authorization: Bearer`, the `auth.*` values, and the `deploy/openshift/setup-idp.sh` prerequisite (Task 7).

- [ ] **Step 4: Verify (manual, no pytest for infra)**

```bash
helm lint deploy/chart/jobprocessor
helm template deploy/chart/jobprocessor | grep -E "kind: (ServiceAccount|ClusterRoleBinding)|serviceAccountName|auth-delegator"
helm template deploy/chart/jobprocessor | grep -i "users-sync\|api-user-keys" || echo "clean"
```

Expected: lint passes; SA + CRB + serviceAccountName present; final grep prints `clean`.

- [ ] **Step 5: Commit**

```bash
git add deploy/chart/jobprocessor
git commit -m "feat(chart): API ServiceAccount + auth-delegator RBAC, drop key secret machinery"
```

---

### Task 7: `setup-idp.sh` cluster-admin script

**Files:**
- Create: `deploy/openshift/setup-idp.sh`
- Delete: `deploy/openshift/init-secrets.sh`
- Modify: `deploy/openshift/bootstrap-cluster.sh` (only if it references init-secrets.sh — grep first), any README mentioning init-secrets.sh

- [ ] **Step 1: Write the script**

Create `deploy/openshift/setup-idp.sh`:

```bash
#!/usr/bin/env bash
# Configures the htpasswd identity provider and the jobprocessor-users
# group. Run ONCE by cluster-admin; replaces init-secrets.sh (API keys are
# gone — the cluster is the IdP, the API validates tokens via TokenReview).
# Idempotent: re-running updates passwords and group membership; the OAuth
# IdP entry is only added if absent.
set -euo pipefail

[ $# -ge 1 ] || { echo "usage: setup-idp.sh user:password [user:password ...]" >&2; exit 1; }
command -v htpasswd >/dev/null 2>&1 \
  || { echo "htpasswd not found (install httpd-tools / apache2-utils)" >&2; exit 1; }

IDP_NAME="jobprocessor-htpasswd"
SECRET_NAME="jobprocessor-htpasswd"
GROUP_NAME="jobprocessor-users"

HTPASSWD_FILE="$(mktemp)"
trap 'rm -f "$HTPASSWD_FILE"' EXIT

USERS=()
for pair in "$@"; do
  user="${pair%%:*}"
  pass="${pair#*:}"
  if [ -z "$user" ] || [ -z "$pass" ] || [ "$user" = "$pair" ]; then
    echo "bad user:password pair: $pair" >&2; exit 1
  fi
  htpasswd -B -b "$HTPASSWD_FILE" "$user" "$pass"
  USERS+=("$user")
done

oc create secret generic "$SECRET_NAME" -n openshift-config \
  --from-file=htpasswd="$HTPASSWD_FILE" \
  --dry-run=client -o yaml | oc apply -f -

if oc get oauth cluster -o jsonpath='{.spec.identityProviders[*].name}' \
    | grep -qw "$IDP_NAME"; then
  echo "IdP $IDP_NAME already configured on OAuth/cluster"
else
  IDP_JSON='{"name":"'"$IDP_NAME"'","mappingMethod":"claim","type":"HTPasswd","htpasswd":{"fileData":{"name":"'"$SECRET_NAME"'"}}}'
  # json-patch append fails when identityProviders is absent entirely; the
  # merge fallback then initializes the list (safe: the list was empty).
  oc patch oauth cluster --type=json \
    -p '[{"op":"add","path":"/spec/identityProviders/-","value":'"$IDP_JSON"'}]' \
  || oc patch oauth cluster --type=merge \
    -p '{"spec":{"identityProviders":['"$IDP_JSON"']}}'
fi

oc adm groups new "$GROUP_NAME" >/dev/null 2>&1 \
  || echo "group $GROUP_NAME already exists"
oc adm groups add-users "$GROUP_NAME" "${USERS[@]}"

echo
echo "Done. OAuth pods roll out in ~1 minute before logins work."
echo "Client flow: oc login --username <user> --password <pass>"
echo "             TOKEN=\$(oc whoami -t)"
echo "             curl -H \"Authorization: Bearer \$TOKEN\" https://<api-route>/jobs"
```

Delete `deploy/openshift/init-secrets.sh`.

- [ ] **Step 2: Sweep references**

```bash
grep -rn "init-secrets" --include="*.md" --include="*.sh" --include="*.yaml" .
```

Update every hit (bootstrap-cluster.sh comments, READMEs, chart values comments) to point at `setup-idp.sh` / the token flow.

- [ ] **Step 3: Verify (manual)**

```bash
bash -n deploy/openshift/setup-idp.sh
```

Expected: exits 0 (syntax check). Live-cluster run happens at rollout time; note in commit message that it needs cluster-admin.

- [ ] **Step 4: Commit**

```bash
git add deploy/openshift
git commit -m "feat(deploy): setup-idp.sh configures htpasswd IdP + group, replaces init-secrets.sh"
```

---

### Task 8: docker-compose — fake TokenReview sidecar

**Files:**
- Modify: `docker-compose.yml`

- [ ] **Step 1: Edit compose**

- Delete the `users-sync` service and the entire trailing `configs:` block (`api_user_keys`).
- Remove `users-sync: condition: service_completed_successfully` from the api service's `depends_on`.
- Add the sidecar (the app image contains `tests/` via `COPY . .`):

```yaml
  fake-tokenreview:
    build: .
    image: jobprocessor-app:${APP_TAG:-dev}
    # Dev-only stand-in for the cluster apiserver's TokenReview endpoint.
    # Baked-in tokens: dev-alice / dev-bob (see tests/support/fake_tokenreview.py).
    command: python -m tests.support.fake_tokenreview
    environment:
      PORT: "8443"
    healthcheck:
      test: ["CMD", "python", "-c",
             "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://localhost:8443/health').status==200 else 1)"]
      interval: 5s
      timeout: 3s
      retries: 10
```

- In the `api` service: add env `AUTH_TOKENREVIEW_URL: http://fake-tokenreview:8443/apis/authentication.k8s.io/v1/tokenreviews` and `depends_on` entry `fake-tokenreview: {condition: service_healthy}`.

- [ ] **Step 2: Verify (manual, no pytest for infra)**

```bash
docker compose up -d --build
curl -s -o /dev/null -w "%{http_code}\n" localhost:8000/jobs                       # expect 401
curl -s -o /dev/null -w "%{http_code}\n" -H "Authorization: Bearer dev-alice" localhost:8000/jobs   # expect 200
curl -s -X POST -H "Authorization: Bearer dev-alice" -H "Content-Type: application/json" \
  -d '{"type":"email","payload":{"to":"a@example.com","subject":"hi"}}' localhost:8000/jobs         # expect 202 body
docker compose down
```

- [ ] **Step 3: Commit**

```bash
git add docker-compose.yml
git commit -m "feat(compose): fake TokenReview sidecar replaces users-sync"
```

---

### Task 9: Docs sweep + full verification

**Files:**
- Modify: `README.md`, `DECISIONS.md` (append entry), any remaining doc with API-key instructions

- [ ] **Step 1: Sweep docs**

```bash
grep -rn "X-API-Key\|API key\|api_user_keys\|init-secrets" --include="*.md" . | grep -v docs/superpowers
```

Update every hit: auth section now documents `oc login` → `oc whoami -t` → `Authorization: Bearer <token>`, the `jobprocessor-users` group requirement, and dev tokens `dev-alice`/`dev-bob` for compose. Append a DECISIONS.md entry (dated 2026-07-14): auth delegated to cluster via TokenReview; ownership = OpenShift User UID; group gate `jobprocessor-users`; users table dropped.

- [ ] **Step 2: Full verification**

```bash
uv run ruff check --fix && uv run ruff format
uv run pytest
```

Expected: everything passes.

- [ ] **Step 3: Commit**

```bash
git add -A
git commit -m "docs: document TokenReview SSO auth flow"
```
