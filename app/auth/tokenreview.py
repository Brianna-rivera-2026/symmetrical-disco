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
