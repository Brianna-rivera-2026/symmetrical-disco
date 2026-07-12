"""ASGI middleware for request body size limits (spec §3)."""

import json


class _BodyTooLarge(Exception):
    pass


async def _send_413(send) -> None:
    body = json.dumps({"detail": "request body too large"}).encode()
    await send(
        {
            "type": "http.response.start",
            "status": 413,
            "headers": [
                (b"content-type", b"application/json"),
                (b"content-length", str(len(body)).encode()),
            ],
        }
    )
    await send({"type": "http.response.body", "body": body})


class BodySizeLimitMiddleware:
    """Rejects oversize requests with 413: via Content-Length when declared,
    otherwise by counting streamed bytes and aborting once the cap is crossed."""

    def __init__(self, app, max_bytes: int) -> None:
        self.app = app
        self.max_bytes = max_bytes

    async def __call__(self, scope, receive, send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        for name, value in scope["headers"]:
            if name == b"content-length":
                try:
                    if int(value) > self.max_bytes:
                        await _send_413(send)
                        return
                except ValueError:
                    pass  # malformed header: fall through to counting

        received = 0
        response_started = False

        async def wrapped_send(message) -> None:
            nonlocal response_started
            if message["type"] == "http.response.start":
                response_started = True
            await send(message)

        async def wrapped_receive():
            nonlocal received
            message = await receive()
            if message["type"] == "http.request":
                received += len(message.get("body", b""))
                if received > self.max_bytes:
                    raise _BodyTooLarge
            return message

        try:
            await self.app(scope, wrapped_receive, wrapped_send)
        except _BodyTooLarge:
            if response_started:
                raise  # too late for a clean 413; let the server drop the connection
            await _send_413(send)
