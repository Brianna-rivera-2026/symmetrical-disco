"""ASGI middleware for request body size limits (spec §3)."""

import json


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
                    pass  # malformed header: fall through to buffering

        # Buffer the body ourselves (bounded to max_bytes + 1) so we can
        # reject BEFORE the inner app (FastAPI) ever starts reading it --
        # FastAPI's own body-parsing catches arbitrary exceptions raised
        # from receive() and converts them into its own 400 response, so
        # raising mid-stream from a wrapped receive() never reaches this
        # middleware's exception handling.
        buffered = []
        total = 0
        while True:
            message = await receive()
            if message["type"] != "http.request":
                buffered.append(message)  # e.g. http.disconnect
                break
            body = message.get("body", b"")
            total += len(body)
            if total > self.max_bytes:
                await _send_413(send)
                return
            buffered.append(message)
            if not message.get("more_body", False):
                break

        queue = list(buffered)

        async def replay_receive():
            if queue:
                return queue.pop(0)
            return await receive()

        await self.app(scope, replay_receive, send)
