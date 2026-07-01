from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FuturesTimeout
from typing import TypeVar

T = TypeVar("T")


class HandlerTimeout(Exception):
    """Raised when a job handler exceeds its allotted execution time."""


def run_with_timeout(fn: Callable[[], T], timeout_s: float) -> T:
    """Run `fn` in a single-use worker thread and wait up to `timeout_s`.

    On timeout raise HandlerTimeout; the underlying thread is abandoned (Python
    cannot kill it) — the worker recycles its process to reclaim it.
    """
    executor = ThreadPoolExecutor(max_workers=1)
    future = executor.submit(fn)
    try:
        return future.result(timeout=timeout_s)
    except FuturesTimeout as exc:
        raise HandlerTimeout(f"handler exceeded {timeout_s}s") from exc
    finally:
        executor.shutdown(wait=False)
