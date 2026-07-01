import time

import pytest

from app.worker.timeout import HandlerTimeout, run_with_timeout


def test_returns_value_when_fast():
    assert run_with_timeout(lambda: 21 * 2, timeout_s=1.0) == 42


def test_raises_handler_timeout_when_slow():
    with pytest.raises(HandlerTimeout):
        run_with_timeout(lambda: time.sleep(1.0), timeout_s=0.05)


def test_propagates_handler_exception():
    def boom():
        raise ValueError("nope")

    with pytest.raises(ValueError):
        run_with_timeout(boom, timeout_s=1.0)
