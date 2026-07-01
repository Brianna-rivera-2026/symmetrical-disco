from app.retry import backoff_delay


def test_backoff_immediate_first_retry():
    assert backoff_delay(1, [0, 30, 120]) == 0


def test_backoff_second_and_third():
    assert backoff_delay(2, [0, 30, 120]) == 30
    assert backoff_delay(3, [0, 30, 120]) == 120


def test_backoff_clamps_past_end():
    assert backoff_delay(9, [0, 30, 120]) == 120
