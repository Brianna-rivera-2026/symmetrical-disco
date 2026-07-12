from app.core.db import engine_kwargs


def test_engine_kwargs_defaults():
    kwargs = engine_kwargs(pool_size=5, disable_prepared_statements=False)
    assert kwargs["pool_size"] == 5
    assert kwargs["pool_pre_ping"] is True
    assert kwargs["pool_timeout"] == 5
    assert "connect_args" not in kwargs


def test_engine_kwargs_disables_prepared_statements_for_pgbouncer():
    kwargs = engine_kwargs(pool_size=8, disable_prepared_statements=True)
    assert kwargs["pool_size"] == 8
    assert kwargs["connect_args"] == {"prepare_threshold": None}
