import pytest


@pytest.fixture(autouse=True)
def _no_db_log_flush(monkeypatch):
    """Impede que a thread de logs conecte ao Postgres durante os testes."""
    monkeypatch.setattr("app.logger._flush_to_db", lambda records: None, raising=False)
