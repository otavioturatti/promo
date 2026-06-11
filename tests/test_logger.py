import app.logger as logger
from app.config import NICHE_BY_KEY

CARROS = NICHE_BY_KEY["carros"]


def test_group_by_table_separa_por_tabela():
    recs = [
        {"log_table": "logs", "message": "a"},
        {"log_table": "logs_carros", "message": "b"},
        {"log_table": "logs", "message": "c"},
    ]
    groups = logger._group_by_table(recs)
    assert set(groups.keys()) == {"logs", "logs_carros"}
    assert len(groups["logs"]) == 2
    assert len(groups["logs_carros"]) == 1


def test_oplogger_enfileira_na_tabela_do_nicho(monkeypatch):
    captured = []
    monkeypatch.setattr(logger._db_queue, "put_nowait", lambda rec: captured.append(rec))
    log = logger.OpLogger("affiliate", CARROS)
    log.info("create_link", "ok")
    assert captured[0]["log_table"] == "logs_carros"
    assert captured[0]["module"] == "affiliate"
    assert captured[0]["level"] == "INFO"


def test_oplogger_default_e_geral(monkeypatch):
    captured = []
    monkeypatch.setattr(logger._db_queue, "put_nowait", lambda rec: captured.append(rec))
    log = logger.OpLogger("system")
    log.info("startup", "ok")
    assert captured[0]["log_table"] == "logs"


def test_cleanup_old_logs_apaga_de_todas_as_tabelas(monkeypatch):
    deletes = []

    class _Cur:
        def execute(self, query, params=None):
            deletes.append(query)
        @property
        def rowcount(self):
            return 0
        def __enter__(self):
            return self
        def __exit__(self, *a):
            return False

    class _Conn:
        def cursor(self):
            return _Cur()
        def __enter__(self):
            return self
        def __exit__(self, *a):
            return False
        def close(self):
            pass

    monkeypatch.setattr(logger.psycopg2, "connect", lambda *a, **k: _Conn())
    logger.cleanup_old_logs()
    blob = " ".join(str(q) for q in deletes)
    assert "logs_carros" in blob
    assert "logs" in blob
