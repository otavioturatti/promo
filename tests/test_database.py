import pytest

import app.database as database
from app.config import NICHE_BY_KEY
from tests.helpers import FakeCursor, fake_conn_cm, identifiers_in

CARROS = NICHE_BY_KEY["carros"]
GERAL = NICHE_BY_KEY["geral"]


def _patch_conn(monkeypatch, cursor):
    monkeypatch.setattr(database, "get_conn", lambda: fake_conn_cm(cursor))


def test_connect_reconecta_apos_conexao_derrubada(monkeypatch):
    """_connect deve reconectar quando o pooler derruba a conexão."""
    calls = {"n": 0}

    class _Cur:
        def __enter__(self): return self
        def __exit__(self, *a): return False
        def execute(self, q): pass

    class _Conn:
        def cursor(self): return _Cur()

    def fake_connect(*a, **k):
        calls["n"] += 1
        if calls["n"] == 1:
            raise database.psycopg2.OperationalError("connection already closed")
        return _Conn()

    monkeypatch.setattr(database.psycopg2, "connect", fake_connect)
    monkeypatch.setattr(database.time, "sleep", lambda s: None)

    conn = database._connect()
    assert calls["n"] == 2                 # falhou 1x, reconectou
    assert isinstance(conn, _Conn)


def test_get_pending_products_usa_tabela_do_nicho(monkeypatch):
    cur = FakeCursor(fetch_result=[])
    _patch_conn(monkeypatch, cur)
    database.get_pending_products(CARROS)
    query, _ = cur.executed[0]
    assert "Produtos_carros" in identifiers_in(query)


def test_get_active_categories_usa_tabela_do_nicho(monkeypatch):
    cur = FakeCursor(fetch_result=[])
    _patch_conn(monkeypatch, cur)
    database.get_active_categories(CARROS)
    query, _ = cur.executed[0]
    assert "categorias_carros" in identifiers_in(query)


def test_count_affiliate_failures_usa_tabela_de_logs_do_nicho(monkeypatch):
    cur = FakeCursor(fetch_result=[(0,)])
    _patch_conn(monkeypatch, cur)
    database.count_affiliate_failures("MLB1", CARROS)
    query, params = cur.executed[0]
    assert "logs_carros" in identifiers_in(query)
    assert params == ("MLB1",)


def test_mark_as_sent_usa_tabela_do_nicho(monkeypatch):
    cur = FakeCursor()
    _patch_conn(monkeypatch, cur)
    database.mark_as_sent("MLB1", CARROS)
    query, params = cur.executed[0]
    assert "Produtos_carros" in identifiers_in(query)
    assert params == ("MLB1",)


def test_query_logs_usa_tabela_e_filtros(monkeypatch):
    cur = FakeCursor(fetch_result=[])
    _patch_conn(monkeypatch, cur)
    database.query_logs(limit=10, level="error", niche=CARROS)
    query, params = cur.executed[0]
    assert "logs_carros" in identifiers_in(query)
    assert params == ["ERROR", 10]


def test_upsert_products_batch_usa_tabela_do_nicho(monkeypatch):
    cur = FakeCursor()
    _patch_conn(monkeypatch, cur)
    produto = {"nome": "X", "id_produto": "MLB1", "imagem": "i",
               "preco": "p", "link": "l"}
    database.upsert_products_batch([produto], CARROS)
    query, _ = cur.executed[0]
    assert "Produtos_carros" in identifiers_in(query)
