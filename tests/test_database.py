import pytest

import app.database as database
from app.config import NICHE_BY_KEY
from tests.helpers import FakeCursor, fake_conn_cm, identifiers_in

CARROS = NICHE_BY_KEY["carros"]
GERAL = NICHE_BY_KEY["geral"]


def _patch_conn(monkeypatch, cursor):
    monkeypatch.setattr(database, "get_conn", lambda: fake_conn_cm(cursor))


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


def test_default_e_o_nicho_geral(monkeypatch):
    cur = FakeCursor(fetch_result=[])
    _patch_conn(monkeypatch, cur)
    database.get_pending_products()  # sem niche -> geral (compat temporária)
    query, _ = cur.executed[0]
    assert identifiers_in(query) == {"Produtos"}


def test_upsert_products_batch_usa_tabela_do_nicho(monkeypatch):
    cur = FakeCursor()
    _patch_conn(monkeypatch, cur)
    produto = {"nome": "X", "id_produto": "MLB1", "imagem": "i",
               "preco": "p", "link": "l"}
    database.upsert_products_batch([produto], CARROS)
    query, _ = cur.executed[0]
    assert "Produtos_carros" in identifiers_in(query)
