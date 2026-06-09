import app.affiliate as affiliate
from app.config import NICHE_BY_KEY

CARROS = NICHE_BY_KEY["carros"]


def test_process_products_passa_niche_ao_atualizar_link(monkeypatch):
    monkeypatch.setattr(affiliate.time, "sleep", lambda *_: None)
    monkeypatch.setattr(affiliate, "create_affiliate_link",
                        lambda url, log, product_id=None: "http://aff/x")
    captured = {}
    monkeypatch.setattr(affiliate, "update_affiliate_link",
                        lambda pid, link, niche: captured.update(args=(pid, link, niche)))

    log = affiliate.OpLogger("affiliate", CARROS)
    affiliate._process_products(
        [{"id_produto": "MLB1", "Link_Compra": "http://ml/x"}], log, CARROS)

    assert captured["args"][0] == "MLB1"
    assert captured["args"][2] is CARROS


def test_process_products_falha_consulta_failures_com_niche(monkeypatch):
    monkeypatch.setattr(affiliate.time, "sleep", lambda *_: None)
    monkeypatch.setattr(affiliate, "create_affiliate_link",
                        lambda url, log, product_id=None: None)  # falhou
    seen = {}
    monkeypatch.setattr(affiliate, "count_affiliate_failures",
                        lambda pid, niche: seen.update(niche=niche) or 1)
    monkeypatch.setattr(affiliate, "send_alert", lambda msg: None)

    log = affiliate.OpLogger("affiliate", CARROS)
    affiliate._process_products(
        [{"id_produto": "MLB1", "Link_Compra": "http://ml/x"}], log, CARROS)

    assert seen["niche"] is CARROS
