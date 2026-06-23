import app.scraper as scraper
from app.config import NICHE_BY_KEY
from app.logger import OpLogger


def test_with_page_pagina1_intacta_e_pagina2_preserva_fragmento():
    base = "https://www.mercadolivre.com.br/ofertas?category=MLB1574#filter_applied=category"
    assert scraper._with_page(base, 1) == base            # page<=1 não mexe
    out = scraper._with_page(base, 2)
    assert "page=2" in out
    assert out.endswith("#filter_applied=category")       # fragmento preservado
    assert "category=MLB1574" in out


def test_scrape_category_pagina_e_para_na_vazia(monkeypatch):
    chamadas = []

    def fake_page(url, log, min_discount):
        chamadas.append(url)
        n = len(chamadas)
        if n == 1:
            return ([{"id_produto": "MLB1"}], 48)
        if n == 2:
            return ([{"id_produto": "MLB2"}], 48)
        return ([], 0)            # página 3 vazia → deve parar aqui

    monkeypatch.setattr(scraper, "_scrape_page", fake_page)
    log = OpLogger("scraper")
    out = scraper.scrape_category("http://x?category=MLB1", log, 30, max_pages=5)

    assert [p["id_produto"] for p in out] == ["MLB1", "MLB2"]
    assert len(chamadas) == 3     # parou na vazia, não foi até a 4/5


def test_scrape_category_default_uma_pagina(monkeypatch):
    chamadas = []

    def fake_page(url, log, min_discount):
        chamadas.append(url)
        return ([{"id_produto": "MLB1"}], 48)

    monkeypatch.setattr(scraper, "_scrape_page", fake_page)
    out = scraper.scrape_category("http://x?category=MLB1", OpLogger("scraper"), 30)

    assert len(chamadas) == 1     # max_pages=1 (default) = comportamento antigo
    assert [p["id_produto"] for p in out] == ["MLB1"]


def test_extract_social_com_e_sem_badge():
    import json
    from bs4 import BeautifulSoup
    html = '''<div class="poly-card__content">
        <span class="poly-component__highlight">MAIS VENDIDO</span>
        <span class="poly-reviews__rating">4.9</span>
        <span class="poly-reviews__total">(49256)</span></div>'''
    card = BeautifulSoup(html, "html.parser").select_one("div.poly-card__content")
    d = json.loads(scraper._extract_social(card))
    assert d["badge"] == "MAIS VENDIDO"
    assert d["rating"] == "4.9"
    assert d["reviews"] == 49256

    vazio = BeautifulSoup('<div class="poly-card__content"></div>',
                          "html.parser").select_one("div.poly-card__content")
    assert scraper._extract_social(vazio) is None


def test_config_scrape_max_pages():
    assert NICHE_BY_KEY["geral"].scrape_max_pages == 3
    assert NICHE_BY_KEY["carros"].scrape_max_pages == 3


def test_contador_vazio_isolado_por_nicho():
    scraper._consecutive_empty.clear()
    assert scraper._register_empty_round("geral") == 1
    assert scraper._register_empty_round("geral") == 2
    assert scraper._register_empty_round("carros") == 1  # isolado do geral


def test_register_success_zera_apenas_o_nicho():
    scraper._consecutive_empty.clear()
    scraper._register_empty_round("geral")
    scraper._register_empty_round("carros")
    scraper._register_success("geral")
    assert scraper._consecutive_empty["geral"] == 0
    assert scraper._consecutive_empty["carros"] == 1  # carros intacto
