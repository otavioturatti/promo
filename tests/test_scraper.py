import app.scraper as scraper


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
