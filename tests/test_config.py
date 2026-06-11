from app.config import NICHES, NICHE_BY_KEY, Niche


def test_dois_nichos_definidos():
    assert {n.key for n in NICHES} == {"geral", "carros"}


def test_nicho_carros_tem_tabelas_sufixadas():
    carros = NICHE_BY_KEY["carros"]
    assert carros.table_categorias == "categorias_carros"
    assert carros.table_produtos == "Produtos_carros"
    assert carros.table_logs == "logs_carros"


def test_nicho_geral_mantem_tabelas_atuais():
    geral = NICHE_BY_KEY["geral"]
    assert geral.table_categorias == "categorias"
    assert geral.table_produtos == "Produtos"
    assert geral.table_logs == "logs"


def test_whatsapp_carros_tem_tres_faixas_de_horario():
    carros = NICHE_BY_KEY["carros"]
    assert len(carros.schedules["whatsapp"]) == 3


def test_niche_e_imutavel():
    import dataclasses
    import pytest
    with pytest.raises(dataclasses.FrozenInstanceError):
        NICHE_BY_KEY["geral"].key = "x"


def test_limiar_de_desconto_por_nicho():
    assert NICHE_BY_KEY["geral"].min_discount_pct == 30
    assert NICHE_BY_KEY["carros"].min_discount_pct == 20
