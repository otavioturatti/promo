import app.whatsapp as whatsapp
from app.config import NICHE_BY_KEY

CARROS = NICHE_BY_KEY["carros"]


class _FakeResp:
    status_code = 200
    text = "ok"


def test_send_text_message_usa_release_id_recebido(monkeypatch):
    captured = {}

    def fake_post(url, headers=None, json=None, timeout=None):
        captured["json"] = json
        return _FakeResp()

    monkeypatch.setattr(whatsapp.requests, "post", fake_post)
    log = whatsapp.OpLogger("whatsapp", CARROS)

    ok = whatsapp.send_text_message("oi", log, "REL_CARROS", product_id="MLB1")

    assert ok is True
    assert captured["json"]["releaseId"] == "REL_CARROS"


def test_format_message_inalterado():
    msg = whatsapp.format_message({
        "Nomes_Produtos": "Pneu X",
        "Preco": "R$500,00\nR$300,00\n40% OFF",
        "Link_de_afiliado": "http://aff/y",
    })
    assert "Pneu X" in msg
    assert "De: R$500,00 Por: R$300,00 (40% OFF)" in msg
    assert "http://aff/y" in msg


def test_run_send_whatsapp_pula_duplicado(monkeypatch):
    candidatos = [
        {"id_produto": "MLB1",
         "Nomes_Produtos": "Aromatizante Gecko Audi Cockpit Air Genuino",
         "Preco": "R$10,00\nR$8,00\n20% OFF", "Link_de_afiliado": "http://a"},
        {"id_produto": "MLB2",
         "Nomes_Produtos": "Capa De Chuva Moto Impermeavel Motoqueiro",
         "Preco": "R$50,00\nR$40,00\n20% OFF", "Link_de_afiliado": "http://b"},
    ]
    monkeypatch.setattr(whatsapp, "get_ready_candidates", lambda niche: candidatos)
    monkeypatch.setattr(whatsapp, "get_recent_sent_names",
                        lambda niche: ["Audi Aromatizante Gecko Cockpit Air Genuino"])
    monkeypatch.setattr(whatsapp, "send_text_message",
                        lambda msg, log, rel, product_id=None: True)
    enviado = {}
    monkeypatch.setattr(whatsapp, "mark_as_sent",
                        lambda pid, niche: enviado.update(pid=pid))

    whatsapp.run_send_whatsapp(CARROS)

    # MLB1 é duplicado de um enviado recente -> deve escolher o fresco MLB2
    assert enviado["pid"] == "MLB2"
