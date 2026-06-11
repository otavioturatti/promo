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
