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


def test_format_message_sem_prova_social_degrada():
    msg = whatsapp.format_message({
        "Nomes_Produtos": "Pneu X",
        "Preco": "R$500,00\nR$300,00\n40% OFF",
        "Link_de_afiliado": "http://aff/y",
    })
    assert "Pneu X" in msg
    assert "De: R$500 Por: R$300 (40% OFF" in msg   # normalizado p/ padrão BR
    assert "economize R$200" in msg          # ancoragem reforçada
    assert "⭐" not in msg                     # sem prova social, sem linha
    assert msg.rstrip().endswith("http://aff/y")   # link por último (CTA acima, 👇 aponta pra ele)


def test_format_message_com_prova_social():
    import json
    msg = whatsapp.format_message({
        "Nomes_Produtos": "Kit 6 Cuecas Lupo",
        "Preco": "R$129,90\nR$59,90\n54% OFF",
        "Link_de_afiliado": "http://aff/z",
        "social_proof": json.dumps({"rating": "4,9", "reviews": 49256,
                                     "badge": "MAIS VENDIDO"}),
    })
    assert "🏆" not in msg                      # badge removido da mensagem
    assert "⭐ 4,9" in msg
    assert "49 mil avaliações" in msg
    assert "economize R$70" in msg


def test_prova_social_nota_baixa_e_omitida():
    import json
    # nota 4.1 < piso (4.5) e sem badge → linha de prova social vazia
    assert whatsapp._format_social(json.dumps(
        {"rating": "4.1", "reviews": 5000, "badge": None})) == ""


def test_prova_social_nunca_mostra_badge():
    import json
    # badge não entra mais na mensagem, seja qual for
    for b in ("MAIS VENDIDO", "OFERTA IMPERDÍVEL", "OFERTA DO DIA"):
        out = whatsapp._format_social(json.dumps(
            {"rating": "4.9", "reviews": 5000, "badge": b}))
        assert "🏆" not in out
        assert "⭐ 4.9" in out


def test_prova_social_poucas_avaliacoes_mostra_nota_sem_contagem():
    import json
    # nota boa mas poucas avaliações (< 100) → mostra a nota, esconde a contagem fraca
    out = whatsapp._format_social(json.dumps(
        {"rating": "4.8", "reviews": 12, "badge": None}))
    assert "⭐ 4.8" in out
    assert "avaliações" not in out


def test_num_aceita_ponto_e_virgula():
    assert whatsapp._num("R$131.61") == 131.61    # ponto decimal
    assert whatsapp._num("R$1.899") == 1899        # ponto = milhar
    assert whatsapp._num("R$500,00") == 500.0      # vírgula decimal


def test_format_social_vazio_quando_sem_dados():
    assert whatsapp._format_social(None) == ""
    assert whatsapp._format_social("não-json") == ""


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
