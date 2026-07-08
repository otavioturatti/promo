import json
import random
import re
from collections import defaultdict, deque

import requests

from app.config import (
    SENDFLOW_TOKEN, SENDFLOW_ACCOUNT_ID, SENDFLOW_ALERT_RELEASE_ID, Niche,
)
from app.database import get_ready_candidates, get_recent_sent_names, mark_as_sent
from app.dedup import is_duplicate
from app.logger import OpLogger

SENDFLOW_URL = "https://sendflow.pro/sendapi/actions/send-text-message"

# Cooldown de categoria: não repetir uma categoria nos últimos N envios (por nicho).
CATEGORY_COOLDOWN = 5
_recent_categories: dict = defaultdict(lambda: deque(maxlen=CATEGORY_COOLDOWN))


# ── Formata mensagem ───────────────────────────────────────

def _num(s: str):
    """Extrai o valor de uma string 'R$...'. Aceita 'R$131.61' e 'R$500,00'."""
    m = re.search(r"R\$\s*([\d.,]+)", s or "")
    if not m:
        return None
    raw = m.group(1)
    if "," in raw:                       # vírgula decimal (ponto = milhar)
        return float(raw.replace(".", "").replace(",", "."))
    parts = raw.strip(".").split(".")    # só pontos
    if len(parts) >= 2 and len(parts[-1]) == 2:   # último grupo = centavos
        return float("".join(parts[:-1]) + "." + parts[-1])
    return float("".join(parts))         # pontos = milhar


def _brl(v: float) -> str:
    s = f"{v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    if s.endswith(",00"):
        s = s[:-3]
    return f"R${s}"


def _fmt_reviews(n: int) -> str:
    return f"{round(n / 1000)} mil" if n >= 1000 else str(n)


# Pisos de qualidade: só exibimos prova social que de fato persuade.
# Nota baixa ou poucas avaliações = prova social fraca/negativa → omitir.
MIN_RATING = 4.5
MIN_REVIEWS = 100

# CTAs alternados para evitar habituação (mensagem sempre igual = ignorada).
CTA_VARIANTS = [
    "GARANTA O SEU AQUI 👇",
    "PEGUE O SEU AQUI 👇",
    "APROVEITE AQUI 👇",
    "CONFIRA A OFERTA AQUI 👇",
    "VER NO MERCADO LIVRE 👇",
]


def _parse_rating(s):
    try:
        return float(str(s).replace(",", "."))
    except (ValueError, TypeError):
        return None


def _format_social(raw) -> str:
    """Linha de prova social a partir do JSON salvo, aplicando pisos; '' se nada qualifica."""
    if not raw:
        return ""
    try:
        d = json.loads(raw) if isinstance(raw, str) else raw
    except (ValueError, TypeError):
        return ""
    rating = _parse_rating(d.get("rating"))
    if rating is None or rating < MIN_RATING:
        return ""
    r = f"⭐ {d['rating']}"
    reviews = d.get("reviews")
    if reviews and int(reviews) >= MIN_REVIEWS:
        r += f" ({_fmt_reviews(int(reviews))} avaliações)"
    return r


def format_message(product: dict) -> str:
    """
    Formato do campo Preco no banco (novo):
        R$2.499,90
        R$1.484,00
        40% OFF
    """
    nome = product.get("Nomes_Produtos", "")
    preco_raw = product.get("Preco", "")
    link = product.get("Link_de_afiliado", "")

    parts = re.split(r"\\n|\n", preco_raw)

    original = parts[0].strip() if len(parts) > 0 else ""
    desconto = parts[1].strip() if len(parts) > 1 else ""
    pct = parts[2].strip().replace(" OFF", "") if len(parts) > 2 else ""

    social_line = _format_social(product.get("social_proof"))

    o, d = _num(original), _num(desconto)
    economia = f" — economize {_brl(o - d)}" if (o and d and o > d) else ""
    orig_disp = _brl(o) if o else original       # normaliza p/ padrão BR (R$289,99)
    desc_disp = _brl(d) if d else desconto

    msg = f"{nome}\n"
    if social_line:
        msg += f"{social_line}\n"
    msg += "\n"
    msg += f"De: {orig_disp} Por: {desc_disp} ({pct} OFF{economia})"
    msg += f"\n\n{random.choice(CTA_VARIANTS)}\n{link}"   # CTA acima, 👇 aponta pro link

    return msg


# ── Envio via SendFlow ─────────────────────────────────────

def send_text_message(message: str, log: OpLogger, release_id: str,
                      product_id: str = None) -> bool:
    headers = {
        "Authorization": f"Bearer {SENDFLOW_TOKEN}",
        "Content-Type": "application/json",
    }
    payload = {
        "linkPreview": True,
        "messageText": message,
        "releaseId": release_id,
        "accountId": SENDFLOW_ACCOUNT_ID,
    }

    with log.timed() as t:
        try:
            resp = requests.post(SENDFLOW_URL, headers=headers,
                                 json=payload, timeout=30)
        except requests.RequestException as e:
            log.error("send", f"Request SendFlow falhou: {e}",
                      product_id=product_id, exc=e)
            return False

    if resp.status_code >= 400:
        log.error("send", f"SendFlow → {resp.status_code}: {resp.text[:200]}",
                  product_id=product_id, duration_ms=t.ms,
                  status=resp.status_code)
        return False

    log.info("send", f"SendFlow → {resp.status_code} OK",
             product_id=product_id, duration_ms=t.ms,
             status=resp.status_code)
    return True


# ── Alerta para admin ──────────────────────────────────────

# Alertas de erro no WhatsApp — desativados temporariamente (flip para True p/ reativar).
ALERTS_ENABLED = False


def send_alert(message: str):
    """Envia alerta para a campanha de admin via SendFlow."""
    if not ALERTS_ENABLED:
        return
    log = OpLogger("alert")
    headers = {
        "Authorization": f"Bearer {SENDFLOW_TOKEN}",
        "Content-Type": "application/json",
    }
    payload = {
        "linkPreview": False,
        "messageText": message,
        "releaseId": SENDFLOW_ALERT_RELEASE_ID,
        "accountId": SENDFLOW_ACCOUNT_ID,
    }

    try:
        resp = requests.post(SENDFLOW_URL, headers=headers,
                             json=payload, timeout=30)
        if resp.status_code < 400:
            log.info("send", f"Alerta enviado → {resp.status_code}")
        else:
            log.error("send", f"Falha ao enviar alerta → {resp.status_code}: {resp.text[:200]}")
    except requests.RequestException as e:
        log.error("send", f"Request falhou: {e}", exc=e)


# ── Job principal ──────────────────────────────────────────

def run_send_whatsapp(niche: Niche):
    log = OpLogger("whatsapp", niche)
    log.info("start", f"Buscando próximo produto para enviar [{niche.key}]")

    try:
        candidatos = get_ready_candidates(niche)
        enviados = get_recent_sent_names(niche)
    except Exception as e:
        log.error("fetch", "Falha ao buscar produtos no banco", exc=e)
        return

    if not candidatos:
        log.info("fetch", "Nenhum produto pronto para enviar")
        return

    frescos = [c for c in candidatos
               if not is_duplicate(c.get("Nomes_Produtos", ""), enviados)]
    pulados = len(candidatos) - len(frescos)

    # Cooldown de categoria: evita repetir categoria nos últimos N envios
    recentes = _recent_categories[niche.key]
    frescos_cat = [c for c in frescos
                   if not c.get("categoria") or c.get("categoria") not in recentes]
    cat_pulados = len(frescos) - len(frescos_cat)
    escolhidos = frescos_cat or frescos or candidatos  # fallbacks em cascata
    product = random.choice(escolhidos)

    pid = product["id_produto"]

    log.info("fetch", f"Produto selecionado: {product.get('Nomes_Produtos', '')[:60]}",
             product_id=pid,
             nome=product.get("Nomes_Produtos", ""),
             link_afiliado=product.get("Link_de_afiliado", ""),
             categoria=product.get("categoria"),
             candidatos=len(candidatos), duplicados_pulados=pulados,
             categoria_pulada=cat_pulados,
             fallback=(len(frescos) == 0),
             fallback_categoria=(len(frescos_cat) == 0 and len(frescos) > 0))

    caption = format_message(product)
    log.info("format", f"Mensagem formatada ({len(caption)} chars)",
             product_id=pid, caption_length=len(caption))

    if not send_text_message(caption, log, niche.sendflow_release_id, product_id=pid):
        log.error("done", "Envio falhou — produto NÃO marcado como enviado", product_id=pid)
        return

    try:
        mark_as_sent(pid, niche)
        log.info("mark_sent", "Status → ENVIADO", product_id=pid)
    except Exception as e:
        log.error("mark_sent", f"Falha ao marcar como enviado: {e}", product_id=pid, exc=e)
        return

    cat = product.get("categoria")
    if cat:
        _recent_categories[niche.key].append(cat)

    log.info("done", "Produto enviado com sucesso", product_id=pid)
