import random
import re

import requests

from app.config import (
    SENDFLOW_TOKEN, SENDFLOW_ACCOUNT_ID, SENDFLOW_ALERT_RELEASE_ID, Niche,
)
from app.database import get_ready_candidates, get_recent_sent_names, mark_as_sent
from app.dedup import is_duplicate
from app.logger import OpLogger

SENDFLOW_URL = "https://sendflow.pro/sendapi/actions/send-text-message"


# ── Formata mensagem ───────────────────────────────────────

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

    msg = f"{nome}\n\n"
    msg += f"De: {original} Por: {desconto} ({pct} OFF)"
    msg += f"\n\n{link}\nGARANTA O SEU AQUI"

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

def send_alert(message: str):
    """Envia alerta para a campanha de admin via SendFlow."""
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
    escolhidos = frescos or candidatos  # fallback: se todos forem duplicados, envia mesmo
    product = random.choice(escolhidos)

    pid = product["id_produto"]

    log.info("fetch", f"Produto selecionado: {product.get('Nomes_Produtos', '')[:60]}",
             product_id=pid,
             nome=product.get("Nomes_Produtos", ""),
             link_afiliado=product.get("Link_de_afiliado", ""),
             candidatos=len(candidatos), duplicados_pulados=pulados,
             fallback=(len(frescos) == 0))

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

    log.info("done", "Produto enviado com sucesso", product_id=pid)
