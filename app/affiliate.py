import time

import requests

from app.config import ML_COOKIES, ML_CSRF_TOKEN, ML_AFFILIATE_TAG
from app.database import (
    count_affiliate_failures,
    get_pending_products,
    get_ready_with_null_links,
    mark_as_failed,
    update_affiliate_link,
)
from app.logger import OpLogger
from app.whatsapp import send_alert

ML_API_URL = "https://www.mercadolivre.com.br/affiliate-program/api/v2/affiliates/createLink"

ML_HEADERS = {
    "accept": "application/json, text/plain, */*",
    "accept-language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
    "content-type": "application/json",
    "origin": "https://www.mercadolivre.com.br",
    "referer": "https://www.mercadolivre.com.br/afiliados/linkbuilder",
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
    "user-agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/143.0.0.0 Safari/537.36"
    ),
}


def create_affiliate_link(product_url: str, log: OpLogger,
                          product_id: str = None) -> str | None:
    headers = {**ML_HEADERS, "x-csrf-token": ML_CSRF_TOKEN, "cookie": ML_COOKIES}
    payload = {"urls": [product_url], "tag": ML_AFFILIATE_TAG}

    with log.timed() as t:
        try:
            resp = requests.post(ML_API_URL, json=payload, headers=headers, timeout=15)
        except requests.RequestException as e:
            log.error("create_link", f"Request falhou: {e}",
                      product_id=product_id, exc=e,
                      product_url=product_url)
            return None

    if resp.status_code != 200:
        log.error("create_link", f"API ML → {resp.status_code}: {resp.text[:200]}",
                  product_id=product_id, duration_ms=t.ms,
                  status=resp.status_code, product_url=product_url)
        return None

    try:
        data = resp.json()
        url_obj = data["urls"][0]
        affiliate_url = url_obj.get("short_url") or url_obj.get("origin_url")
        if not affiliate_url:
            raise KeyError("sem short_url nem origin_url na resposta")
    except (KeyError, IndexError, ValueError) as e:
        log.error("create_link", f"Resposta inesperada da API: {e}",
                  product_id=product_id, duration_ms=t.ms, exc=e,
                  response_body=resp.text[:300])
        return None

    log.info("create_link", f"Link gerado → {affiliate_url}",
             product_id=product_id, duration_ms=t.ms,
             affiliate_url=affiliate_url)
    return affiliate_url


def _process_products(products: list[dict], log: OpLogger):
    total = len(products)
    success = 0
    failed = 0

    for i, product in enumerate(products, 1):
        pid = product["id_produto"]
        link_compra = product.get("Link_Compra", "")

        if not link_compra:
            log.warning("skip", "Link_Compra vazio", product_id=pid)
            continue

        log.info("processing", f"({i}/{total}) Gerando link", product_id=pid)

        affiliate_link = create_affiliate_link(link_compra, log, product_id=pid)

        if affiliate_link:
            try:
                update_affiliate_link(pid, affiliate_link)
                log.info("update_db", "Status → PRONTO", product_id=pid,
                         affiliate_url=affiliate_link)
                success += 1
            except Exception as e:
                log.error("update_db", f"Falha ao atualizar banco: {e}",
                          product_id=pid, exc=e)
                failed += 1
        else:
            failed += 1
            try:
                failures = count_affiliate_failures(pid)
                if failures >= 3:
                    mark_as_failed(pid)
                    log.warning("permanent_fail",
                                f"Produto marcado como FALHA após {failures} tentativas",
                                product_id=pid)
            except Exception as e:
                log.error("check_failures", f"Erro ao verificar falhas: {e}",
                          product_id=pid, exc=e)

        time.sleep(3)

    log.info("done", f"Concluído: {success} OK, {failed} falhas de {total}",
             total=total, success=success, failed=failed)

    # Se todos falharam, provavelmente cookies expiraram
    if total > 0 and success == 0:
        log.error("alert", "Todos os links falharam — cookies podem ter expirado")
        send_alert(
            "⚠️ ALERTA BOT PROMO ⚠️\n\n"
            f"Geração de links falhou em todos os {total} produtos.\n"
            "Os cookies do Mercado Livre provavelmente expiraram.\n\n"
            "Atualize ML_COOKIES e ML_CSRF_TOKEN no EasyPanel."
        )


def run_affiliate_generation():
    log = OpLogger("affiliate")
    log.info("start", "Iniciando geração de links de afiliado")

    try:
        products = get_pending_products()
    except Exception as e:
        log.error("fetch", "Falha ao buscar produtos pendentes", exc=e)
        return

    if not products:
        log.info("fetch", "Nenhum produto pendente")
        return

    log.info("fetch", f"{len(products)} produtos pendentes", count=len(products))
    _process_products(products, log)


def run_retry_null_links():
    log = OpLogger("retry")
    log.info("start", "Retentando links nulos")

    try:
        products = get_ready_with_null_links()
    except Exception as e:
        log.error("fetch", "Falha ao buscar produtos com link nulo", exc=e)
        return

    if not products:
        log.info("fetch", "Nenhum link nulo para retentar")
        return

    log.info("fetch", f"{len(products)} produtos com link nulo", count=len(products))
    _process_products(products, log)
