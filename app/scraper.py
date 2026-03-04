import re
import random

import requests
from bs4 import BeautifulSoup

from app.database import get_active_categories, upsert_products_batch
from app.logger import OpLogger
from app.whatsapp import send_alert

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/143.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
}


# ── Parsing de preço via aria-label ─────────────────────────

def _aria_to_brl(label: str) -> str:
    """Converte aria-label tipo 'Antes: 2499 reais com 90 centavos' → 'R$2.499,90'."""
    m = re.search(r"(\d+)\s*reais(?:\s+com\s+(\d+)\s+centavos)?", label)
    if not m:
        return ""
    inteiro = int(m.group(1))
    centavos = m.group(2)
    if centavos:
        return f"R${inteiro:,},{centavos}".replace(",", ".")
    return f"R${inteiro:,}".replace(",", ".")


def parse_price_element(price_el) -> dict | None:
    """Extrai preço original, com desconto e % OFF de um elemento de preço."""
    html_str = str(price_el)

    # Busca aria-labels com "Antes:" e "Agora:"
    antes_m = re.search(r'aria-label="(Antes:\s*\d+\s*reais[^"]*)"', html_str)
    agora_m = re.search(r'aria-label="(Agora:\s*\d+\s*reais[^"]*)"', html_str)

    if not antes_m or not agora_m:
        return None

    original = _aria_to_brl(antes_m.group(1))
    desconto_valor = _aria_to_brl(agora_m.group(1))

    if not original or not desconto_valor:
        return None

    # Busca porcentagem de desconto no texto (ex: "40% OFF")
    off_m = re.search(r"(\d+)%\s*OFF", html_str)
    if not off_m:
        return None

    return {
        "original": original,
        "desconto_valor": desconto_valor,
        "desconto_pct": int(off_m.group(1)),
    }


# ── Extrai ID do produto ───────────────────────────────────

def extract_product_id(url: str) -> str:
    match = re.search(r"MLB-?\d+", url, re.IGNORECASE)
    if match:
        return match.group(0).replace("-", "").upper()
    return "SEM_ID"


# ── Scraping de uma categoria ──────────────────────────────

def scrape_category(url: str, log: OpLogger) -> list[dict]:
    # ── HTTP GET ────────────────────────────────────────────
    with log.timed() as t:
        try:
            resp = requests.get(url, headers=HEADERS, timeout=30)
            resp.raise_for_status()
        except requests.RequestException as e:
            log.error("http", f"GET {url} → FALHA: {e}", exc=e,
                      url=url)
            return []

    log.info("http", f"GET {url} → {resp.status_code}",
             duration_ms=t.ms, url=url, status=resp.status_code,
             content_length=len(resp.content))

    # ── Parse HTML ──────────────────────────────────────────
    soup = BeautifulSoup(resp.text, "html.parser")

    names = [el.get_text(strip=True) for el in soup.select(".poly-card__content > h3 > a")]
    images = [el.get("src", "") for el in soup.select("img.poly-component__picture")]
    links = [el.get("href", "") for el in soup.select(".poly-card__content > h3 > a")]
    price_els = soup.select("div.poly-card__content > div.poly-component__price")

    log.info("parse", f"Elementos: {len(names)} nomes, {len(images)} imgs, "
             f"{len(price_els)} preços, {len(links)} links",
             names=len(names), images=len(images),
             prices=len(price_els), links=len(links))

    # ── Montar produtos ────────────────────────────────────
    products = []
    skipped = {"empty": 0, "click1": 0, "price_parse": 0, "low_discount": 0, "no_id": 0}

    for name, image, price_el, link in zip(names, images, price_els, links):
        if not all([name, image, link]):
            skipped["empty"] += 1
            continue

        if "click1." in link:
            skipped["click1"] += 1
            continue

        price = parse_price_element(price_el)
        if not price:
            skipped["price_parse"] += 1
            continue

        if price["desconto_pct"] <= 30:
            skipped["low_discount"] += 1
            continue

        id_produto = extract_product_id(link)
        if id_produto == "SEM_ID":
            skipped["no_id"] += 1
            continue

        # Salva preço já formatado para exibição no WhatsApp
        preco_formatado = (
            f"{price['original']}\n{price['desconto_valor']}\n{price['desconto_pct']}% OFF"
        )

        products.append({
            "nome": name,
            "imagem": image,
            "preco": preco_formatado,
            "link": link,
            "id_produto": id_produto,
        })

    log.info("filter", f"{len(products)} produtos válidos | "
             f"descartados: {sum(skipped.values())}",
             valid=len(products), **skipped)

    return products


# ── Contador de falhas consecutivas ────────────────────────

_consecutive_empty = 0
_ALERT_AFTER = 3  # alerta após 3 rodadas sem produtos


# ── Job principal ──────────────────────────────────────────

def run_scraping():
    global _consecutive_empty

    log = OpLogger("scraper")
    log.info("start", "Iniciando scraping de categorias")

    # ── Buscar categorias ──────────────────────────────────
    try:
        categories = get_active_categories()
    except Exception as e:
        log.error("categories", "Falha ao buscar categorias no banco", exc=e)
        _check_alert(log, "Falha ao buscar categorias no banco de dados.")
        return

    if not categories:
        log.warning("categories", "Nenhuma categoria ativa encontrada")
        _check_alert(log, "Nenhuma categoria ativa encontrada no banco.")
        return

    log.info("categories", f"{len(categories)} categorias ativas",
             count=len(categories))

    # ── Scraping por categoria ─────────────────────────────
    all_products = []
    seen_ids = set()
    cat_ok = 0
    cat_fail = 0

    with log.timed() as t_total:
        for cat in categories:
            url = cat.get("URL") or cat.get("url")
            if not url:
                continue

            products = scrape_category(url, log)

            if products:
                cat_ok += 1
            else:
                cat_fail += 1

            for p in products:
                if p["id_produto"] not in seen_ids:
                    seen_ids.add(p["id_produto"])
                    all_products.append(p)

    log.info("deduplicate", f"{len(all_products)} produtos únicos de {len(seen_ids)} IDs",
             unique=len(all_products), categories_ok=cat_ok, categories_fail=cat_fail)

    # ── Salvar no banco ────────────────────────────────────
    if not all_products:
        log.warning("save", "Nenhum produto para salvar")
        _consecutive_empty += 1
        log.warning("monitor", f"Rodadas consecutivas sem produtos: {_consecutive_empty}/{_ALERT_AFTER}",
                    consecutive=_consecutive_empty)
        if _consecutive_empty >= _ALERT_AFTER:
            _check_alert(log,
                f"Scraping zerou por {_consecutive_empty} rodadas seguidas. "
                f"O Mercado Livre pode ter mudado o HTML. "
                f"Categorias: {cat_ok} OK, {cat_fail} sem produtos.")
        return

    # Reset contador em caso de sucesso
    _consecutive_empty = 0

    random.shuffle(all_products)

    with log.timed() as t_save:
        saved, errors = upsert_products_batch(all_products)

    log.info("save", f"{saved} salvos, {errors} erros",
             duration_ms=t_save.ms, saved=saved, errors=errors)

    log.info("done", f"Scraping finalizado: {saved} produtos salvos",
             duration_ms=t_total.ms, total_products=saved,
             categories=len(categories))


def _check_alert(log: OpLogger, message: str):
    """Envia alerta crítico via WhatsApp."""
    log.error("alert", f"ALERTA CRÍTICO: {message}")
    send_alert(f"⚠️ ALERTA BOT PROMO ⚠️\n\n{message}\n\nVerifique os logs em /logs?level=ERROR")
