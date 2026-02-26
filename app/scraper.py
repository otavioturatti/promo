import re
import random

import requests
from bs4 import BeautifulSoup

from app.database import get_active_categories, upsert_products_batch
from app.logger import OpLogger

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/143.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
}


# ── Parsing de preço ────────────────────────────────────────

def parse_price(raw: str) -> dict | None:
    if not raw:
        return None

    parts = re.split(r"\\n|\n", raw)
    if len(parts) < 2:
        return None

    preco_original = parts[0].strip()
    linha_suja = parts[1].strip()

    match = re.search(r"R\$\s*([^%]+)%", linha_suja)
    if not match:
        return None

    bloco = match.group(1).strip()
    preco_match = re.match(r"^(.+?,\d{2})(\d+)$", bloco)
    if not preco_match:
        return None

    return {
        "original": preco_original,
        "desconto_valor": "R$" + preco_match.group(1),
        "desconto_pct": int(preco_match.group(2)),
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

    prices = []
    for price_el in soup.select("div.poly-card__content > div.poly-component__price"):
        clone = BeautifulSoup(str(price_el), "html.parser")
        for skip in clone.select(".andes-money-amount_discount"):
            skip.decompose()
        prices.append(clone.get_text(separator="\n", strip=True))

    log.info("parse", f"Elementos: {len(names)} nomes, {len(images)} imgs, "
             f"{len(prices)} preços, {len(links)} links",
             names=len(names), images=len(images),
             prices=len(prices), links=len(links))

    # ── Montar produtos ────────────────────────────────────
    products = []
    skipped = {"empty": 0, "click1": 0, "price_parse": 0, "low_discount": 0, "no_id": 0}

    for name, image, price_raw, link in zip(names, images, prices, links):
        if not all([name, image, price_raw, link]):
            skipped["empty"] += 1
            continue

        if "click1." in link:
            skipped["click1"] += 1
            continue

        price = parse_price(price_raw)
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

        products.append({
            "nome": name,
            "imagem": image,
            "preco": price_raw,
            "link": link,
            "id_produto": id_produto,
        })

    log.info("filter", f"{len(products)} produtos válidos | "
             f"descartados: {sum(skipped.values())}",
             valid=len(products), **skipped)

    return products


# ── Job principal ──────────────────────────────────────────

def run_scraping():
    log = OpLogger("scraper")
    log.info("start", "Iniciando scraping de categorias")

    # ── Buscar categorias ──────────────────────────────────
    try:
        categories = get_active_categories()
    except Exception as e:
        log.error("categories", "Falha ao buscar categorias no banco", exc=e)
        return

    if not categories:
        log.warning("categories", "Nenhuma categoria ativa encontrada")
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
        return

    random.shuffle(all_products)

    with log.timed() as t_save:
        saved, errors = upsert_products_batch(all_products)

    log.info("save", f"{saved} salvos, {errors} erros",
             duration_ms=t_save.ms, saved=saved, errors=errors)

    log.info("done", f"Scraping finalizado: {saved} produtos salvos",
             duration_ms=t_total.ms, total_products=saved,
             categories=len(categories))
