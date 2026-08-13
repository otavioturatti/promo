import re
import json
import random
from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode

import requests
from bs4 import BeautifulSoup

from app.config import Niche
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
    """Extrai preço original, com desconto e % OFF de um elemento de preço.

    HTML do ML (2026-07): preço original tem aria-label 'Antes: N reais...';
    o preço atual está em .poly-price__current com aria-label só do valor
    (o prefixo 'Agora:' foi removido pelo ML).
    """
    # Preço original (riscado): aria-label começa com "Antes:"
    antes_el = price_el.select_one('[aria-label^="Antes:"]')
    if not antes_el:
        return None
    original = _aria_to_brl(antes_el.get("aria-label", ""))

    # Preço atual: aria-label (com "reais") dentro de .poly-price__current
    atual_el = price_el.select_one('.poly-price__current [aria-label*="reais"]')
    if atual_el is None:
        # fallback: primeiro aria-label com "reais" que não seja o "Antes:"
        for el in price_el.select('[aria-label*="reais"]'):
            if not el.get("aria-label", "").startswith("Antes:"):
                atual_el = el
                break
    if atual_el is None:
        return None
    desconto_valor = _aria_to_brl(atual_el.get("aria-label", ""))

    if not original or not desconto_valor:
        return None

    # Porcentagem de desconto no texto (ex: "40% OFF")
    off_m = re.search(r"(\d+)%\s*OFF", str(price_el))
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

def _with_page(url: str, page: int) -> str:
    """Adiciona page=N à query preservando o fragmento; page<=1 → URL intacta."""
    if page <= 1:
        return url
    parts = urlsplit(url)
    q = dict(parse_qsl(parts.query, keep_blank_values=True))
    q["page"] = str(page)
    return urlunsplit((parts.scheme, parts.netloc, parts.path,
                       urlencode(q), parts.fragment))


def _extract_social(card) -> str | None:
    """Prova social do card (nota, nº avaliações, badge) como JSON, ou None."""
    def txt(sel):
        el = card.select_one(sel)
        return el.get_text(" ", strip=True) if el else None

    rating = txt(".poly-reviews__rating")
    total = txt(".poly-reviews__total")        # ex.: "(25038)"
    badge = txt(".poly-component__highlight")   # ex.: "MAIS VENDIDO"
    if not (rating or total or badge):
        return None

    reviews = None
    if total:
        m = re.search(r"\d[\d.]*", total)
        if m:
            reviews = int(m.group(0).replace(".", ""))

    return json.dumps({"rating": rating, "reviews": reviews, "badge": badge},
                      ensure_ascii=False)


MIN_RATING = 4.2  # descarta produtos com nota abaixo disso (só entra coisa boa)


def _rating_from_social(social) -> float | None:
    if not social:
        return None
    try:
        r = json.loads(social).get("rating")
        return float(str(r).replace(",", ".")) if r else None
    except (ValueError, TypeError):
        return None


def _scrape_page(url: str, log: OpLogger, min_discount: int) -> tuple[list[dict], int]:
    """Raspa UMA página; devolve (produtos_válidos, nº_de_cards_brutos)."""
    # ── HTTP GET ────────────────────────────────────────────
    with log.timed() as t:
        try:
            resp = requests.get(url, headers=HEADERS, timeout=30)
            resp.raise_for_status()
        except requests.RequestException as e:
            log.error("http", f"GET {url} → FALHA: {e}", exc=e,
                      url=url)
            return [], 0

    log.info("http", f"GET {url} → {resp.status_code}",
             duration_ms=t.ms, url=url, status=resp.status_code,
             content_length=len(resp.content))

    # ── Parse HTML ──────────────────────────────────────────
    soup = BeautifulSoup(resp.text, "html.parser")

    anchors = soup.select(".poly-card__content > h3 > a")
    names = [el.get_text(strip=True) for el in anchors]
    images = [el.get("src", "") for el in soup.select("img.poly-component__picture")]
    links = [el.get("href", "") for el in anchors]
    price_els = soup.select("div.poly-card__content > div.poly-component__price")

    # Prova social do card de cada título → alinhada 1:1 com names/links
    socials = [_extract_social(a.find_parent(class_="poly-card__content")) for a in anchors]

    log.info("parse", f"Elementos: {len(names)} nomes, {len(images)} imgs, "
             f"{len(price_els)} preços, {len(links)} links",
             names=len(names), images=len(images),
             prices=len(price_els), links=len(links))

    # ── Montar produtos ────────────────────────────────────
    products = []
    skipped = {"empty": 0, "click1": 0, "price_parse": 0, "low_discount": 0,
               "no_id": 0, "low_rating": 0}

    for name, image, price_el, link, social in zip(names, images, price_els, links, socials):
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

        if price["desconto_pct"] <= min_discount:
            skipped["low_discount"] += 1
            continue

        id_produto = extract_product_id(link)
        if id_produto == "SEM_ID":
            skipped["no_id"] += 1
            continue

        rating = _rating_from_social(social)
        if rating is not None and rating < MIN_RATING:
            skipped["low_rating"] += 1
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
            "social": social,
        })

    log.info("filter", f"{len(products)} produtos válidos | "
             f"descartados: {sum(skipped.values())}",
             valid=len(products), **skipped)

    return products, len(names)


def scrape_category(url: str, log: OpLogger, min_discount: int = 30,
                    max_pages: int = 1) -> list[dict]:
    """Raspa até max_pages páginas da categoria (&page=N), parando na primeira vazia."""
    all_products = []
    for page in range(1, max_pages + 1):
        products, n_cards = _scrape_page(_with_page(url, page), log, min_discount)
        all_products.extend(products)
        if n_cards == 0:        # fim dos resultados → não busca além
            break
    if max_pages > 1:
        log.info("paginate", f"{len(all_products)} produtos em até {max_pages} página(s)",
                 produtos=len(all_products), max_pages=max_pages)
    return all_products


# ── Contador de rodadas vazias (por nicho) ─────────────────

_consecutive_empty: dict[str, int] = {}
_ALERT_AFTER = 3  # alerta após 3 rodadas sem produtos


def _register_empty_round(niche_key: str) -> int:
    _consecutive_empty[niche_key] = _consecutive_empty.get(niche_key, 0) + 1
    return _consecutive_empty[niche_key]


def _register_success(niche_key: str) -> None:
    _consecutive_empty[niche_key] = 0


# ── Job principal ──────────────────────────────────────────

def run_scraping(niche: Niche):
    log = OpLogger("scraper", niche)
    log.info("start", f"Iniciando scraping [{niche.key}]")

    # ── Buscar categorias ──────────────────────────────────
    try:
        categories = get_active_categories(niche)
    except Exception as e:
        log.error("categories", "Falha ao buscar categorias no banco", exc=e)
        _check_alert(log, niche, "Falha ao buscar categorias no banco de dados.")
        return

    if not categories:
        log.warning("categories", "Nenhuma categoria ativa encontrada")
        _check_alert(log, niche, "Nenhuma categoria ativa encontrada no banco.")
        return

    log.info("categories", f"{len(categories)} categorias ativas", count=len(categories))

    # ── Scraping por categoria (salva a cada categoria: memória baixa + durável) ──
    seen_ids = set()
    cat_ok = 0
    cat_fail = 0
    total_unique = 0
    total_saved = 0
    total_errors = 0

    with log.timed() as t_total:
        for cat in categories:
            url = cat.get("URL") or cat.get("url")
            if not url:
                continue

            products = scrape_category(url, log, niche.min_discount_pct,
                                       niche.scrape_max_pages)

            if products:
                cat_ok += 1
            else:
                cat_fail += 1

            # dedup entre categorias + tag da categoria de origem
            frescos = []
            for p in products:
                if p["id_produto"] not in seen_ids:
                    seen_ids.add(p["id_produto"])
                    p["categoria"] = cat.get("categoria") or cat.get("Categoria")
                    frescos.append(p)

            if not frescos:
                continue

            # Salva JÁ os produtos desta categoria (não acumula tudo na memória;
            # se o processo morrer no meio, o que já salvou persiste).
            total_unique += len(frescos)
            random.shuffle(frescos)
            try:
                saved, errors = upsert_products_batch(frescos, niche)
                total_saved += saved
                total_errors += errors
            except Exception as e:
                log.error("save", f"Falha ao salvar categoria [{cat.get('categoria')}]: {e}",
                          exc=e)

    log.info("deduplicate", f"{total_unique} produtos únicos",
             unique=total_unique, categories_ok=cat_ok, categories_fail=cat_fail)

    if total_unique == 0:
        log.warning("save", "Nenhum produto para salvar")
        empty = _register_empty_round(niche.key)
        log.warning("monitor", f"Rodadas consecutivas sem produtos: {empty}/{_ALERT_AFTER}",
                    consecutive=empty)
        if empty >= _ALERT_AFTER:
            _check_alert(log, niche,
                f"Scraping zerou por {empty} rodadas seguidas. "
                f"O Mercado Livre pode ter mudado o HTML. "
                f"Categorias: {cat_ok} OK, {cat_fail} sem produtos.")
        return

    _register_success(niche.key)

    log.info("save", f"{total_saved} salvos, {total_errors} erros",
             saved=total_saved, errors=total_errors)

    log.info("done", f"Scraping finalizado: {total_saved} produtos salvos",
             duration_ms=t_total.ms, total_products=total_saved, categories=len(categories))


def _check_alert(log: OpLogger, niche: Niche, message: str):
    """Envia alerta crítico via WhatsApp (campanha admin)."""
    log.error("alert", f"ALERTA CRÍTICO [{niche.key}]: {message}")
    send_alert(f"⚠️ ALERTA BOT PROMO [{niche.key.upper()}] ⚠️\n\n{message}\n\n"
               f"Verifique os logs em /logs?level=ERROR&niche={niche.key}")
