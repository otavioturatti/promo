# Spec — Paginação no scraper (&page=N)

- **Data:** 2026-06-23
- **Status:** aprovado (implementar)

## Problema
`scrape_category` faz **1 GET** por categoria e lê só os ~48 cards da página 1. O catálogo fica raso → repetição no envio (medido: ~13% dos envios do geral em fallback). A página 1 também tende a repetir os mesmos "top deals" entre rodadas.

## Descoberta
A página `/ofertas?category=MLBxxxx&page=N` é **renderizada no servidor** (testado: `&page=2` traz 48 produtos diferentes). Dá pra paginar com um laço, sem browser/JS.

## Solução
Paginar dentro de `scrape_category`, com teto configurável por nicho e **parada antecipada** quando a página vem vazia.

### `app/scraper.py`
- `_with_page(url, page)` — adiciona `page=N` à query preservando o fragmento (`#filter_applied=...`); `page<=1` devolve a URL intacta (sem mudança de comportamento).
- `_scrape_page(url, log, min_discount) -> (produtos, n_cards)` — extrai o GET+parse+filtro de UMA página (o corpo atual da função) e devolve também a contagem bruta de cards.
- `scrape_category(url, log, min_discount=30, max_pages=1)` — itera `page` de 1 a `max_pages`, acumula produtos e **para quando `n_cards == 0`** (fim dos resultados → categorias pequenas custam só 1 request).
- `run_scraping` passa `niche.scrape_max_pages` para `scrape_category`. A dedup por `id` no `run_scraping` (já existe) cobre eventual sobreposição entre páginas.

### `app/config.py`
- Novo campo `Niche.scrape_max_pages: int = 1` (default mantém comportamento atual).
- `geral` e `carros`: `scrape_max_pages = 3`.

## Parâmetros / tradeoff
- `max_pages` é o único botão: mais páginas = catálogo mais fundo, porém **+1 request por categoria por página** = scrape mais lento e **mais exposição de ToS**. A parada antecipada limita o custo (categoria com 1 página de resultado = 1 request). Começar em 3, ajustável.

## Não-objetivos
- Paginar via offset `_Desde_` ou scroll JS (não funcionam no SSR — testado).
- Sleep/throttle entre requests (manter comportamento atual; avaliar se o ML reclamar).
- Mudar schema ou o fluxo de envio.

## Testes (pytest)
- `_with_page`: `page=1` intacto; `page=2` adiciona `page=2` preservando fragmento.
- `scrape_category`: com `_scrape_page` mockado, pagina e **para na página vazia** (não busca além); `max_pages=1` (default) faz 1 chamada (compatibilidade).
- `config`: `scrape_max_pages` presente e com os valores esperados por nicho.

## Validação manual (pós-deploy)
`POST /trigger/scraping?niche=geral` → `/logs?niche=geral&module=scraper` → conferir o log `paginate` e o volume salvo maior. Em ~2-3 dias, remedir o fallback no envio.
