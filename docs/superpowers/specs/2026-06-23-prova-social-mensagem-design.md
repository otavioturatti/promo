# Spec — Prova social na mensagem (conversão)

- **Data:** 2026-06-23
- **Status:** aprovado (implementar; sem A/B por enquanto)

## Objetivo
Enriquecer a mensagem do WhatsApp com **prova social honesta** (nota, nº de avaliações, badge da ML) e **ancoragem reforçada** (R$ economizado), pra aumentar conversão. Princípios: prova social (Cialdini), ancoragem (Kahneman/Tversky), carga cognitiva baixa.

## Viabilidade (verificada)
O card de `/ofertas` expõe, com cobertura medida em 3 categorias:
- `.poly-reviews__rating` → nota (48/48)
- `.poly-reviews__total` → nº avaliações (48/48)
- `.poly-component__highlight` → badge "MAIS VENDIDO" / "OFERTA IMPERDÍVEL" / "OFERTA DO DIA" (~43/48)

**Frete fica de fora:** vem como "...por ser sua primeira compra" (específico de quem vê) → seria desonesto pro grupo.

## Mensagem (novo formato)
```
Kit 6 Cuecas Lupo Boxer Sem Costura
🏆 MAIS VENDIDO  ·  ⭐ 4,9 (49 mil avaliações)

De: R$129,90 Por: R$59,90 (54% OFF — economize R$70)

https://meli.la/xxxx
GARANTA O SEU AQUI
```
Linha de prova social só aparece se houver dados (degrada pro formato atual sem ela).

## Design
### Schema (migração aditiva, reversível)
`ALTER TABLE "Produtos" ADD COLUMN IF NOT EXISTS social_proof text;` (idem `Produtos_carros`). Guarda JSON: `{"rating":"4,9","reviews":49256,"badge":"MAIS VENDIDO"}`.

### `app/scraper.py`
- `_extract_social(card_div)` → JSON (ou None) com rating/reviews/badge.
- `_scrape_page`: `card_divs = soup.select("div.poly-card__content")` (mesma contagem que `names`); monta `socials` alinhado e injeta `"social"` no dict do produto.

### `app/database.py`
- `_upsert_sql`: incluir coluna `"social_proof"` e valor `%(social)s`; `ON CONFLICT … DO UPDATE` também atualiza `social_proof`.
- `upsert_product`: `"social": product.get("social")`.

### `app/whatsapp.py`
- `_num(s)` robusto (aceita `R$131.61` e `R$500,00`), `_brl(v)`, `_fmt_reviews(n)` (≥1000 → "N mil"), `_format_social(json)`.
- `format_message`: adiciona linha de prova social (se houver) e "economize R$X" no bloco de preço.

## Não-objetivos
- A/B controlado (grupo único não permite limpo) → sobe como padrão, observa tendência agregada.
- Mostrar frete (condicional ao comprador).
- Mudar seleção/peso de produtos (outro lever).

## Testes (pytest)
- `_extract_social`: HTML com/sem badge → JSON correto / None.
- `format_message`: com prova social mostra badge+nota+avaliações+economia; sem prova social degrada pro formato antigo (atualizar `test_format_message`).
- `_num`: formatos ponto e vírgula.

## Validação manual
Scrape real de 1 categoria → conferir `social_proof` salvo; `format_message` de um produto real → conferir a mensagem. Pós-deploy: observar venda por tag/clique nas semanas seguintes.
