# Spec — Cooldown de categoria no envio (anti-repetição por tipo)

- **Data:** 2026-06-23
- **Status:** aprovado (implementar)

## Problema
O dedup atual só impede o **mesmo produto** (nome similar) repetir. Nada impede mandar vários produtos da **mesma categoria em sequência** (ex.: 5 eletrônicos seguidos). Como o catálogo concentra em algumas categorias, a clusterização é provável — e cansa o membro (variedade percebida baixa → churn).

## Solução
Cooldown de categoria no envio: não repetir uma categoria nos últimos **N = 5** envios (do nicho). Com fallback gracioso (se todos os candidatos livres estiverem em cooldown, envia mesmo e loga).

## Parâmetro
- `CATEGORY_COOLDOWN = 5` (configurável). 5 = forte variedade sem starvation, dado que o catálogo concentra em poucas categorias. Subir depois se o log de fallback mostrar folga.
- Pool de candidatos do envio: 20 → **40**, pra dar mais categorias ao filtro.

## Design
### Schema (migração aditiva, reversível)
`ALTER TABLE "Produtos" ADD COLUMN IF NOT EXISTS categoria text;` (idem `Produtos_carros`). Guarda o nome da categoria de origem do scrape.

### `app/scraper.py`
- `run_scraping`: ao acumular os produtos de cada categoria, marca `p["categoria"] = cat["categoria"]`. (A dedup por id já existente mantém o primeiro que achar.)

### `app/database.py`
- `_upsert_sql`: incluir coluna `"categoria"` e valor `%(categoria)s`; `ON CONFLICT … DO UPDATE` também seta `categoria` (backfill quando NULL, junto da regra de social_proof).
- `upsert_product`: `"categoria": product.get("categoria")`.
- `get_ready_candidates(niche, limit=40)`: bump do default p/ 40.

### `app/whatsapp.py`
- Estado em memória por nicho: `_recent_categories: dict[str, deque(maxlen=CATEGORY_COOLDOWN)]` (mesmo padrão do `_consecutive_empty` do scraper).
- `run_send_whatsapp`: após o filtro de nome (`frescos`), filtrar candidatos cuja `categoria` **não** esteja na fila recente (categoria `None` nunca é bloqueada). Cadeia de fallback: `sem_categoria_repetida or frescos or candidatos`.
- Após envio bem-sucedido (`mark_as_sent`), dar `append` da categoria na fila.
- Log: quantos candidatos foram pulados por categoria e se caiu em fallback.

## Não-objetivos
- Cota por categoria por janela de tempo (só sequência).
- Persistir a fila entre restarts (em memória; após deploy, auto-corrige em poucos envios).
- Mudar o dedup por nome (continua).

## Testes (pytest)
- `run_send_whatsapp`: com candidatos de categorias variadas e fila recente contendo a categoria A, o escolhido **não** é da A; quando só sobram candidatos da A (todos em cooldown), envia em fallback.
- categoria `None` não é bloqueada.
- após envio, a categoria entra na fila (maxlen respeitado).

## Validação manual (pós-deploy)
Re-scrape p/ popular `categoria` → alguns `POST /trigger/whatsapp?niche=geral` → conferir nos logs `categoria_pulada` e a sequência de categorias enviadas (sem repetir em janela de 5).
