# Spec — Deduplicação de produtos similares no envio (Fase 1)

- **Data:** 2026-06-16
- **Status:** aprovado (design validado nos logs reais de produção)

## Problema
O bot deduplica por `id_produto` (MLB), mas o mesmo produto físico tem **N anúncios** (vendedores/variações diferentes) com `MLB` ids distintos. O controle de `Status=ENVIADO` nunca pega isso, então produtos idênticos/quase-idênticos são enviados ao grupo em janelas curtas.

**Evidência (logs de carros, 78 envios em ~15h):** ~13% dos envios eram redundantes — ex.: "Luva moto Sport" enviada **4x**, "Aromatizante Gecko Audi" **4x** (2 com título idêntico, a 15 min), Retrovisor Kawasaki Z400 2x, Scanner Jdiag M100 2x, Capa de chuva moto 2x.

## Solução (Fase 1 — dedup no envio)
Ao escolher o próximo produto, **pular candidatos cujo NOME seja similar a algum produto já ENVIADO** ainda presente no catálogo. Similaridade = **Jaccard de tokens normalizados ≥ 0,5** (calibrado nos dados reais: agrupa os duplicados verdadeiros e NÃO agrupa variações legítimas, como pneus de medidas diferentes).

Aplica a **todos os nichos** (a lógica é parametrizada por `niche`).

## Design

### Novo módulo `app/dedup.py`
```python
def normalize_tokens(nome: str) -> set[str]:
    # minúsculas, sem acento, palavras com >= 3 caracteres
def jaccard(a: set, b: set) -> float:
    # |a∩b| / |a∪b| (0.0 se algum vazio)
def is_duplicate(nome: str, sent_names: list[str], threshold: float = 0.5) -> bool:
    # True se jaccard(tokens(nome), tokens(s)) >= threshold para algum s
```

### `app/database.py`
- `get_ready_candidates(niche, limit=20)` — os 20 produtos PRONTO mais recentes (com `Link_de_afiliado` não nulo). Substitui o `ORDER BY RANDOM()` do antigo `get_next_product_to_send` (o sorteio passa a ser no Python, após a dedup).
- `get_recent_sent_names(niche, limit=500)` — os nomes (`Nomes_Produtos`) dos produtos `Status='ENVIADO'` mais recentes (cooldown natural: o cleanup já remove após 8 dias). `limit` protege a performance.
- Remover `get_next_product_to_send` (substituído).

### `app/whatsapp.py` — `run_send_whatsapp(niche)`
```python
candidatos = get_ready_candidates(niche)
if not candidatos: return  # nada pronto
enviados = get_recent_sent_names(niche)
frescos = [c for c in candidatos if not is_duplicate(c["Nomes_Produtos"], enviados)]
escolhidos = frescos or candidatos          # fallback: se todos duplicados, envia mesmo
product = random.choice(escolhidos)
# log: quantos candidatos, quantos duplicados pulados
```
Resto do fluxo (format → send → mark_sent) inalterado.

## Parâmetros
- `threshold = 0.5`
- janela de cooldown = produtos ENVIADOS ainda no catálogo (~8 dias via cleanup), limitado a 500 mais recentes.

## Não-objetivos (Fase 1)
- Dedup no `save` (limpar o catálogo na raiz) → **Fase 2**, avaliada depois.
- Limitar concentração por tipo (ex.: 3 pneus seguidos) → fora de escopo.
- Mudança de schema → nenhuma (usa colunas existentes).

## Testes (pytest)
- `dedup`: `normalize_tokens` (acento/caixa/curtas), `jaccard`, `is_duplicate` com casos reais — "Audi - Aromatizante Gecko..." vs "Aromatizante Audi Gecko..." → duplicado; "Kit 4 Pneus 175/70r14" vs "Kit 2 Pneus 205/70r15" → NÃO duplicado.
- `whatsapp`: `run_send_whatsapp` pula candidato similar a um enviado recente e envia um fresco (com `get_ready_candidates`/`get_recent_sent_names`/`send_text_message` mockados).

## Validação manual (pós-deploy)
`POST /trigger/whatsapp?niche=carros` algumas vezes + `/logs?niche=carros` → conferir o log de "duplicados pulados" e ausência de repetição.
