# Spec — Campanha de aquisição paga (teste, nicho carros)

- **Data:** 2026-06-22
- **Status:** em revisão

## Objetivo
Provar a **unidade econômica** da aquisição paga em **1 nicho (carros)**: adquirir membros via Meta Ads (click-to-WhatsApp) por um custo **abaixo do LTV**, medindo o funil completo até a comissão atribuída. Só replicar para outros nichos se o payback fechar.

Foco em 1 nicho primeiro porque o orçamento é limitado e o objetivo é validar barato antes de escalar.

## Regra econômica (o coração da campanha)
O número que decide não é CPA nem ROAS do dia 1 — é a relação **CPA × LTV**, porque se paga uma vez pelo membro mas ele gera comissão por meses.

- **LTV** = comissão/membro/mês × **vida média do membro**
- **vida média (meses) = 1 ÷ churn mensal** (medido no SendFlow)
- **CPA alvo ≤ 50% do LTV** → dobra o investido na vida do membro
- **Teto absoluto (break-even) = 100% do LTV** → acima disso, prejuízo

Baseline conhecido hoje (grupo geral): **~R$1 de comissão por membro/mês** (350 membros ≈ R$350/mês).

A base de cálculo tem que ser a **vida média real medida pelo churn**, nunca um "6 meses" assumido. Tabela de referência com R$1/mês:

| Churn mensal | Vida média | LTV | CPA alvo (50%) | Teto (break-even) |
|---|---|---|---|---|
| ~17%/mês | ~6 meses | R$6 | R$3,00 | R$6 |
| ~33%/mês | ~3 meses | R$3 | R$1,50 | R$3 |
| ~10%/mês | ~10 meses | R$10 | R$5,00 | R$10 |

**Ordem obrigatória:** puxar o churn no SendFlow → calcular vida média e LTV → fixar o CPA alvo **antes** de gastar em anúncio. O nicho carros pode valer mais que R$1/membro/mês (oferta mais relevante); o teste mede o LTV real de carros pela tag.

## Funil
```
Anúncio Meta CTWA → clique → entra no grupo carros → bot manda promoções (já roda)
→ clica link de afiliado c/ tag carros → compra no ML → comissão atribuída à tag
```

## Setup (uma vez)
1. **Tag/SubID "carros"** carimbada nos links de afiliado enviados ao grupo carros → permite atribuir vendas ao grupo no painel ML.
   - *Ponto técnico a validar na implementação:* se a geração de link de afiliado do ML aceita SubID/tag por link. Se não aceitar, usar a separação física (1 grupo = 1 nicho) + leitura por tag/relatório no painel ML.
2. **Campanha Meta CTWA**, destino = entrada do grupo carros.
3. **Criativo:** ângulo "Grupo VIP de ofertas de carros — achados com até X% OFF direto no seu WhatsApp." Segmentação por interesse (carros, marcas, mecânica), Brasil.
4. **Orçamento de teste:** ~R$20–30/dia por 2–3 semanas (dentro da verba de teste).

## Métricas (medição semanal, por nicho)
1. **CPA de conversa** e **CPA de membro que entrou** — Meta Ads + contagem de membros
2. **Taxa de entrada** = conversa iniciada → entrou no grupo
3. **Retenção D7/D30 e vida média/churn** — SendFlow
4. **LTV carros** = comissão/membro/mês × vida média (calibrado ao vivo)
5. **Comissão atribuída pela tag** — painel ML
6. **CPA de membro retido D30 vs LTV** — número-chave
7. **Payback** (dias até comissão acumulada > gasto) e **ROAS acumulado**

## Critérios de decisão
- ✅ **Sucesso:** CPA de membro retido D30 **≤ 50% do LTV** de carros
- 🔴 **Matar o pago:** CPA de membro retido **≥ LTV** (break-even) sem tendência de queda → volta para orgânico (CAC ≈ 0)
- 🔁 **Replicar p/ próximo nicho:** quando carros mostrar payback **< ~3–4 meses**

## Não-objetivos (YAGNI até validar)
- Landing page / página de ponte
- Comunidade ou Canal do WhatsApp
- Escalar orçamento antes do payback fechar
- Mexer no bot de scraping/envio (já funciona)

## Plano B
Se o CPA pago vier acima do teto sem queda, a aquisição vira **orgânica** (Reels/TikTok mostrando achados → link do grupo; indicação de membros). CAC ≈ 0, a conta sempre fecha, só cresce mais devagar.

## Validação manual
- Antes de gastar: churn do geral no SendFlow → LTV → CPA alvo fixado.
- Durante: planilha semanal com as 7 métricas acima, por nicho.
- Decisão de replicar/matar baseada nos critérios, não em sensação.
