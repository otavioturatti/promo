# Spec — Crescimento líquido dos grupos (retenção + aquisição)

- **Data:** 2026-06-22
- **Status:** em revisão

## Diagnóstico (dados reais)
- **Receita (painel ML, "Minha receita"):** ~R$150–180/mês (Nov/25–Mai/26; Jan/26 foi outlier alto a R$302; Jun parcial).
- **Membros:** ~256.
- **Valor por membro:** ~**R$0,64/membro/mês** (≈ R$165 ÷ 256). *(Mais baixo que a estimativa inicial de R$1.)*
- **Churn (últimos 90 dias):** saíram **176** (~59/mês), entraram **128** (~43/mês) → **saldo −16/mês**. Churn ≈ **21%/mês** → **vida média ≈ 5 meses**.

**Conclusão central: os grupos estão encolhendo.** O objetivo, portanto, não é "encher o grupo" — é virar o **saldo líquido (entradas − saídas) para positivo** de forma sustentável.

## Regra econômica
- **LTV** = valor/membro/mês × vida média = R$0,64 × ~5 ≈ **R$3,00**
- **CPA alvo ≤ 50% do LTV** ≈ **R$1,50**
- **Teto absoluto (break-even) = LTV** ≈ **R$3,00**
- vida média = 1 ÷ churn mensal (medido no SendFlow; recalcular conforme a retenção mudar)

**Implicação honesta:** com R$0,64/membro/mês, o CPA alvo (~R$1,50) é **apertado** para tráfego pago. Conseguir membro retido abaixo disso no Meta é difícil. Logo, o **orgânico (CAC ≈ 0) é o caminho mais seguro**, e o teste pago serve para *provar se o pago é viável*, não para assumir que é.

## Duas frentes (medidas juntas)

### Frente A — Retenção (prioritária; mexe nos dois lados)
Reduzir churn sobe o LTV **e** reduz quantos membros é preciso repor. Alavancas, da mais barata pra mais cara:
1. **Dedup de repetição** — já ativa em produção para todos os nichos (PR #2). **Ação:** monitorar o churn no SendFlow nas próximas 4–6 semanas para medir o efeito (o churn de 21% é pré-dedup).
2. **Frequência de envio** — avaliar se o volume de mensagens está afugentando (geral envia a cada 7 min; carros a cada 5 min nas faixas). Testar reduzir frequência e medir churn.
3. **Relevância da oferta** — % de desconto e categoria; ofertas fracas/repetitivas cansam.

**Métrica-guia:** churn mensal por nicho e saldo líquido (entradas − saídas).

### Frente B — Aquisição (teste, 1 nicho primeiro)
Foco em **1 nicho** por causa do orçamento limitado; replicar só se fechar.
- **Canal primário:** orgânico (Reels/TikTok mostrando achados → link do grupo; indicação de membros).
- **Teste pago em paralelo:** Meta Ads click-to-WhatsApp, ~R$20–30/dia por 2–3 semanas, **só para descobrir o CPA real** e comparar com o teto de R$3.
- **Atribuição:** tag/SubID por grupo nos links de afiliado → comissão atribuída ao grupo no painel ML. *(Ponto técnico a validar: se a geração de link do ML aceita SubID; senão, separação física por grupo + leitura por tag no painel.)*
- **Qual nicho:** o mais segmentável no Meta + boa comissão. "Geral" é ruim para anúncio (audiência ampla). Candidato natural: **carros** (interesse claro, já montado). Carros, sendo nichado, pode valer mais que R$0,64/membro/mês — a tag mede isso.

## Métricas (medição semanal, por nicho)
1. **Saldo líquido** = entradas − saídas *(SendFlow)* ← métrica-mãe
2. Churn mensal e vida média *(SendFlow)*
3. CPA de membro que entrou (Meta) e CPA de membro retido D30
4. LTV recalculado = valor/membro/mês × vida média
5. Comissão atribuída por tag *(painel ML)*
6. Payback (dias até comissão acumulada > gasto) e ROAS acumulado

## Critérios de decisão
- ✅ **Aquisição paga vale:** CPA de membro retido D30 **≤ 50% do LTV** (~R$1,50)
- 🔴 **Matar o pago:** CPA de membro retido **≥ LTV** (~R$3) sem tendência de queda → fica só no orgânico
- 🔁 **Replicar p/ próximo nicho:** quando o nicho-teste mostrar **saldo líquido positivo** e payback < ~3–4 meses
- 🎯 **Sucesso geral do projeto:** saldo líquido dos grupos > 0 de forma sustentável

## Não-objetivos (YAGNI)
- Landing page / página de ponte
- Comunidade ou Canal do WhatsApp
- Escalar orçamento de anúncio antes do payback fechar
- Mexer no bot de scraping/envio (já funciona)

## Validação manual
- Antes de gastar em anúncio: puxar churn atualizado no SendFlow → LTV → fixar CPA alvo.
- Semanal: planilha com as 6 métricas por nicho; decisões pelos critérios, não por sensação.
- 4–6 semanas: reavaliar churn pós-dedup para recalibrar o LTV.
