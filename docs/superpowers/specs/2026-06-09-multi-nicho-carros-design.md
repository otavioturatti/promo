# Spec — Suporte a múltiplos nichos (grupo de carros)

- **Data:** 2026-06-09
- **Status:** aprovado para planejamento
- **Autor:** otavio (+ Claude)

## 1. Contexto e objetivo

O bot atual (`ML Afiliados`) raspa ofertas do Mercado Livre, gera links de afiliado
e divulga num grupo de WhatsApp. Hoje as tabelas (`categorias`, `Produtos`, `logs`)
e a campanha de envio são **fixas** no código.

Queremos suportar um segundo grupo nichado (**carros**) e, de forma extensível,
N grupos no futuro — cada um com seu catálogo, suas categorias, seus logs, sua
campanha e seus **horários próprios** — sem duplicar código nem alterar a lógica
a cada novo nicho.

## 2. Decisões travadas

1. **Parametrização por nicho.** Um único código e **um único deploy** rodando
   todos os nichos em paralelo no mesmo scheduler.
2. **Tabelas isoladas por nicho** via sufixo: `categorias_carros`,
   `Produtos_carros`, `logs_carros`. O grupo geral mantém as tabelas atuais.
3. **Credenciais compartilhadas.** Mesma conta de afiliado do ML
   (`ML_COOKIES`, `ML_CSRF_TOKEN`, `ML_AFFILIATE_TAG`) e mesma conta SendFlow
   (`SENDFLOW_TOKEN`, `SENDFLOW_ACCOUNT_ID`). **Só o `release_id` da campanha
   varia por nicho.**
4. **Definição dos nichos em lista declarativa no código** (`config.py`).
5. **Horários por nicho**, declarados na própria entrada do nicho. Cada tipo de
   job aceita uma **lista de specs cron** (permite janelas com início/fim
   "quebrados", como 06:30–22:00).
6. **Extensibilidade:** adicionar um nicho no futuro =
   `+1 entrada em NICHES` + `+1 variável de campanha no .env` +
   `criar as 3 tabelas no Supabase`. **Nenhum módulo de lógica é tocado.**

## 3. Não-objetivos (YAGNI)

- Não criar painel/UI para gerenciar nichos.
- Não separar credenciais ML/SendFlow por nicho.
- Não migrar dados existentes — o grupo geral permanece nas tabelas atuais.
- Não alterar a lógica de scraping/parse/geração de link/envio em si.
- Não tornar a retenção de limpeza (8 dias / 15 dias) configurável por nicho
  (segue global por enquanto).

## 4. Modelo de nicho (`config.py`)

Núcleo de toda a parametrização. Tudo que varia entre grupos vive aqui.

```python
from dataclasses import dataclass

@dataclass(frozen=True)
class Niche:
    key: str                 # "geral" | "carros"
    table_categorias: str    # "categorias" | "categorias_carros"
    table_produtos: str      # "Produtos"   | "Produtos_carros"
    table_logs: str          # "logs"       | "logs_carros"
    sendflow_release_id: str # campanha SendFlow do grupo
    schedules: dict          # {job_name: [ {cron kwargs}, ... ]}

NICHES = [
    Niche(
        key="geral",
        table_categorias="categorias",
        table_produtos="Produtos",
        table_logs="logs",
        sendflow_release_id=SENDFLOW_RELEASE_ID,
        schedules={
            "scraping":  [{"hour": "6-22", "minute": 0}],
            "affiliate": [{"hour": "6-22", "minute": 5}],
            "retry":     [{"hour": "*/4"}],
            "whatsapp":  [{"hour": "6-22", "minute": "*/7"}],
        },
    ),
    Niche(
        key="carros",
        table_categorias="categorias_carros",
        table_produtos="Produtos_carros",
        table_logs="logs_carros",
        sendflow_release_id=SENDFLOW_RELEASE_ID_CARROS,
        schedules={
            "scraping":  [{"hour": "6-22", "minute": 15}],
            "affiliate": [{"hour": "6-22", "minute": 20}],
            "retry":     [{"hour": "*/4", "minute": 30}],
            "whatsapp":  [
                {"hour": 6,      "minute": "30-55/5"},  # 06:30 … 06:55
                {"hour": "7-21", "minute": "*/5"},      # 07:00 … 21:55
                {"hour": 22,     "minute": 0},          # 22:00
            ],
        },
    ),
]

NICHE_BY_KEY = {n.key: n for n in NICHES}
```

**Horários:**

| Job       | Geral (mantido)        | Carros                                   |
|-----------|------------------------|------------------------------------------|
| scraping  | 06–22h, min :00        | 06–22h, min :15                          |
| affiliate | 06–22h, min :05        | 06–22h, min :20                          |
| retry     | a cada 4h              | a cada 4h, min :30                       |
| whatsapp  | 06–22h, a cada 7 min   | **a cada 5 min, 06:30–22:00** (3 faixas) |

> Os horários de scraping/affiliate/retry de carros foram derivados da janela de
> envio (06:15 raspa → 06:20 gera links → 06:30 começa a enviar) e deslocados do
> grupo geral para não dobrar a carga no ML no mesmo minuto. São ajustáveis
> editando a entrada do nicho.

## 5. Variáveis de ambiente (`.env` / `.env.example`)

Uma variável nova:

```
# Campanha do grupo de carros
SENDFLOW_RELEASE_ID_CARROS=xxxxxxxxxxxxxxxxxxxx
```

`config.py` passa a ler `SENDFLOW_RELEASE_ID_CARROS = os.getenv("SENDFLOW_RELEASE_ID_CARROS", "")`.

## 6. Camada de dados (`database.py`)

Todas as funções com nome de tabela hardcoded passam a **receber o `Niche`** e
compõem o SQL com `psycopg2.sql.Identifier` — forma correta e segura de injetar
**identificador** (nome de tabela) dinâmico; `%s` continua só para valores.

Exemplo:

```python
from psycopg2 import sql

def get_pending_products(niche: Niche) -> list[dict]:
    with get_conn() as conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            sql.SQL('SELECT * FROM {} WHERE "Status" = %s ORDER BY "created_at" DESC')
               .format(sql.Identifier(niche.table_produtos)),
            ("PENDENTE",),
        )
        return cur.fetchall()
```

Funções afetadas (todas ganham `niche`):
`get_active_categories`, `upsert_product`, `upsert_products_batch`,
`update_affiliate_link`, `mark_as_sent`, `mark_as_failed`,
`count_affiliate_failures` (consulta a tabela de logs do nicho),
`get_pending_products`, `get_ready_with_null_links`, `get_next_product_to_send`,
`cleanup_old_products`, `cleanup_null_links`, `query_logs`.

## 7. Logs por nicho (`logger.py`)

- `OpLogger(module, niche)` — cada log conhece sua tabela de destino
  (`niche.table_logs`).
- Cada item enfileirado carrega `log_table`. `_flush_to_db` **agrupa o batch por
  tabela** e faz um `INSERT` por tabela. Mantém-se um único worker/fila.
- `cleanup_old_logs()` varre as tabelas de log de **todos os nichos**.
- Logs de sistema (startup/shutdown em `main.py`) usam o nicho `geral`.

## 8. Jobs (`scraper.py`, `affiliate.py`, `whatsapp.py`)

Todas as funções `run_*` recebem `niche`:

- **scraper:** `run_scraping(niche)`. O contador `_consecutive_empty` vira um
  `dict` por nicho (`{"geral": 0, "carros": 0}`) para não misturar a contagem
  entre grupos. Mensagens de alerta citam o nicho.
- **affiliate:** `run_affiliate_generation(niche)` e `run_retry_null_links(niche)`.
  `create_affiliate_link` não muda (credenciais ML compartilhadas).
- **whatsapp:** `send_text_message` usa `niche.sendflow_release_id`.
  `format_message` não muda. `send_alert` continua na campanha admin
  compartilhada (`SENDFLOW_ALERT_RELEASE_ID`), com o nome do nicho na mensagem.

## 9. Scheduler e endpoints (`main.py`)

`setup_jobs()` fica genérico — nunca mais é editado para novos nichos:

```python
from functools import partial

JOB_FUNCS = {
    "scraping":  run_scraping,
    "affiliate": run_affiliate_generation,
    "retry":     run_retry_null_links,
    "whatsapp":  run_send_whatsapp,
}

for niche in NICHES:
    for job_name, crons in niche.schedules.items():
        for i, cron in enumerate(crons):
            suffix = f"_{i}" if len(crons) > 1 else ""
            scheduler.add_job(
                partial(JOB_FUNCS[job_name], niche),
                CronTrigger(timezone=TZ, **cron),
                id=f"{job_name}_{niche.key}{suffix}",
                replace_existing=True,
            )

# cleanup global (varre todos os nichos) — 1 job à meia-noite
scheduler.add_job(run_cleanup, CronTrigger(hour=0, minute=0, timezone=TZ),
                  id="cleanup", replace_existing=True)
```

`run_cleanup()` itera sobre `NICHES` chamando `cleanup_old_products(niche)` e
`cleanup_null_links(niche)` para cada um; `cleanup_old_logs()` já varre todas as
tabelas de log.

**Endpoints:**

- `GET /health` — lista os jobs de todos os nichos (comportamento atual já serve).
- `GET /logs` — ganha `?niche=geral|carros` (default `geral`, retrocompatível);
  consulta a tabela de logs do nicho.
- `POST /trigger/{scraping,affiliate,whatsapp,full}` — ganham `?niche=` opcional;
  **sem o parâmetro, dispara para todos os nichos**. Nicho inválido → 400.
- `POST /trigger/alert` — inalterado (campanha admin).

## 10. Banco de dados (executar no Supabase)

Clonar a estrutura exata das tabelas atuais (traz tipos, defaults, constraints e
índices — evita reescrever DDL à mão):

```sql
CREATE TABLE "categorias_carros" (LIKE "categorias" INCLUDING ALL);
CREATE TABLE "Produtos_carros"   (LIKE "Produtos"   INCLUDING ALL);
CREATE TABLE "logs_carros"       (LIKE "logs"       INCLUDING ALL);
```

Depois, popular `categorias_carros` com as URLs de ofertas de carros. As tabelas
do grupo geral permanecem intactas.

## 11. Considerações e riscos

- **Rate-limit do ML:** os dois nichos usam a mesma conta de afiliado e rodam em
  paralelo. O `time.sleep(3)` entre produtos passa a valer por nicho, então a
  taxa combinada pode dobrar em janelas coincidentes. Os horários deslocados
  mitigam; monitorar e, se preciso, defasar mais ou serializar.
- **Threadpool do scheduler:** `BackgroundScheduler` usa `ThreadPoolExecutor(10)`
  por padrão. Com 2 nichos é folgado; ao crescer o número de nichos, aumentar
  `max_workers` (≈ nichos × jobs simultâneos).
- **Envio a cada 5 min em carros:** a lista de 3 faixas cobre 06:30–22:00 com
  precisão (06:30…06:55, 07:00…21:55, 22:00).

## 12. Plano de validação

1. Criar as 3 tabelas `_carros` no Supabase e popular `categorias_carros`.
2. Definir `SENDFLOW_RELEASE_ID_CARROS` no `.env`.
3. Subir local; `GET /health` deve listar os jobs dos dois nichos com
   `next_run` coerentes (carros: whatsapp a cada 5 min na janela).
4. `POST /trigger/scraping?niche=carros` → popular `Produtos_carros` (Status
   PENDENTE).
5. `POST /trigger/affiliate?niche=carros` → links gerados, Status PRONTO.
6. `POST /trigger/whatsapp?niche=carros` → envio na **campanha de carros**.
7. `GET /logs?niche=carros` → logs gravados em `logs_carros`.
8. Confirmar que o grupo **geral** segue funcionando sem alteração.

## 13. Arquivos impactados

- `app/config.py` — dataclass `Niche`, lista `NICHES`, `NICHE_BY_KEY`, nova env.
- `app/database.py` — todas as funções parametrizadas por `niche` + `sql.Identifier`.
- `app/logger.py` — `OpLogger(module, niche)`, fila com `log_table`, flush por
  tabela, cleanup multi-tabela.
- `app/scraper.py` — `run_scraping(niche)`, `_consecutive_empty` por nicho.
- `app/affiliate.py` — `run_affiliate_generation(niche)`, `run_retry_null_links(niche)`.
- `app/whatsapp.py` — `run_send_whatsapp(niche)`, release por nicho.
- `app/main.py` — `setup_jobs()` genérico, `run_cleanup()` multi-nicho, endpoints
  com `?niche=`.
- `.env` / `.env.example` — `SENDFLOW_RELEASE_ID_CARROS`.
- **Supabase (manual):** criar `categorias_carros`, `Produtos_carros`, `logs_carros`.
