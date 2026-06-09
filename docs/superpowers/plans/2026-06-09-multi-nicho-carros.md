# Suporte a Múltiplos Nichos (grupo de carros) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Permitir que o bot opere N grupos nichados (começando por "carros") a partir de um único código e um único deploy, cada nicho com suas próprias tabelas, sua campanha SendFlow e seus horários.

**Architecture:** Introduz um objeto `Niche` declarativo em `config.py` (tabelas + campanha + horários). Todas as funções de dados, logs e jobs passam a receber um `Niche`; o scheduler varre `NICHES × jobs × crons`. As tabelas viram identificadores dinâmicos via `psycopg2.sql.Identifier`. Credenciais ML/SendFlow são compartilhadas; só o `release_id` varia.

**Tech Stack:** Python 3.12, FastAPI, APScheduler, psycopg2, pytest (novo, para testes focados com mocks).

**Spec:** `docs/superpowers/specs/2026-06-09-multi-nicho-carros-design.md`

**Estratégia de incrementalidade:** as funções de `database.py` e `logger.py` ganham `niche` como **último parâmetro com default temporário** (`NICHE_BY_KEY["geral"]`), para que os callers antigos continuem funcionando entre tarefas. Os jobs/endpoints passam a enviar `niche` explícito (Tasks 5–8). A Task 9 remove os defaults de `database.py` (torna `niche` obrigatório), garantindo que nenhum caller foi esquecido. Convenção: **`niche` é sempre o último parâmetro.**

---

## Estrutura de arquivos

**Criar:**
- `requirements-dev.txt` — dependência de teste (pytest)
- `pytest.ini` — config do pytest (pythonpath, testpaths)
- `tests/__init__.py` — torna `tests` um pacote importável
- `tests/helpers.py` — fakes de conexão/cursor + extrator de identifiers
- `tests/conftest.py` — fixture autouse que bloqueia escrita real de logs no DB
- `tests/test_smoke.py`, `tests/test_config.py`, `tests/test_database.py`, `tests/test_logger.py`, `tests/test_scraper.py`, `tests/test_affiliate.py`, `tests/test_whatsapp.py`, `tests/test_main.py`

**Modificar:**
- `app/config.py` — `Niche`, `NICHES`, `NICHE_BY_KEY`, env `SENDFLOW_RELEASE_ID_CARROS`
- `app/database.py` — todas as funções parametrizadas por `niche` + `sql.Identifier`
- `app/logger.py` — `OpLogger(module, niche)`, record com `log_table`, flush por tabela, cleanup multi-tabela
- `app/scraper.py` — `run_scraping(niche)`, contador por nicho
- `app/affiliate.py` — `run_affiliate_generation(niche)`, `run_retry_null_links(niche)`, `_process_products(..., niche)`
- `app/whatsapp.py` — `run_send_whatsapp(niche)`, `send_text_message(..., release_id)`
- `app/main.py` — `setup_jobs(scheduler)` genérico, `run_cleanup` multi-nicho, `resolve_niches`, endpoints com `?niche=`
- `.env.example` — nova variável de campanha

**Banco (manual, fora do código):** criar `categorias_carros`, `Produtos_carros`, `logs_carros` no Supabase (DDL na Task 9).

---

## Task 1: Infraestrutura de testes

**Files:**
- Create: `requirements-dev.txt`
- Create: `pytest.ini`
- Create: `tests/__init__.py`
- Create: `tests/helpers.py`
- Create: `tests/conftest.py`
- Create: `tests/test_smoke.py`

- [ ] **Step 1: Criar `requirements-dev.txt`**

```
pytest==8.3.4
```

- [ ] **Step 2: Criar `pytest.ini`**

```ini
[pytest]
pythonpath = .
testpaths = tests
```

- [ ] **Step 3: Criar `tests/__init__.py`** (arquivo vazio)

```python
```

- [ ] **Step 4: Criar `tests/helpers.py`**

```python
"""Fakes e utilitários para testes sem banco/HTTP reais."""
from contextlib import contextmanager

from psycopg2 import sql


class FakeCursor:
    """Cursor que captura execute() e devolve resultados pré-definidos."""

    def __init__(self, fetch_result=None):
        self.executed = []  # lista de (query, params)
        self._fetch_result = fetch_result if fetch_result is not None else []

    def execute(self, query, params=None):
        self.executed.append((query, params))

    def fetchall(self):
        return self._fetch_result

    def fetchone(self):
        return self._fetch_result[0] if self._fetch_result else None

    @property
    def rowcount(self):
        return len(self._fetch_result)

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class FakeConn:
    def __init__(self, cursor):
        self._cursor = cursor

    def cursor(self, *a, **k):
        return self._cursor

    def commit(self):
        pass

    def rollback(self):
        pass

    def close(self):
        pass


@contextmanager
def fake_conn_cm(cursor):
    """Substituto para database.get_conn() nos testes."""
    yield FakeConn(cursor)


def identifiers_in(query) -> set:
    """Nomes de tabela (Identifier) presentes numa query sql.Composed/Identifier."""
    names = set()
    if isinstance(query, sql.Composed):
        for part in query.seq:
            if isinstance(part, sql.Identifier):
                names.update(part.strings)
    elif isinstance(query, sql.Identifier):
        names.update(query.strings)
    return names
```

- [ ] **Step 5: Criar `tests/conftest.py`**

```python
import pytest


@pytest.fixture(autouse=True)
def _no_db_log_flush(monkeypatch):
    """Impede que a thread de logs conecte ao Postgres durante os testes."""
    monkeypatch.setattr("app.logger._flush_to_db", lambda records: None, raising=False)
```

- [ ] **Step 6: Criar `tests/test_smoke.py`**

```python
def test_pacote_app_importavel():
    import app.config  # noqa: F401
    assert True
```

- [ ] **Step 7: Instalar e rodar**

Run: `pip install -r requirements-dev.txt`
Then: `python -m pytest -q`
Expected: PASS (1 passed). Confirma pythonpath e descoberta de testes.

- [ ] **Step 8: Commit**

```bash
git add requirements-dev.txt pytest.ini tests/
git commit -m "test: infraestrutura de testes (pytest + fakes de DB)"
```

---

## Task 2: Modelo de nicho em `config.py`

**Files:**
- Modify: `app/config.py`
- Create/Test: `tests/test_config.py`

- [ ] **Step 1: Escrever o teste falhando — `tests/test_config.py`**

```python
from app.config import NICHES, NICHE_BY_KEY, Niche


def test_dois_nichos_definidos():
    assert {n.key for n in NICHES} == {"geral", "carros"}


def test_nicho_carros_tem_tabelas_sufixadas():
    carros = NICHE_BY_KEY["carros"]
    assert carros.table_categorias == "categorias_carros"
    assert carros.table_produtos == "Produtos_carros"
    assert carros.table_logs == "logs_carros"


def test_nicho_geral_mantem_tabelas_atuais():
    geral = NICHE_BY_KEY["geral"]
    assert geral.table_categorias == "categorias"
    assert geral.table_produtos == "Produtos"
    assert geral.table_logs == "logs"


def test_whatsapp_carros_tem_tres_faixas_de_horario():
    carros = NICHE_BY_KEY["carros"]
    assert len(carros.schedules["whatsapp"]) == 3


def test_niche_e_imutavel():
    import dataclasses
    import pytest
    with pytest.raises(dataclasses.FrozenInstanceError):
        NICHE_BY_KEY["geral"].key = "x"
```

- [ ] **Step 2: Rodar — verificar falha**

Run: `python -m pytest tests/test_config.py -q`
Expected: FAIL com `ImportError: cannot import name 'Niche'` (ou `NICHES`).

- [ ] **Step 3: Reescrever `app/config.py`**

```python
import os
from dataclasses import dataclass

from dotenv import load_dotenv

load_dotenv()


DATABASE_URL = os.getenv("DATABASE_URL", "")

ML_COOKIES = os.getenv("ML_COOKIES", "")
ML_CSRF_TOKEN = os.getenv("ML_CSRF_TOKEN", "")
ML_AFFILIATE_TAG = os.getenv("ML_AFFILIATE_TAG", "alessandrejr")

SENDFLOW_TOKEN = os.getenv("SENDFLOW_TOKEN", "")
SENDFLOW_ACCOUNT_ID = os.getenv("SENDFLOW_ACCOUNT_ID", "")
SENDFLOW_RELEASE_ID = os.getenv("SENDFLOW_RELEASE_ID", "")
SENDFLOW_RELEASE_ID_CARROS = os.getenv("SENDFLOW_RELEASE_ID_CARROS", "")
SENDFLOW_ALERT_RELEASE_ID = os.getenv("SENDFLOW_ALERT_RELEASE_ID", "")

TZ = os.getenv("TZ", "America/Sao_Paulo")
PORT = int(os.getenv("PORT", "8000"))


@dataclass(frozen=True)
class Niche:
    key: str
    table_categorias: str
    table_produtos: str
    table_logs: str
    sendflow_release_id: str
    schedules: dict  # {job_name: [ {cron kwargs}, ... ]}


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

- [ ] **Step 4: Rodar — verificar que passa**

Run: `python -m pytest tests/test_config.py -q`
Expected: PASS (5 passed).

- [ ] **Step 5: Commit**

```bash
git add app/config.py tests/test_config.py
git commit -m "feat: modelo de nicho declarativo em config (geral + carros)"
```

---

## Task 3: Parametrizar `database.py` por nicho

**Files:**
- Modify: `app/database.py`
- Create/Test: `tests/test_database.py`

- [ ] **Step 1: Escrever o teste falhando — `tests/test_database.py`**

```python
import pytest

import app.database as database
from app.config import NICHE_BY_KEY
from tests.helpers import FakeCursor, fake_conn_cm, identifiers_in

CARROS = NICHE_BY_KEY["carros"]
GERAL = NICHE_BY_KEY["geral"]


def _patch_conn(monkeypatch, cursor):
    monkeypatch.setattr(database, "get_conn", lambda: fake_conn_cm(cursor))


def test_get_pending_products_usa_tabela_do_nicho(monkeypatch):
    cur = FakeCursor(fetch_result=[])
    _patch_conn(monkeypatch, cur)
    database.get_pending_products(CARROS)
    query, _ = cur.executed[0]
    assert "Produtos_carros" in identifiers_in(query)


def test_get_active_categories_usa_tabela_do_nicho(monkeypatch):
    cur = FakeCursor(fetch_result=[])
    _patch_conn(monkeypatch, cur)
    database.get_active_categories(CARROS)
    query, _ = cur.executed[0]
    assert "categorias_carros" in identifiers_in(query)


def test_count_affiliate_failures_usa_tabela_de_logs_do_nicho(monkeypatch):
    cur = FakeCursor(fetch_result=[(0,)])
    _patch_conn(monkeypatch, cur)
    database.count_affiliate_failures("MLB1", CARROS)
    query, params = cur.executed[0]
    assert "logs_carros" in identifiers_in(query)
    assert params == ("MLB1",)


def test_mark_as_sent_usa_tabela_do_nicho(monkeypatch):
    cur = FakeCursor()
    _patch_conn(monkeypatch, cur)
    database.mark_as_sent("MLB1", CARROS)
    query, params = cur.executed[0]
    assert "Produtos_carros" in identifiers_in(query)
    assert params == ("MLB1",)


def test_query_logs_usa_tabela_e_filtros(monkeypatch):
    cur = FakeCursor(fetch_result=[])
    _patch_conn(monkeypatch, cur)
    database.query_logs(limit=10, level="error", niche=CARROS)
    query, params = cur.executed[0]
    assert "logs_carros" in identifiers_in(query)
    assert params == ["ERROR", 10]


def test_default_e_o_nicho_geral(monkeypatch):
    cur = FakeCursor(fetch_result=[])
    _patch_conn(monkeypatch, cur)
    database.get_pending_products()  # sem niche -> geral (compat temporária)
    query, _ = cur.executed[0]
    assert "Produtos" in identifiers_in(query)
```

- [ ] **Step 2: Rodar — verificar falha**

Run: `python -m pytest tests/test_database.py -q`
Expected: FAIL (`get_pending_products()` ainda não aceita `niche` / não usa `sql.Identifier`).

- [ ] **Step 3: Reescrever `app/database.py`**

```python
from contextlib import contextmanager

import psycopg2
import psycopg2.extras
from psycopg2 import sql

from app.config import DATABASE_URL, Niche, NICHE_BY_KEY

_GERAL = NICHE_BY_KEY["geral"]  # default temporário — removido na Task 9


@contextmanager
def get_conn():
    conn = psycopg2.connect(DATABASE_URL)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ── Categorias ──────────────────────────────────────────────

def get_active_categories(niche: Niche = _GERAL) -> list[dict]:
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                sql.SQL('SELECT * FROM {} WHERE "status" = TRUE')
                   .format(sql.Identifier(niche.table_categorias))
            )
            return cur.fetchall()


# ── Produtos — Escrita ──────────────────────────────────────

def _upsert_sql(niche: Niche):
    t = sql.Identifier(niche.table_produtos)
    return sql.SQL("""
        INSERT INTO {t} (
            "Nomes_Produtos", "id_produto", "Imagem_Produtos",
            "Preco", "Link_Compra", "Status", "created_at"
        ) VALUES (
            %(nome)s, %(id_produto)s, %(imagem)s,
            %(preco)s, %(link)s, 'PENDENTE', NOW()
        )
        ON CONFLICT ("id_produto")
        DO UPDATE SET "Preco" = EXCLUDED."Preco"
        WHERE {t}."Status" != 'ENVIADO'
          AND {t}."Preco" != EXCLUDED."Preco";
    """).format(t=t)


def upsert_product(conn, product: dict, niche: Niche = _GERAL) -> bool:
    """Insere ou atualiza um produto. Retorna True se afetou alguma row."""
    with conn.cursor() as cur:
        cur.execute(_upsert_sql(niche), {
            "nome": product["nome"],
            "id_produto": product["id_produto"],
            "imagem": product["imagem"],
            "preco": product["preco"],
            "link": product["link"],
        })
        return cur.rowcount > 0


def upsert_products_batch(products: list[dict], niche: Niche = _GERAL) -> tuple[int, int]:
    """Insere batch. Retorna (salvos, erros)."""
    saved = 0
    errors = 0
    with get_conn() as conn:
        for p in products:
            try:
                if upsert_product(conn, p, niche):
                    saved += 1
            except Exception:
                errors += 1
                conn.rollback()
    return saved, errors


def update_affiliate_link(id_produto: str, link: str, niche: Niche = _GERAL):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL('UPDATE {} SET "Link_de_afiliado" = %s, "Status" = \'PRONTO\' '
                        'WHERE "id_produto" = %s')
                   .format(sql.Identifier(niche.table_produtos)),
                (link, id_produto),
            )


def mark_as_sent(id_produto: str, niche: Niche = _GERAL):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL('UPDATE {} SET "Status" = \'ENVIADO\' WHERE "id_produto" = %s')
                   .format(sql.Identifier(niche.table_produtos)),
                (id_produto,),
            )


def mark_as_failed(id_produto: str, niche: Niche = _GERAL):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL('UPDATE {} SET "Status" = \'FALHA\' WHERE "id_produto" = %s')
                   .format(sql.Identifier(niche.table_produtos)),
                (id_produto,),
            )


def count_affiliate_failures(id_produto: str, niche: Niche = _GERAL) -> int:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL("""
                    SELECT COUNT(*) FROM {}
                    WHERE product_id = %s
                      AND module = 'affiliate'
                      AND stage = 'create_link'
                      AND level = 'ERROR'
                """).format(sql.Identifier(niche.table_logs)),
                (id_produto,),
            )
            return cur.fetchone()[0]


# ── Produtos — Leitura ──────────────────────────────────────

def get_pending_products(niche: Niche = _GERAL) -> list[dict]:
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                sql.SQL('SELECT * FROM {} WHERE "Status" = \'PENDENTE\' '
                        'ORDER BY "created_at" DESC')
                   .format(sql.Identifier(niche.table_produtos))
            )
            return cur.fetchall()


def get_ready_with_null_links(niche: Niche = _GERAL) -> list[dict]:
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                sql.SQL('SELECT * FROM {} WHERE "Status" = \'PRONTO\' '
                        'AND "Link_de_afiliado" IS NULL')
                   .format(sql.Identifier(niche.table_produtos))
            )
            return cur.fetchall()


def get_next_product_to_send(niche: Niche = _GERAL) -> dict | None:
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            # Sorteia 1 entre os 20 mais recentes prontos
            cur.execute(
                sql.SQL("""
                    SELECT * FROM (
                        SELECT * FROM {}
                        WHERE "Status" = 'PRONTO'
                          AND "Link_de_afiliado" IS NOT NULL
                          AND "Link_de_afiliado" != ''
                        ORDER BY "created_at" DESC
                        LIMIT 20
                    ) recentes
                    ORDER BY RANDOM()
                    LIMIT 1
                """).format(sql.Identifier(niche.table_produtos))
            )
            return cur.fetchone()


# ── Limpeza ─────────────────────────────────────────────────

def cleanup_old_products(niche: Niche = _GERAL) -> int:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL("DELETE FROM {} WHERE \"created_at\" < NOW() - INTERVAL '8 days'")
                   .format(sql.Identifier(niche.table_produtos))
            )
            return cur.rowcount


def cleanup_null_links(niche: Niche = _GERAL) -> int:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL('DELETE FROM {} WHERE "Status" = \'PRONTO\' '
                        'AND "Link_de_afiliado" IS NULL')
                   .format(sql.Identifier(niche.table_produtos))
            )
            return cur.rowcount


# ── Logs — Leitura (endpoint /logs) ─────────────────────────

def query_logs(
    limit: int = 50,
    level: str = None,
    module: str = None,
    request_id: str = None,
    product_id: str = None,
    niche: Niche = _GERAL,
) -> list[dict]:
    clauses = []
    params = []

    if level:
        clauses.append("level = %s")
        params.append(level.upper())
    if module:
        clauses.append("module = %s")
        params.append(module)
    if request_id:
        clauses.append("request_id = %s")
        params.append(request_id)
    if product_id:
        clauses.append("product_id = %s")
        params.append(product_id)

    query = sql.SQL("SELECT * FROM {} ").format(sql.Identifier(niche.table_logs))
    if clauses:
        query = query + sql.SQL("WHERE " + " AND ".join(clauses) + " ")
    query = query + sql.SQL("ORDER BY created_at DESC LIMIT %s")
    params.append(min(limit, 500))

    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(query, params)
            rows = cur.fetchall()
            for r in rows:
                r["created_at"] = r["created_at"].isoformat() if r.get("created_at") else None
            return rows
```

> Nota: as cláusulas em `query_logs` (`"level = %s"` etc.) são literais fixos do código — os valores vão sempre em `params`, então não há injeção. O nome da tabela é um `Identifier` whitelisted.

- [ ] **Step 4: Rodar — verificar que passa**

Run: `python -m pytest tests/test_database.py -q`
Expected: PASS (6 passed).

- [ ] **Step 5: Garantir que nada quebrou e commitar**

Run: `python -m pytest -q`
Expected: PASS (todos).

```bash
git add app/database.py tests/test_database.py
git commit -m "feat: parametrizar database.py por nicho (sql.Identifier)"
```

---

## Task 4: Parametrizar `logger.py` por nicho

**Files:**
- Modify: `app/logger.py`
- Create/Test: `tests/test_logger.py`

- [ ] **Step 1: Escrever o teste falhando — `tests/test_logger.py`**

```python
import app.logger as logger
from app.config import NICHE_BY_KEY

CARROS = NICHE_BY_KEY["carros"]


def test_group_by_table_separa_por_tabela():
    recs = [
        {"log_table": "logs", "message": "a"},
        {"log_table": "logs_carros", "message": "b"},
        {"log_table": "logs", "message": "c"},
    ]
    groups = logger._group_by_table(recs)
    assert set(groups.keys()) == {"logs", "logs_carros"}
    assert len(groups["logs"]) == 2
    assert len(groups["logs_carros"]) == 1


def test_oplogger_enfileira_na_tabela_do_nicho(monkeypatch):
    captured = []
    monkeypatch.setattr(logger._db_queue, "put_nowait", lambda rec: captured.append(rec))
    log = logger.OpLogger("affiliate", CARROS)
    log.info("create_link", "ok")
    assert captured[0]["log_table"] == "logs_carros"
    assert captured[0]["module"] == "affiliate"
    assert captured[0]["level"] == "INFO"


def test_oplogger_default_e_geral(monkeypatch):
    captured = []
    monkeypatch.setattr(logger._db_queue, "put_nowait", lambda rec: captured.append(rec))
    log = logger.OpLogger("system")
    log.info("startup", "ok")
    assert captured[0]["log_table"] == "logs"


def test_cleanup_old_logs_apaga_de_todas_as_tabelas(monkeypatch):
    deletes = []

    class _Cur:
        def execute(self, query, params=None):
            deletes.append(query)
        @property
        def rowcount(self):
            return 0
        def __enter__(self):
            return self
        def __exit__(self, *a):
            return False

    class _Conn:
        def cursor(self):
            return _Cur()
        def __enter__(self):
            return self
        def __exit__(self, *a):
            return False
        def close(self):
            pass

    monkeypatch.setattr(logger.psycopg2, "connect", lambda *a, **k: _Conn())
    logger.cleanup_old_logs()
    blob = " ".join(deletes)
    assert "logs_carros" in blob
    assert "logs" in blob
```

- [ ] **Step 2: Rodar — verificar falha**

Run: `python -m pytest tests/test_logger.py -q`
Expected: FAIL (`_group_by_table` não existe; record sem `log_table`).

- [ ] **Step 3: Editar `app/logger.py`**

Adicionar `from psycopg2 import sql` e `from app.config import DATABASE_URL, Niche, NICHE_BY_KEY` no topo (substituindo o import atual de `DATABASE_URL`).

Substituir a função `_flush_to_db` e adicionar `_group_by_table`:

```python
def _group_by_table(records: list[dict]) -> dict:
    groups = {}
    for r in records:
        groups.setdefault(r["log_table"], []).append(r)
    return groups


def _flush_to_db(records: list[dict]):
    conn = None
    try:
        conn = psycopg2.connect(DATABASE_URL)
        with conn:
            with conn.cursor() as cur:
                for table, recs in _group_by_table(records).items():
                    table_ident = sql.Identifier(table).as_string(conn)
                    psycopg2.extras.execute_values(
                        cur,
                        f"""
                        INSERT INTO {table_ident}
                            (request_id, level, module, stage, product_id,
                             message, extra, traceback, duration_ms)
                        VALUES %s
                        """,
                        [
                            (
                                r["request_id"], r["level"], r["module"], r["stage"],
                                r.get("product_id"), r["message"], r.get("extra"),
                                r.get("traceback"), r.get("duration_ms"),
                            )
                            for r in recs
                        ],
                        template="(%s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s)",
                    )
    except Exception as e:
        _console.error("[LOG-DB] Falha ao persistir %d logs: %s", len(records), e)
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass
```

> `sql.Identifier(table).as_string(conn)` faz o quoting correto usando a conexão real, evitando depender de `execute_values` aceitar objetos `Composed`.

Substituir `cleanup_old_logs` para varrer todas as tabelas de log dos nichos:

```python
def cleanup_old_logs():
    conn = None
    total = 0
    try:
        conn = psycopg2.connect(DATABASE_URL)
        with conn:
            with conn.cursor() as cur:
                for niche in NICHES:
                    cur.execute(
                        sql.SQL("DELETE FROM {} WHERE created_at < NOW() - INTERVAL '15 days'")
                           .format(sql.Identifier(niche.table_logs))
                    )
                    total += cur.rowcount
        _console.info("[LOG-DB] Limpeza: %d logs com +15 dias removidos", total)
        return total
    except Exception as e:
        _console.error("[LOG-DB] Falha na limpeza de logs: %s", e)
        return 0
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass
```

Adicionar `NICHES` ao import (`from app.config import DATABASE_URL, Niche, NICHES, NICHE_BY_KEY`).

Atualizar `OpLogger.__init__` e o `_emit` para carregar o nicho e o `log_table`:

```python
class OpLogger:
    def __init__(self, module: str, niche: Niche = None):
        self.module = module
        self.niche = niche or NICHE_BY_KEY["geral"]
        self.request_id = uuid.uuid4().hex[:8]
```

No final de `_emit`, trocar o bloco que monta o dict da fila por:

```python
        # ── Fila → DB ──────────────────────────────────────
        extra_json = json.dumps(extra, ensure_ascii=False, default=str) if extra else None

        try:
            _db_queue.put_nowait({
                "request_id": self.request_id,
                "level": level,
                "module": self.module,
                "stage": stage,
                "product_id": product_id,
                "message": msg,
                "extra": extra_json,
                "traceback": traceback_str,
                "duration_ms": duration_ms,
                "log_table": self.niche.table_logs,
            })
        except queue.Full:
            _console.warning("[LOG-DB] Queue cheia — log descartado")
```

- [ ] **Step 4: Rodar — verificar que passa**

Run: `python -m pytest tests/test_logger.py -q`
Expected: PASS (4 passed).

- [ ] **Step 5: Suíte completa e commit**

Run: `python -m pytest -q`
Expected: PASS (todos).

```bash
git add app/logger.py tests/test_logger.py
git commit -m "feat: logs por nicho (log_table na fila, flush e cleanup multi-tabela)"
```

---

## Task 5: `scraper.py` — `run_scraping(niche)` e contador por nicho

**Files:**
- Modify: `app/scraper.py`
- Create/Test: `tests/test_scraper.py`

- [ ] **Step 1: Escrever o teste falhando — `tests/test_scraper.py`**

```python
import app.scraper as scraper


def test_contador_vazio_isolado_por_nicho():
    scraper._consecutive_empty.clear()
    assert scraper._register_empty_round("geral") == 1
    assert scraper._register_empty_round("geral") == 2
    assert scraper._register_empty_round("carros") == 1  # isolado do geral


def test_register_success_zera_apenas_o_nicho():
    scraper._consecutive_empty.clear()
    scraper._register_empty_round("geral")
    scraper._register_empty_round("carros")
    scraper._register_success("geral")
    assert scraper._consecutive_empty["geral"] == 0
    assert scraper._consecutive_empty["carros"] == 1  # carros intacto
```

- [ ] **Step 2: Rodar — verificar falha**

Run: `python -m pytest tests/test_scraper.py -q`
Expected: FAIL (`_register_empty_round` não existe).

- [ ] **Step 3: Editar `app/scraper.py`**

Atualizar imports do database para incluir `Niche`:

```python
from app.config import Niche
from app.database import get_active_categories, upsert_products_batch
```

Substituir o contador global e adicionar os helpers:

```python
# ── Contador de rodadas vazias (por nicho) ─────────────────

_consecutive_empty: dict[str, int] = {}
_ALERT_AFTER = 3  # alerta após 3 rodadas sem produtos


def _register_empty_round(niche_key: str) -> int:
    _consecutive_empty[niche_key] = _consecutive_empty.get(niche_key, 0) + 1
    return _consecutive_empty[niche_key]


def _register_success(niche_key: str) -> None:
    _consecutive_empty[niche_key] = 0
```

Substituir `run_scraping` e `_check_alert`:

```python
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

    random.shuffle(all_products)

    with log.timed() as t_save:
        saved, errors = upsert_products_batch(all_products, niche)

    log.info("save", f"{saved} salvos, {errors} erros",
             duration_ms=t_save.ms, saved=saved, errors=errors)

    log.info("done", f"Scraping finalizado: {saved} produtos salvos",
             duration_ms=t_total.ms, total_products=saved, categories=len(categories))


def _check_alert(log: OpLogger, niche: Niche, message: str):
    """Envia alerta crítico via WhatsApp (campanha admin)."""
    log.error("alert", f"ALERTA CRÍTICO [{niche.key}]: {message}")
    send_alert(f"⚠️ ALERTA BOT PROMO [{niche.key.upper()}] ⚠️\n\n{message}\n\n"
               f"Verifique os logs em /logs?level=ERROR&niche={niche.key}")
```

- [ ] **Step 4: Rodar — verificar que passa**

Run: `python -m pytest tests/test_scraper.py -q`
Expected: PASS (2 passed).

- [ ] **Step 5: Suíte completa e commit**

Run: `python -m pytest -q`
Expected: PASS (todos).

```bash
git add app/scraper.py tests/test_scraper.py
git commit -m "feat: run_scraping(niche) com contador de rodadas vazias por nicho"
```

---

## Task 6: `affiliate.py` — jobs por nicho

**Files:**
- Modify: `app/affiliate.py`
- Create/Test: `tests/test_affiliate.py`

- [ ] **Step 1: Escrever o teste falhando — `tests/test_affiliate.py`**

```python
import app.affiliate as affiliate
from app.config import NICHE_BY_KEY

CARROS = NICHE_BY_KEY["carros"]


def test_process_products_passa_niche_ao_atualizar_link(monkeypatch):
    monkeypatch.setattr(affiliate.time, "sleep", lambda *_: None)
    monkeypatch.setattr(affiliate, "create_affiliate_link",
                        lambda url, log, product_id=None: "http://aff/x")
    captured = {}
    monkeypatch.setattr(affiliate, "update_affiliate_link",
                        lambda pid, link, niche: captured.update(args=(pid, link, niche)))

    log = affiliate.OpLogger("affiliate", CARROS)
    affiliate._process_products(
        [{"id_produto": "MLB1", "Link_Compra": "http://ml/x"}], log, CARROS)

    assert captured["args"][0] == "MLB1"
    assert captured["args"][2] is CARROS


def test_process_products_falha_consulta_failures_com_niche(monkeypatch):
    monkeypatch.setattr(affiliate.time, "sleep", lambda *_: None)
    monkeypatch.setattr(affiliate, "create_affiliate_link",
                        lambda url, log, product_id=None: None)  # falhou
    seen = {}
    monkeypatch.setattr(affiliate, "count_affiliate_failures",
                        lambda pid, niche: seen.update(niche=niche) or 1)
    monkeypatch.setattr(affiliate, "send_alert", lambda msg: None)

    log = affiliate.OpLogger("affiliate", CARROS)
    affiliate._process_products(
        [{"id_produto": "MLB1", "Link_Compra": "http://ml/x"}], log, CARROS)

    assert seen["niche"] is CARROS
```

- [ ] **Step 2: Rodar — verificar falha**

Run: `python -m pytest tests/test_affiliate.py -q`
Expected: FAIL (`_process_products` ainda não aceita `niche`).

- [ ] **Step 3: Editar `app/affiliate.py`**

Atualizar imports:

```python
from app.config import ML_COOKIES, ML_CSRF_TOKEN, ML_AFFILIATE_TAG, Niche
```

Substituir `_process_products`, `run_affiliate_generation` e `run_retry_null_links` (a função `create_affiliate_link` permanece inalterada):

```python
def _process_products(products: list[dict], log: OpLogger, niche: Niche):
    total = len(products)
    success = 0
    failed = 0

    for i, product in enumerate(products, 1):
        pid = product["id_produto"]
        link_compra = product.get("Link_Compra", "")

        if not link_compra:
            log.warning("skip", "Link_Compra vazio", product_id=pid)
            continue

        log.info("processing", f"({i}/{total}) Gerando link", product_id=pid)

        affiliate_link = create_affiliate_link(link_compra, log, product_id=pid)

        if affiliate_link:
            try:
                update_affiliate_link(pid, affiliate_link, niche)
                log.info("update_db", "Status → PRONTO", product_id=pid,
                         affiliate_url=affiliate_link)
                success += 1
            except Exception as e:
                log.error("update_db", f"Falha ao atualizar banco: {e}",
                          product_id=pid, exc=e)
                failed += 1
        else:
            failed += 1
            try:
                failures = count_affiliate_failures(pid, niche)
                if failures >= 3:
                    mark_as_failed(pid, niche)
                    log.warning("permanent_fail",
                                f"Produto marcado como FALHA após {failures} tentativas",
                                product_id=pid)
            except Exception as e:
                log.error("check_failures", f"Erro ao verificar falhas: {e}",
                          product_id=pid, exc=e)

        time.sleep(3)

    log.info("done", f"Concluído: {success} OK, {failed} falhas de {total}",
             total=total, success=success, failed=failed)

    # Se todos falharam, provavelmente cookies expiraram
    if total > 0 and success == 0:
        log.error("alert", "Todos os links falharam — cookies podem ter expirado")
        send_alert(
            f"⚠️ ALERTA BOT PROMO [{niche.key.upper()}] ⚠️\n\n"
            f"Geração de links falhou em todos os {total} produtos.\n"
            "Os cookies do Mercado Livre provavelmente expiraram.\n\n"
            "Atualize ML_COOKIES e ML_CSRF_TOKEN no EasyPanel."
        )


def run_affiliate_generation(niche: Niche):
    log = OpLogger("affiliate", niche)
    log.info("start", f"Iniciando geração de links de afiliado [{niche.key}]")

    try:
        products = get_pending_products(niche)
    except Exception as e:
        log.error("fetch", "Falha ao buscar produtos pendentes", exc=e)
        return

    if not products:
        log.info("fetch", "Nenhum produto pendente")
        return

    log.info("fetch", f"{len(products)} produtos pendentes", count=len(products))
    _process_products(products, log, niche)


def run_retry_null_links(niche: Niche):
    log = OpLogger("retry", niche)
    log.info("start", f"Retentando links nulos [{niche.key}]")

    try:
        products = get_ready_with_null_links(niche)
    except Exception as e:
        log.error("fetch", "Falha ao buscar produtos com link nulo", exc=e)
        return

    if not products:
        log.info("fetch", "Nenhum link nulo para retentar")
        return

    log.info("fetch", f"{len(products)} produtos com link nulo", count=len(products))
    _process_products(products, log, niche)
```

- [ ] **Step 4: Rodar — verificar que passa**

Run: `python -m pytest tests/test_affiliate.py -q`
Expected: PASS (2 passed).

- [ ] **Step 5: Suíte completa e commit**

Run: `python -m pytest -q`
Expected: PASS (todos).

```bash
git add app/affiliate.py tests/test_affiliate.py
git commit -m "feat: geração e retry de links de afiliado por nicho"
```

---

## Task 7: `whatsapp.py` — envio por nicho (release da campanha)

**Files:**
- Modify: `app/whatsapp.py`
- Create/Test: `tests/test_whatsapp.py`

- [ ] **Step 1: Escrever o teste falhando — `tests/test_whatsapp.py`**

```python
import app.whatsapp as whatsapp
from app.config import NICHE_BY_KEY

CARROS = NICHE_BY_KEY["carros"]


class _FakeResp:
    status_code = 200
    text = "ok"


def test_send_text_message_usa_release_id_recebido(monkeypatch):
    captured = {}

    def fake_post(url, headers=None, json=None, timeout=None):
        captured["json"] = json
        return _FakeResp()

    monkeypatch.setattr(whatsapp.requests, "post", fake_post)
    log = whatsapp.OpLogger("whatsapp", CARROS)

    ok = whatsapp.send_text_message("oi", log, "REL_CARROS", product_id="MLB1")

    assert ok is True
    assert captured["json"]["releaseId"] == "REL_CARROS"


def test_format_message_inalterado():
    msg = whatsapp.format_message({
        "Nomes_Produtos": "Pneu X",
        "Preco": "R$500,00\nR$300,00\n40% OFF",
        "Link_de_afiliado": "http://aff/y",
    })
    assert "Pneu X" in msg
    assert "De: R$500,00 Por: R$300,00 (40% OFF)" in msg
    assert "http://aff/y" in msg
```

- [ ] **Step 2: Rodar — verificar falha**

Run: `python -m pytest tests/test_whatsapp.py -q`
Expected: FAIL (`send_text_message` ainda exige só 2 args / usa `SENDFLOW_RELEASE_ID` fixo).

- [ ] **Step 3: Editar `app/whatsapp.py`**

Atualizar imports para incluir `Niche` (a constante `SENDFLOW_RELEASE_ID` deixa de ser usada por `send_text_message`, mas `SENDFLOW_ALERT_RELEASE_ID` permanece para `send_alert`):

```python
from app.config import (
    SENDFLOW_TOKEN, SENDFLOW_ACCOUNT_ID, SENDFLOW_ALERT_RELEASE_ID, Niche,
)
```

Substituir `send_text_message` (recebe `release_id`):

```python
def send_text_message(message: str, log: OpLogger, release_id: str,
                      product_id: str = None) -> bool:
    headers = {
        "Authorization": f"Bearer {SENDFLOW_TOKEN}",
        "Content-Type": "application/json",
    }
    payload = {
        "linkPreview": True,
        "messageText": message,
        "releaseId": release_id,
        "accountId": SENDFLOW_ACCOUNT_ID,
    }

    with log.timed() as t:
        try:
            resp = requests.post(SENDFLOW_URL, headers=headers,
                                 json=payload, timeout=30)
        except requests.RequestException as e:
            log.error("send", f"Request SendFlow falhou: {e}",
                      product_id=product_id, exc=e)
            return False

    if resp.status_code >= 400:
        log.error("send", f"SendFlow → {resp.status_code}: {resp.text[:200]}",
                  product_id=product_id, duration_ms=t.ms,
                  status=resp.status_code)
        return False

    log.info("send", f"SendFlow → {resp.status_code} OK",
             product_id=product_id, duration_ms=t.ms,
             status=resp.status_code)
    return True
```

Substituir `run_send_whatsapp`:

```python
def run_send_whatsapp(niche: Niche):
    log = OpLogger("whatsapp", niche)
    log.info("start", f"Buscando próximo produto para enviar [{niche.key}]")

    try:
        product = get_next_product_to_send(niche)
    except Exception as e:
        log.error("fetch", "Falha ao buscar produto no banco", exc=e)
        return

    if not product:
        log.info("fetch", "Nenhum produto pronto para enviar")
        return

    pid = product["id_produto"]

    log.info("fetch", f"Produto selecionado: {product.get('Nomes_Produtos', '')[:60]}",
             product_id=pid,
             nome=product.get("Nomes_Produtos", ""),
             link_afiliado=product.get("Link_de_afiliado", ""))

    caption = format_message(product)
    log.info("format", f"Mensagem formatada ({len(caption)} chars)",
             product_id=pid, caption_length=len(caption))

    if not send_text_message(caption, log, niche.sendflow_release_id, product_id=pid):
        log.error("done", "Envio falhou — produto NÃO marcado como enviado", product_id=pid)
        return

    try:
        mark_as_sent(pid, niche)
        log.info("mark_sent", "Status → ENVIADO", product_id=pid)
    except Exception as e:
        log.error("mark_sent", f"Falha ao marcar como enviado: {e}", product_id=pid, exc=e)
        return

    log.info("done", "Produto enviado com sucesso", product_id=pid)
```

> `format_message` e `send_alert` permanecem inalterados.

- [ ] **Step 4: Rodar — verificar que passa**

Run: `python -m pytest tests/test_whatsapp.py -q`
Expected: PASS (2 passed).

- [ ] **Step 5: Suíte completa e commit**

Run: `python -m pytest -q`
Expected: PASS (todos).

```bash
git add app/whatsapp.py tests/test_whatsapp.py
git commit -m "feat: envio WhatsApp por nicho (release da campanha)"
```

---

## Task 8: `main.py` — scheduler genérico e endpoints com `?niche=`

**Files:**
- Modify: `app/main.py`
- Create/Test: `tests/test_main.py`

- [ ] **Step 1: Escrever o teste falhando — `tests/test_main.py`**

```python
import pytest
from fastapi import HTTPException

import app.main as main
from app.config import NICHES, NICHE_BY_KEY


def test_resolve_niches_sem_param_retorna_todos():
    assert main.resolve_niches(None) == NICHES


def test_resolve_niches_com_param_valido():
    assert main.resolve_niches("carros") == [NICHE_BY_KEY["carros"]]


def test_resolve_niches_invalido_levanta_400():
    with pytest.raises(HTTPException) as exc:
        main.resolve_niches("inexistente")
    assert exc.value.status_code == 400


def test_setup_jobs_registra_jobs_de_todos_os_nichos():
    from apscheduler.schedulers.background import BackgroundScheduler
    sched = BackgroundScheduler(timezone=main.TZ)
    main.setup_jobs(sched)
    ids = {j.id for j in sched.get_jobs()}

    # geral: 1 cron por job → sem sufixo numérico
    assert {"scraping_geral", "affiliate_geral", "retry_geral", "whatsapp_geral"} <= ids
    # carros: scraping/affiliate/retry únicos; whatsapp tem 3 faixas
    assert {"scraping_carros", "affiliate_carros", "retry_carros"} <= ids
    assert {"whatsapp_carros_0", "whatsapp_carros_1", "whatsapp_carros_2"} <= ids
    # limpeza global
    assert "cleanup" in ids
```

- [ ] **Step 2: Rodar — verificar falha**

Run: `python -m pytest tests/test_main.py -q`
Expected: FAIL (`resolve_niches` não existe; `setup_jobs` não aceita scheduler).

- [ ] **Step 3: Reescrever `app/main.py`**

```python
from contextlib import asynccontextmanager
from functools import partial

from fastapi import FastAPI, Query, HTTPException
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from app.config import TZ, PORT, NICHES, NICHE_BY_KEY
from app.scraper import run_scraping
from app.affiliate import run_affiliate_generation, run_retry_null_links
from app.whatsapp import run_send_whatsapp, send_alert
from app.database import cleanup_old_products, cleanup_null_links, query_logs
from app.logger import OpLogger, flush_logs, cleanup_old_logs

scheduler = BackgroundScheduler(timezone=TZ)

JOB_FUNCS = {
    "scraping":  run_scraping,
    "affiliate": run_affiliate_generation,
    "retry":     run_retry_null_links,
    "whatsapp":  run_send_whatsapp,
}


def run_cleanup():
    log = OpLogger("cleanup")  # logs administrativos → nicho geral
    log.info("start", "Iniciando limpeza programada")

    for niche in NICHES:
        try:
            prod_old = cleanup_old_products(niche)
            log.info("old_products", f"[{niche.key}] {prod_old} produtos +8 dias removidos",
                     deleted=prod_old)
        except Exception as e:
            log.error("old_products", f"[{niche.key}] Falha: {e}", exc=e)

        try:
            prod_null = cleanup_null_links(niche)
            log.info("null_links", f"[{niche.key}] {prod_null} PRONTO sem link removidos",
                     deleted=prod_null)
        except Exception as e:
            log.error("null_links", f"[{niche.key}] Falha: {e}", exc=e)

    try:
        logs_deleted = cleanup_old_logs()
        log.info("old_logs", f"{logs_deleted} logs com +15 dias removidos", deleted=logs_deleted)
    except Exception as e:
        log.error("old_logs", f"Falha: {e}", exc=e)

    log.info("done", "Limpeza concluída")


def setup_jobs(scheduler):
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

    # Limpeza global: meia-noite
    scheduler.add_job(
        run_cleanup,
        CronTrigger(hour=0, minute=0, timezone=TZ),
        id="cleanup",
        replace_existing=True,
    )


def resolve_niches(niche: str | None):
    """Sem param → todos os nichos. Param válido → [nicho]. Inválido → 400."""
    if niche is None:
        return NICHES
    if niche not in NICHE_BY_KEY:
        raise HTTPException(status_code=400,
                            detail=f"Nicho inválido: {niche}. Use: {list(NICHE_BY_KEY)}")
    return [NICHE_BY_KEY[niche]]


@asynccontextmanager
async def lifespan(app: FastAPI):
    setup_jobs(scheduler)
    scheduler.start()
    log = OpLogger("system")
    log.info("startup", f"Scheduler iniciado com {len(scheduler.get_jobs())} jobs")
    for job in scheduler.get_jobs():
        log.info("startup", f"Job [{job.id}] próxima execução: {job.next_run_time}",
                 job_id=job.id, next_run=str(job.next_run_time))
    yield
    flush_logs()
    scheduler.shutdown()


app = FastAPI(title="ML Afiliados Bot", lifespan=lifespan)


# ── Endpoints ───────────────────────────────────────────────

@app.get("/health")
def health():
    jobs = [
        {"id": j.id, "next_run": str(j.next_run_time)}
        for j in scheduler.get_jobs()
    ]
    return {"status": "ok", "jobs": jobs}


@app.get("/logs")
def get_logs(
    limit: int = Query(50, ge=1, le=500),
    level: str = Query(None, description="INFO, WARNING, ERROR"),
    module: str = Query(None, description="scraper, affiliate, retry, whatsapp, cleanup"),
    request_id: str = Query(None, description="ID da operação (8 chars)"),
    product_id: str = Query(None, description="ID do produto (ex: MLB1234567)"),
    niche: str = Query("geral", description="geral | carros"),
):
    """Consulta logs estruturados. Combina filtros com AND."""
    if niche not in NICHE_BY_KEY:
        raise HTTPException(status_code=400, detail=f"Nicho inválido: {niche}")
    return query_logs(
        limit=limit,
        level=level,
        module=module,
        request_id=request_id,
        product_id=product_id,
        niche=NICHE_BY_KEY[niche],
    )


@app.post("/trigger/scraping")
def trigger_scraping(niche: str = Query(None)):
    targets = resolve_niches(niche)
    for n in targets:
        scheduler.add_job(partial(run_scraping, n),
                          id=f"manual_scraping_{n.key}", replace_existing=True)
    return {"status": "triggered", "job": "scraping", "niches": [n.key for n in targets]}


@app.post("/trigger/affiliate")
def trigger_affiliate(niche: str = Query(None)):
    targets = resolve_niches(niche)
    for n in targets:
        scheduler.add_job(partial(run_affiliate_generation, n),
                          id=f"manual_affiliate_{n.key}", replace_existing=True)
    return {"status": "triggered", "job": "affiliate", "niches": [n.key for n in targets]}


@app.post("/trigger/whatsapp")
def trigger_whatsapp(niche: str = Query(None)):
    targets = resolve_niches(niche)
    for n in targets:
        scheduler.add_job(partial(run_send_whatsapp, n),
                          id=f"manual_whatsapp_{n.key}", replace_existing=True)
    return {"status": "triggered", "job": "whatsapp", "niches": [n.key for n in targets]}


@app.post("/trigger/alert")
def trigger_alert():
    """Envia um alerta de teste para a campanha admin."""
    send_alert("🔔 Teste de alerta — sistema de monitoramento funcionando.")
    return {"status": "sent", "job": "alert_test"}


@app.post("/trigger/full")
def trigger_full(niche: str = Query(None)):
    """Executa o ciclo completo (scraping → links) para o(s) nicho(s)."""
    targets = resolve_niches(niche)

    def full_cycle(selected):
        for n in selected:
            run_scraping(n)
            run_affiliate_generation(n)

    scheduler.add_job(partial(full_cycle, targets), id="manual_full", replace_existing=True)
    return {"status": "triggered", "job": "full_cycle", "niches": [n.key for n in targets]}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app.main:app", host="0.0.0.0", port=PORT, reload=False)
```

- [ ] **Step 4: Rodar — verificar que passa**

Run: `python -m pytest tests/test_main.py -q`
Expected: PASS (4 passed).

- [ ] **Step 5: Suíte completa e commit**

Run: `python -m pytest -q`
Expected: PASS (todos).

```bash
git add app/main.py tests/test_main.py
git commit -m "feat: scheduler genérico por nicho e endpoints com ?niche="
```

---

## Task 9: Finalização — tornar `niche` obrigatório, env e DDL

**Files:**
- Modify: `app/database.py`
- Modify: `.env.example`

- [ ] **Step 1: Remover o default temporário de `app/database.py`**

Remover a linha `_GERAL = NICHE_BY_KEY["geral"]  # default temporário ...` e trocar **todas** as assinaturas `niche: Niche = _GERAL` por `niche: Niche` nas funções: `get_active_categories`, `upsert_product`, `upsert_products_batch`, `update_affiliate_link`, `mark_as_sent`, `mark_as_failed`, `count_affiliate_failures`, `get_pending_products`, `get_ready_with_null_links`, `get_next_product_to_send`, `cleanup_old_products`, `cleanup_null_links`, `query_logs`.

Ajustar o import (não precisa mais de `NICHE_BY_KEY`):

```python
from app.config import DATABASE_URL, Niche
```

- [ ] **Step 2: Atualizar o teste de default de `database`**

Em `tests/test_database.py`, remover `test_default_e_o_nicho_geral` (o default deixou de existir) — a chamada `get_pending_products()` sem argumento agora é, intencionalmente, um erro.

- [ ] **Step 3: Rodar a suíte inteira — garante que nenhum caller ficou sem `niche`**

Run: `python -m pytest -q`
Expected: PASS (todos). Qualquer caller esquecido apareceria como `TypeError: missing 'niche'` nos testes de fluxo (scraper/affiliate/whatsapp/main).

- [ ] **Step 4: Atualizar `.env.example`**

Adicionar, logo após a linha `SENDFLOW_RELEASE_ID=...`:

```
# Campanha do grupo de carros
SENDFLOW_RELEASE_ID_CARROS=xxxxxxxxxxxxxxxxxxxx
```

- [ ] **Step 5: Commit**

```bash
git add app/database.py tests/test_database.py .env.example
git commit -m "refactor: tornar niche obrigatorio no database + env de carros"
```

- [ ] **Step 6: Executar o DDL no Supabase (manual, fora do código)**

No SQL editor do Supabase, rodar:

```sql
CREATE TABLE "categorias_carros" (LIKE "categorias" INCLUDING ALL);
CREATE TABLE "Produtos_carros"   (LIKE "Produtos"   INCLUDING ALL);
CREATE TABLE "logs_carros"       (LIKE "logs"       INCLUDING ALL);
```

Depois popular `categorias_carros` com as URLs de ofertas de carros. As tabelas do grupo geral permanecem intactas.

- [ ] **Step 7: Definir a variável de ambiente no deploy**

No `.env` local e no EasyPanel, definir `SENDFLOW_RELEASE_ID_CARROS` com o release da campanha do grupo de carros.

---

## Validação manual final (após deploy)

Conforme a Seção 12 do spec:

1. `GET /health` → lista os jobs dos dois nichos com `next_run` coerentes (carros: `whatsapp_carros_0/1/2`).
2. `POST /trigger/scraping?niche=carros` → popula `Produtos_carros` (Status PENDENTE).
3. `POST /trigger/affiliate?niche=carros` → links gerados (Status PRONTO).
4. `POST /trigger/whatsapp?niche=carros` → envio na **campanha de carros**.
5. `GET /logs?niche=carros` → logs gravados em `logs_carros`.
6. Confirmar que o grupo **geral** continua funcionando sem alteração.

---

## Self-Review (preenchido)

**1. Cobertura do spec:**
- §4 modelo de nicho → Task 2 ✓
- §5 env CARROS → Task 2 (config) + Task 9 (.env.example) ✓
- §6 database por nicho + Identifier → Task 3 ✓
- §7 logs por nicho (log_table, flush por tabela, cleanup multi-tabela) → Task 4 ✓
- §8 jobs por nicho (scraper/affiliate/whatsapp) + contador por nicho + release por nicho → Tasks 5,6,7 ✓
- §9 setup_jobs genérico + run_cleanup multi-nicho + endpoints ?niche= → Task 8 ✓
- §10 DDL Supabase → Task 9 Step 6 ✓
- §11 considerações (rate-limit/threadpool) → sem mudança de código (documentadas) ✓
- §12 validação → seção "Validação manual final" ✓

**2. Placeholders:** nenhum "TBD"/"TODO"; todo step tem código/comando concreto. (`xxxx` em `.env.example` é formato de exemplo intencional.)

**3. Consistência de tipos/assinaturas:** convenção `niche` por último em todo o `database.py`; `update_affiliate_link(id, link, niche)`, `mark_as_sent(id, niche)`, `count_affiliate_failures(id, niche)`, `_process_products(products, log, niche)`, `send_text_message(message, log, release_id, product_id=None)`, `run_*(niche)`, `setup_jobs(scheduler)`, `resolve_niches(niche)` — usados de forma idêntica entre tarefas e testes. IDs de job: sufixo `_i` só quando há >1 cron (testado em Task 8).
