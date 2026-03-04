from contextlib import contextmanager

import psycopg2
import psycopg2.extras

from app.config import DATABASE_URL


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

def get_active_categories() -> list[dict]:
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute('SELECT * FROM "categorias" WHERE "status" = TRUE')
            return cur.fetchall()


# ── Produtos — Escrita ──────────────────────────────────────

_UPSERT_SQL = """
INSERT INTO "Produtos" (
    "Nomes_Produtos", "id_produto", "Imagem_Produtos",
    "Preco", "Link_Compra", "Status", "created_at"
) VALUES (
    %(nome)s, %(id_produto)s, %(imagem)s,
    %(preco)s, %(link)s, 'PENDENTE', NOW()
)
ON CONFLICT ("id_produto")
DO UPDATE SET "Preco" = EXCLUDED."Preco"
WHERE "Produtos"."Status" != 'ENVIADO'
  AND "Produtos"."Preco" != EXCLUDED."Preco";
"""


def upsert_product(conn, product: dict) -> bool:
    """Insere ou atualiza um produto. Retorna True se afetou alguma row."""
    with conn.cursor() as cur:
        cur.execute(_UPSERT_SQL, {
            "nome": product["nome"],
            "id_produto": product["id_produto"],
            "imagem": product["imagem"],
            "preco": product["preco"],
            "link": product["link"],
        })
        return cur.rowcount > 0


def upsert_products_batch(products: list[dict]) -> tuple[int, int]:
    """Insere batch. Retorna (salvos, erros)."""
    saved = 0
    errors = 0
    with get_conn() as conn:
        for p in products:
            try:
                if upsert_product(conn, p):
                    saved += 1
            except Exception:
                errors += 1
                conn.rollback()
    return saved, errors


def update_affiliate_link(id_produto: str, link: str):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE "Produtos"
                SET "Link_de_afiliado" = %s, "Status" = 'PRONTO'
                WHERE "id_produto" = %s
            """, (link, id_produto))


def mark_as_sent(id_produto: str):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE "Produtos"
                SET "Status" = 'ENVIADO'
                WHERE "id_produto" = %s
            """, (id_produto,))


# ── Produtos — Leitura ──────────────────────────────────────

def get_pending_products() -> list[dict]:
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT * FROM "Produtos"
                WHERE "Status" = 'PENDENTE'
                ORDER BY "created_at" DESC
            """)
            return cur.fetchall()


def get_ready_with_null_links() -> list[dict]:
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT * FROM "Produtos"
                WHERE "Status" = 'PRONTO'
                  AND "Link_de_afiliado" IS NULL
            """)
            return cur.fetchall()


def get_next_product_to_send() -> dict | None:
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            # Sorteia 1 entre os 20 mais recentes prontos
            cur.execute("""
                SELECT * FROM (
                    SELECT * FROM "Produtos"
                    WHERE "Status" = 'PRONTO'
                      AND "Link_de_afiliado" IS NOT NULL
                      AND "Link_de_afiliado" != ''
                    ORDER BY "created_at" DESC
                    LIMIT 20
                ) recentes
                ORDER BY RANDOM()
                LIMIT 1
            """)
            return cur.fetchone()


# ── Limpeza ─────────────────────────────────────────────────

def cleanup_old_products() -> int:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                DELETE FROM "Produtos"
                WHERE "created_at" < NOW() - INTERVAL '8 days'
            """)
            return cur.rowcount


def cleanup_null_links() -> int:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                DELETE FROM "Produtos"
                WHERE "Status" = 'PRONTO'
                  AND "Link_de_afiliado" IS NULL
            """)
            return cur.rowcount


# ── Logs — Leitura (endpoint /logs) ─────────────────────────

def query_logs(
    limit: int = 50,
    level: str = None,
    module: str = None,
    request_id: str = None,
    product_id: str = None,
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

    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    params.append(min(limit, 500))

    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(f"""
                SELECT * FROM logs
                {where}
                ORDER BY created_at DESC
                LIMIT %s
            """, params)
            rows = cur.fetchall()
            for r in rows:
                r["created_at"] = r["created_at"].isoformat() if r.get("created_at") else None
            return rows
