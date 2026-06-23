from contextlib import contextmanager

import psycopg2
import psycopg2.extras
from psycopg2 import sql

from app.config import DATABASE_URL, Niche


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

def get_active_categories(niche: Niche) -> list[dict]:
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
            "Preco", "Link_Compra", "Status", "created_at", "social_proof"
        ) VALUES (
            %(nome)s, %(id_produto)s, %(imagem)s,
            %(preco)s, %(link)s, 'PENDENTE', NOW(), %(social)s
        )
        ON CONFLICT ("id_produto")
        DO UPDATE SET "Preco" = EXCLUDED."Preco",
                      "social_proof" = EXCLUDED."social_proof"
        WHERE {t}."Status" != 'ENVIADO'
          AND ({t}."Preco" != EXCLUDED."Preco"
               OR {t}."social_proof" IS NULL);
    """).format(t=t)


def upsert_product(conn, product: dict, niche: Niche) -> bool:
    """Insere ou atualiza um produto. Retorna True se afetou alguma row."""
    with conn.cursor() as cur:
        cur.execute(_upsert_sql(niche), {
            "nome": product["nome"],
            "id_produto": product["id_produto"],
            "imagem": product["imagem"],
            "preco": product["preco"],
            "link": product["link"],
            "social": product.get("social"),
        })
        return cur.rowcount > 0


def upsert_products_batch(products: list[dict], niche: Niche) -> tuple[int, int]:
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


def update_affiliate_link(id_produto: str, link: str, niche: Niche):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL('UPDATE {} SET "Link_de_afiliado" = %s, "Status" = \'PRONTO\' '
                        'WHERE "id_produto" = %s')
                   .format(sql.Identifier(niche.table_produtos)),
                (link, id_produto),
            )


def mark_as_sent(id_produto: str, niche: Niche):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL('UPDATE {} SET "Status" = \'ENVIADO\' WHERE "id_produto" = %s')
                   .format(sql.Identifier(niche.table_produtos)),
                (id_produto,),
            )


def mark_as_failed(id_produto: str, niche: Niche):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL('UPDATE {} SET "Status" = \'FALHA\' WHERE "id_produto" = %s')
                   .format(sql.Identifier(niche.table_produtos)),
                (id_produto,),
            )


def count_affiliate_failures(id_produto: str, niche: Niche) -> int:
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

def get_pending_products(niche: Niche) -> list[dict]:
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                sql.SQL('SELECT * FROM {} WHERE "Status" = \'PENDENTE\' '
                        'ORDER BY "created_at" DESC')
                   .format(sql.Identifier(niche.table_produtos))
            )
            return cur.fetchall()


def get_ready_with_null_links(niche: Niche) -> list[dict]:
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                sql.SQL('SELECT * FROM {} WHERE "Status" = \'PRONTO\' '
                        'AND "Link_de_afiliado" IS NULL')
                   .format(sql.Identifier(niche.table_produtos))
            )
            return cur.fetchall()


def get_ready_candidates(niche: Niche, limit: int = 20) -> list[dict]:
    """Os N produtos PRONTO mais recentes (com link), priorizando os que têm prova social."""
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                sql.SQL("""
                    SELECT * FROM {}
                    WHERE "Status" = 'PRONTO'
                      AND "Link_de_afiliado" IS NOT NULL
                      AND "Link_de_afiliado" != ''
                    ORDER BY ("social_proof" IS NOT NULL) DESC, "created_at" DESC
                    LIMIT %s
                """).format(sql.Identifier(niche.table_produtos)),
                (limit,),
            )
            return cur.fetchall()


def get_recent_sent_names(niche: Niche, limit: int = 500) -> list[str]:
    """Nomes dos produtos ENVIADOS mais recentes (para deduplicação no envio)."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL("""
                    SELECT "Nomes_Produtos" FROM {}
                    WHERE "Status" = 'ENVIADO'
                    ORDER BY "created_at" DESC
                    LIMIT %s
                """).format(sql.Identifier(niche.table_produtos)),
                (limit,),
            )
            return [row[0] for row in cur.fetchall() if row[0]]


# ── Limpeza ─────────────────────────────────────────────────

def cleanup_old_products(niche: Niche) -> int:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL("DELETE FROM {} WHERE \"created_at\" < NOW() - INTERVAL '8 days'")
                   .format(sql.Identifier(niche.table_produtos))
            )
            return cur.rowcount


def cleanup_null_links(niche: Niche) -> int:
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
    niche: Niche,
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

    query = sql.SQL("SELECT * FROM {} ").format(sql.Identifier(niche.table_logs))
    if clauses:
        query = query + sql.SQL("WHERE ") + sql.SQL(" AND ").join(
            sql.SQL(c) for c in clauses
        ) + sql.SQL(" ")
    query = query + sql.SQL("ORDER BY created_at DESC LIMIT %s")
    params.append(min(limit, 500))

    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(query, params)
            rows = cur.fetchall()
            for r in rows:
                r["created_at"] = r["created_at"].isoformat() if r.get("created_at") else None
            return rows
