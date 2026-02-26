"""
Sistema de logs estruturados com persistência em PostgreSQL.

Cada operação (scraping, affiliate, whatsapp, cleanup) recebe um request_id
único de 8 caracteres. Todos os logs dessa operação compartilham o mesmo
request_id, permitindo rastrear o ciclo completo de uma execução.

Formato console:
  2026-02-25 18:00:01 INFO  [a1b2c3d4] scraper.http | GET ofertas → 200 (342ms)

Formato banco (tabela logs):
  request_id | level | module | stage | product_id | message | extra | traceback | duration_ms
"""

import json
import time
import queue
import logging
import threading
import traceback as tb_module
import uuid
from contextlib import contextmanager

import psycopg2
import psycopg2.extras

from app.config import DATABASE_URL

# ── Console logger ──────────────────────────────────────────

_console = logging.getLogger("app")
_console.setLevel(logging.INFO)
_console.propagate = False

if not _console.handlers:
    _handler = logging.StreamHandler()
    _handler.setFormatter(logging.Formatter(
        "%(asctime)s %(levelname)-5s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    ))
    _console.addHandler(_handler)


# ── Fila e worker de persistência ───────────────────────────

_db_queue: queue.Queue = queue.Queue(maxsize=10_000)
_FLUSH_INTERVAL = 5
_BATCH_SIZE = 100


def _flush_to_db(records: list[dict]):
    conn = None
    try:
        conn = psycopg2.connect(DATABASE_URL)
        with conn:
            with conn.cursor() as cur:
                psycopg2.extras.execute_values(
                    cur,
                    """
                    INSERT INTO logs
                        (request_id, level, module, stage, product_id,
                         message, extra, traceback, duration_ms)
                    VALUES %s
                    """,
                    [
                        (
                            r["request_id"],
                            r["level"],
                            r["module"],
                            r["stage"],
                            r.get("product_id"),
                            r["message"],
                            r.get("extra"),
                            r.get("traceback"),
                            r.get("duration_ms"),
                        )
                        for r in records
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


def _db_worker():
    while True:
        batch = []
        try:
            batch.append(_db_queue.get(timeout=_FLUSH_INTERVAL))
            while len(batch) < _BATCH_SIZE:
                try:
                    batch.append(_db_queue.get_nowait())
                except queue.Empty:
                    break
        except queue.Empty:
            pass

        if batch:
            _flush_to_db(batch)


_worker = threading.Thread(target=_db_worker, daemon=True, name="log-db-worker")
_worker.start()


# ── Flush manual (chamado no shutdown) ──────────────────────

def flush_logs():
    batch = []
    while True:
        try:
            batch.append(_db_queue.get_nowait())
        except queue.Empty:
            break
    if batch:
        _flush_to_db(batch)


# ── Limpeza de logs antigos ─────────────────────────────────

def cleanup_old_logs():
    conn = None
    try:
        conn = psycopg2.connect(DATABASE_URL)
        with conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM logs WHERE created_at < NOW() - INTERVAL '15 days'")
                deleted = cur.rowcount
        _console.info("[LOG-DB] Limpeza: %d logs com +15 dias removidos", deleted)
        return deleted
    except Exception as e:
        _console.error("[LOG-DB] Falha na limpeza de logs: %s", e)
        return 0
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


# ── OpLogger — logger de operação ───────────────────────────

class OpLogger:
    """
    Logger contextual para uma operação.

    Uso:
        log = OpLogger("scraper")
        log.info("start", "Iniciando scraping")
        log.info("http", "GET ofertas → 200", duration_ms=342, url="...")
        log.error("http", "Timeout", exc=e, url="...")
    """

    def __init__(self, module: str):
        self.module = module
        self.request_id = uuid.uuid4().hex[:8]

    def info(self, stage: str, msg: str, *, product_id: str = None,
             duration_ms: int = None, **extra):
        self._emit("INFO", stage, msg, product_id, duration_ms, extra)

    def warning(self, stage: str, msg: str, *, product_id: str = None,
                duration_ms: int = None, **extra):
        self._emit("WARNING", stage, msg, product_id, duration_ms, extra)

    def error(self, stage: str, msg: str, *, product_id: str = None,
              duration_ms: int = None, exc: Exception = None, **extra):
        tb = "".join(tb_module.format_exception(exc)) if exc else None
        self._emit("ERROR", stage, msg, product_id, duration_ms, extra, tb)

    @contextmanager
    def timed(self):
        """Context manager que mede duração em ms."""
        start = time.perf_counter()

        class Timer:
            ms = 0

        try:
            yield Timer
        finally:
            Timer.ms = int((time.perf_counter() - start) * 1000)

    # ── interno ─────────────────────────────────────────────

    def _emit(self, level: str, stage: str, msg: str,
              product_id: str = None, duration_ms: int = None,
              extra: dict = None, traceback_str: str = None):

        # ── Console ─────────────────────────────────────────
        line = f"[{self.request_id}] {self.module}.{stage}"
        if product_id:
            line += f" [{product_id}]"
        line += f" | {msg}"
        if duration_ms is not None:
            line += f" ({duration_ms}ms)"

        getattr(_console, level.lower())(line)

        if traceback_str:
            _console.error(traceback_str.rstrip())

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
            })
        except queue.Full:
            _console.warning("[LOG-DB] Queue cheia — log descartado")
