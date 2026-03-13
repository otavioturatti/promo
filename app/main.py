from contextlib import asynccontextmanager

from fastapi import FastAPI, Query
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from app.config import TZ, PORT
from app.scraper import run_scraping
from app.affiliate import run_affiliate_generation, run_retry_null_links
from app.whatsapp import run_send_whatsapp, send_alert
from app.database import cleanup_old_products, cleanup_null_links, query_logs
from app.logger import OpLogger, flush_logs, cleanup_old_logs

scheduler = BackgroundScheduler(timezone=TZ)


def run_cleanup():
    log = OpLogger("cleanup")
    log.info("start", "Iniciando limpeza programada")

    try:
        prod_old = cleanup_old_products()
        log.info("old_products", f"{prod_old} produtos com +8 dias removidos",
                 deleted=prod_old)
    except Exception as e:
        log.error("old_products", f"Falha: {e}", exc=e)

    try:
        prod_null = cleanup_null_links()
        log.info("null_links", f"{prod_null} produtos PRONTO sem link removidos",
                 deleted=prod_null)
    except Exception as e:
        log.error("null_links", f"Falha: {e}", exc=e)

    try:
        logs_deleted = cleanup_old_logs()
        log.info("old_logs", f"{logs_deleted} logs com +15 dias removidos",
                 deleted=logs_deleted)
    except Exception as e:
        log.error("old_logs", f"Falha: {e}", exc=e)

    log.info("done", "Limpeza concluída")


def setup_jobs():
    # Scraping: a cada hora das 06 às 22
    scheduler.add_job(
        run_scraping,
        CronTrigger(hour="6-22", minute=0, timezone=TZ),
        id="scraping",
        replace_existing=True,
    )

    # Links de afiliado: a cada hora + 5 min das 06 às 22
    scheduler.add_job(
        run_affiliate_generation,
        CronTrigger(hour="6-22", minute=5, timezone=TZ),
        id="affiliate",
        replace_existing=True,
    )

    # Retry links nulos: a cada 4 horas
    scheduler.add_job(
        run_retry_null_links,
        CronTrigger(hour="*/4", timezone=TZ),
        id="retry_links",
        replace_existing=True,
    )

    # Envio WhatsApp: a cada 7 min das 6h às 22h
    scheduler.add_job(
        run_send_whatsapp,
        CronTrigger(minute="*/7", hour="6-22", timezone=TZ),
        id="whatsapp",
        replace_existing=True,
    )

    # Limpeza: meia-noite
    scheduler.add_job(
        run_cleanup,
        CronTrigger(hour=0, minute=0, timezone=TZ),
        id="cleanup",
        replace_existing=True,
    )


@asynccontextmanager
async def lifespan(app: FastAPI):
    setup_jobs()
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
):
    """Consulta logs estruturados. Combina filtros com AND."""
    return query_logs(
        limit=limit,
        level=level,
        module=module,
        request_id=request_id,
        product_id=product_id,
    )


@app.post("/trigger/scraping")
def trigger_scraping():
    scheduler.add_job(run_scraping, id="manual_scraping", replace_existing=True)
    return {"status": "triggered", "job": "scraping"}


@app.post("/trigger/affiliate")
def trigger_affiliate():
    scheduler.add_job(run_affiliate_generation, id="manual_affiliate", replace_existing=True)
    return {"status": "triggered", "job": "affiliate"}


@app.post("/trigger/whatsapp")
def trigger_whatsapp():
    scheduler.add_job(run_send_whatsapp, id="manual_whatsapp", replace_existing=True)
    return {"status": "triggered", "job": "whatsapp"}


@app.post("/trigger/alert")
def trigger_alert():
    """Envia um alerta de teste para a campanha admin."""
    send_alert("🔔 Teste de alerta — sistema de monitoramento funcionando.")
    return {"status": "sent", "job": "alert_test"}


@app.post("/trigger/full")
def trigger_full():
    """Executa o ciclo completo: scraping → links."""
    def full_cycle():
        run_scraping()
        run_affiliate_generation()

    scheduler.add_job(full_cycle, id="manual_full", replace_existing=True)
    return {"status": "triggered", "job": "full_cycle"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app.main:app", host="0.0.0.0", port=PORT, reload=False)
