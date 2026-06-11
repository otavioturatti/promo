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
