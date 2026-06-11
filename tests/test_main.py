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

    assert {"scraping_geral", "affiliate_geral", "retry_geral", "whatsapp_geral"} <= ids
    assert {"scraping_carros", "affiliate_carros", "retry_carros"} <= ids
    assert {"whatsapp_carros_0", "whatsapp_carros_1", "whatsapp_carros_2"} <= ids
    assert "cleanup" in ids
