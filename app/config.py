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
    min_discount_pct: int = 30  # desconto mínimo p/ um produto entrar (descarta <= este valor)
    scrape_max_pages: int = 1   # páginas por categoria no scraping (&page=N); para cedo se vier vazia


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
        scrape_max_pages=3,
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
        min_discount_pct=20,
        scrape_max_pages=3,
    ),
]

NICHE_BY_KEY = {n.key: n for n in NICHES}
