import os
from dotenv import load_dotenv

load_dotenv()


DATABASE_URL = os.getenv("DATABASE_URL", "")

ML_COOKIES = os.getenv("ML_COOKIES", "")
ML_CSRF_TOKEN = os.getenv("ML_CSRF_TOKEN", "")
ML_AFFILIATE_TAG = os.getenv("ML_AFFILIATE_TAG", "alessandrejr")

SENDFLOW_TOKEN = os.getenv("SENDFLOW_TOKEN", "")
SENDFLOW_ACCOUNT_ID = os.getenv("SENDFLOW_ACCOUNT_ID", "")
SENDFLOW_RELEASE_ID = os.getenv("SENDFLOW_RELEASE_ID", "")

TZ = os.getenv("TZ", "America/Sao_Paulo")
PORT = int(os.getenv("PORT", "8000"))
