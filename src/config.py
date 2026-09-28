import os
from pathlib import Path
from dotenv import load_dotenv

# =========================
# PROJECT PATHS
# =========================
BASE_DIR = Path(__file__).resolve().parent.parent

DATA_DIR = BASE_DIR / "data"
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"
LOG_DIR = BASE_DIR / "logs"

# Tự động tạo các thư mục cần thiết nếu chưa có
for directory in [RAW_DIR, PROCESSED_DIR, LOG_DIR]:
    directory.mkdir(parents=True, exist_ok=True)

# =========================
# ENVIRONMENT
# =========================
load_dotenv(BASE_DIR / ".env")

# =========================
# LOGGING CONFIG
# =========================
LOG_LEVEL_CONSOLE = os.getenv("LOG_LEVEL_CONSOLE", os.getenv("LOG_LEVEL", "INFO")).upper()
LOG_LEVEL_FILE = os.getenv("LOG_LEVEL_FILE", "DEBUG").upper()

# =========================
# DATABASE (SQL SERVER)
# =========================
DB_DRIVER = os.getenv("DB_DRIVER", "ODBC Driver 18 for SQL Server")
DB_SERVER = os.getenv("DB_SERVER", "localhost")
DB_PORT = os.getenv("DB_PORT", "1433")
DB_NAME = os.getenv("DB_NAME", "dw_olist")
DB_TRUSTED_CONNECTION = os.getenv("DB_TRUSTED_CONNECTION", "yes").lower() in ["yes", "true", "1"]
DB_TRUST_SERVER_CERTIFICATE = os.getenv("DB_TRUST_SERVER_CERTIFICATE", "yes")

DB_USER = os.getenv("DB_USER", "")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")


auth_part = (
    "Trusted_Connection=yes;"
    if DB_TRUSTED_CONNECTION
    else f"UID={DB_USER};PWD={DB_PASSWORD};"
)

DB_CONNECTION_STRING = (
    f"DRIVER={{{DB_DRIVER}}};"
    f"SERVER={DB_SERVER},{DB_PORT};"
    f"DATABASE={DB_NAME};"
    f"{auth_part}"
    f"TrustServerCertificate={DB_TRUST_SERVER_CERTIFICATE};"
)