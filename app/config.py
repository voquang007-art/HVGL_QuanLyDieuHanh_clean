from pydantic import BaseModel
from dotenv import load_dotenv
import os

load_dotenv()

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DEFAULT_DB_PATH = os.path.join(PROJECT_ROOT, "instance", "qlcv.sqlite3")
DEFAULT_UPLOAD_DIR = os.path.join(PROJECT_ROOT, "instance", "uploads")

DEFAULT_DB_URL = f"sqlite:///{DEFAULT_DB_PATH.replace(os.sep, '/')}"


def _is_legacy_workspace_path(value: str | None) -> bool:
    normalized = str(value or "").replace("\\", "/").lower()
    return "hvgl_workspace" in normalized


def _env_or_default(name: str, default_value: str) -> str:
    value = os.getenv(name)
    if not value:
        return default_value
    if _is_legacy_workspace_path(value):
        return default_value
    return value


class Settings(BaseModel):
    APP_NAME: str = os.getenv("APP_NAME", "Ứng dụng quản lý, điều hành")
    COMPANY_NAME: str = os.getenv("COMPANY_NAME", "BỆNH VIỆN HÙNG VƯƠNG GIA LAI")
    SECRET_KEY: str = os.getenv("SECRET_KEY", "dev-secret-key-change-me")

    DATABASE_URL: str = _env_or_default("DATABASE_URL", DEFAULT_DB_URL)
    UPLOAD_DIR: str = _env_or_default("UPLOAD_DIR", DEFAULT_UPLOAD_DIR)

    MAX_FILE_SIZE_MB: int = int(os.getenv("MAX_FILE_SIZE_MB", "50"))
    PORT: int = int(os.getenv("PORT", "5002"))

    SECRET_CODE_MANAGER: str = os.getenv("SECRET_CODE_MANAGER", "QL-2025")
    SECRET_CODE_COUNCIL: str = os.getenv("SECRET_CODE_COUNCIL", "HĐTV-2025")


settings = Settings()
