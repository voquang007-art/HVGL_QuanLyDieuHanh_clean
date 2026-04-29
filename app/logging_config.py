# app/logging_config.py
import sys
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path

def setup_logging():
    """
    Cấu hình logging toàn cục:
      - In ra console
      - Ghi file logs/app.log (xoay vòng)
    """
    log_dir = Path("logs")
    log_dir.mkdir(parents=True, exist_ok=True)

    handlers = [
        logging.StreamHandler(sys.stdout),
        RotatingFileHandler(log_dir / "app.log", maxBytes=2_000_000, backupCount=3, encoding="utf-8"),
    ]

    logging.basicConfig(
        level=logging.INFO,  # đảm bảo logger.info hiển thị
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=handlers,
    )

    # (tuỳ chọn) giảm độ ồn SQLAlchemy nếu cần
    logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)

    # bật cụ thể logger module tasks
    logging.getLogger("app.routers.tasks").setLevel(logging.INFO)
