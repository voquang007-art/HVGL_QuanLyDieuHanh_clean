"""
app/database.py
Cập nhật: 2025-09-13
Mục tiêu: Bổ sung Base/engine/SessionLocal chuẩn để các router có thể import `from app.database import Base`
mà không lỗi. Giữ nguyên get_db. Không thay đổi route/UX/migration.

Nguyên tắc:
- Dùng DATABASE_URL từ app.config.settings (KHÔNG dùng sqlite tạm).
- Hỗ trợ SQLite (check_same_thread=False) khi URL là sqlite.
"""

from typing import Generator
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
import os

from .config import settings

# -----------------------------
# Engine: ưu tiên DB nội bộ của dự án hiện tại
# -----------------------------
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
LOCAL_DB_PATH = os.path.join(PROJECT_ROOT, "instance", "qlcv.sqlite3")
LOCAL_DB_PATH = os.path.abspath(LOCAL_DB_PATH)

# Chuẩn hóa URL sqlite theo path thực tế của dự án mới
DATABASE_URL = f"sqlite:///{LOCAL_DB_PATH.replace(os.sep, '/')}"

connect_args = {}
if DATABASE_URL.startswith("sqlite:"):
    try:
        folder = os.path.dirname(LOCAL_DB_PATH)
        if folder and not os.path.exists(folder):
            os.makedirs(folder, exist_ok=True)
    except Exception:
        pass
    connect_args = {"check_same_thread": False}

engine = create_engine(DATABASE_URL, connect_args=connect_args)

# -----------------------------
# SessionLocal & Base export
# -----------------------------
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()  # <- QUAN TRỌNG: để models.py import không lỗi

# -----------------------------
# get_db: dùng chung cho Depends
# -----------------------------
def get_db() -> Generator:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
