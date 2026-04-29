# ỨNG DỤNG QUẢN LÝ, ĐIỀU HÀNH CÔNG VIỆC

Cổng chạy mặc định: **5002** (để không xung đột với bản cũ đang chạy 5000).  
Triển khai theo Python **3.11.9**, FastAPI, SQLAlchemy 2.x, Jinja2.

## 1) Cài đặt nhanh (Windows)

```bat
cd C:\QLCV_App
py -3.11 -m venv .venv
.venv\Scripts\pip install --upgrade pip
.venv\Scripts\pip install -r requirements.txt
copy .env.example .env
.venv\Scripts\python scripts\seed_admin.py
.venv\Scripts\uvicorn app.main:app --host 127.0.0.1 --port 5002 --reload
