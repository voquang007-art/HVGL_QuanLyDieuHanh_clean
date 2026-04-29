# -*- coding: utf-8 -*-
"""
app/main.py
Mục tiêu: bổ sung helper user_unit_names và đăng ký vào Jinja; đồng thời gắn helper này
vào mọi Jinja2Templates của các router (như account) để tránh lỗi 'undefined'.
Không thay đổi cấu trúc router/URL, không tạo route mới.
"""

import os
from datetime import datetime
from zoneinfo import ZoneInfo  # giữ như hiện trạng

from fastapi import FastAPI, HTTPException, Request
from starlette.middleware.sessions import SessionMiddleware
from starlette.staticfiles import StaticFiles
from starlette.templating import Jinja2Templates
from starlette.responses import RedirectResponse, FileResponse

from .config import settings
from .bootstrap import initialize_system
from .work_access import get_work_access_from_session

# ===== Khởi tạo app =====
app = FastAPI(title=getattr(settings, "APP_NAME", "QLCV_App"))

# Session middleware
session_secret_key = getattr(settings, "SESSION_SECRET", None) or getattr(settings, "SECRET_KEY", None)
if not session_secret_key or str(session_secret_key).strip() in {"secret", "dev-secret-key-change-me"}:
    raise RuntimeError(
        "SESSION/SECRET_KEY chưa được cấu hình an toàn. "
        "Vui lòng đặt SECRET_KEY trong file .env trước khi chạy ứng dụng."
    )

app.add_middleware(SessionMiddleware, secret_key=str(session_secret_key).strip())

# Static
static_dir = os.path.join(os.path.dirname(__file__), "static")
if os.path.isdir(static_dir):
    app.mount("/static", StaticFiles(directory=static_dir), name="static")

# Templates (mặc định của main)
templates = Jinja2Templates(directory=os.path.join(os.path.dirname(__file__), "templates"))
app.state.templates = templates

# ===== Jinja filter: định dạng thời gian VN =====
def format_vn_dt(dt: datetime) -> str:
    if not isinstance(dt, datetime):
        return ""
    try:
        import datetime as _dt
        vn_tz = _dt.timezone(_dt.timedelta(hours=7))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=_dt.timezone.utc)
        tzdt = dt.astimezone(vn_tz)
    except Exception:
        tzdt = dt
    return f"{tzdt:%d-%m-%Y}-{tzdt:%H}-{tzdt:%M}"

templates.env.filters["format_vn_dt"] = format_vn_dt

# ===== Template globals hiện có (files/reports) =====
from sqlalchemy.orm import Session
from sqlalchemy import or_
from .database import SessionLocal
from .models import Files, Tasks

try:
    from .models import TaskReports  # type: ignore
    HAS_TASK_REPORTS = True
except Exception:
    TaskReports = None  # type: ignore
    HAS_TASK_REPORTS = False

def list_task_files(task_id: str):
    db: Session = SessionLocal()
    try:
        like_task = f"/TASK/{task_id}/"
        like_reports = f"/TASK/{task_id}/REPORTS/"
        q = (
            db.query(Files)
            .filter(or_(Files.path.like(f"%{like_task}%"), Files.path.like(f"%{like_reports}%")))
            .order_by(Files.uploaded_at.desc() if hasattr(Files, "uploaded_at") else Files.path.desc())
        )
        return q.all()
    finally:
        db.close()

def list_task_report_files(task_id: str):
    db: Session = SessionLocal()
    try:
        patterns = [f"/TASK/{task_id}/REPORTS/"]
        if HAS_TASK_REPORTS and TaskReports is not None:
            reps = db.query(TaskReports).filter(getattr(TaskReports, "task_id") == task_id).all()
            for r in reps:
                rid = getattr(r, "id", None)
                if rid:
                    patterns.append(f"/TASK_REPORT/{rid}/")
        conds = [Files.path.like(f"%{p}%") for p in patterns]
        q = db.query(Files).filter(or_(*conds)).order_by(Files.uploaded_at.desc() if hasattr(Files, "uploaded_at") else Files.path.desc())
        return q.all()
    finally:
        db.close()

def list_task_reports(task_id: str):
    if not HAS_TASK_REPORTS or TaskReports is None:
        return []
    db: Session = SessionLocal()
    try:
        q = db.query(TaskReports).filter(getattr(TaskReports, "task_id") == task_id)
        if hasattr(TaskReports, "reported_at"):
            q = q.order_by(getattr(TaskReports, "reported_at").desc())
        return q.all()
    finally:
        db.close()

templates.env.globals["list_task_files"] = list_task_files
templates.env.globals["list_task_report_files"] = list_task_report_files
templates.env.globals["list_task_reports"] = list_task_reports

# ===== BỔ SUNG: helper lấy tên đơn vị & đăng ký vào Jinja =====
from .models import Users, Units
try:
    from .models import UserUnitMemberships as _UserUnits  # nếu schema khác, fallback sẽ an toàn
except Exception:
    _UserUnits = None  # type: ignore

def user_unit_names(user_id):
    """Trả về danh sách tên đơn vị mà user thuộc; ưu tiên cột 'ten_don_vi' nếu có."""
    db: Session = SessionLocal()
    try:
        names = []
        if _UserUnits is not None and hasattr(_UserUnits, "unit_id"):
            q = (
                db.query(Units)
                .join(_UserUnits, Units.id == _UserUnits.unit_id)
                .filter(getattr(_UserUnits, "user_id") == user_id)
                .order_by(Units.id.asc())
            )
            for u in q.all():
                nm = getattr(u, "ten_don_vi", None) or getattr(u, "name", None) or f"Đơn vị #{getattr(u,'id', '')}"
                names.append(nm)
        else:
            # Trường hợp Users có cột unit_id trỏ trực tiếp
            u = db.get(Users, user_id)
            unit_id = getattr(u, "unit_id", None) if u else None
            if unit_id:
                dv = db.get(Units, unit_id)
                if dv:
                    nm = getattr(dv, "ten_don_vi", None) or getattr(dv, "name", None) or f"Đơn vị #{unit_id}"
                    names.append(nm)
        return names
    finally:
        db.close()

def pending_user_approval_count():
    """Đếm số tài khoản đang chờ phê duyệt để hiện badge cho admin/lãnh đạo."""
    db: Session = SessionLocal()
    try:
        from .models import Users, UserStatus
        return db.query(Users).filter(Users.status == UserStatus.PENDING_APPROVAL).count()
    except Exception:
        return 0
    finally:
        db.close()

def current_user_has_committee(session):
    """Trả về True nếu user hiện tại có quyền thấy ít nhất một Ban kiêm nhiệm."""
    session = session or {}
    user_id = session.get("user_id")
    if not user_id:
        return False

    db: Session = SessionLocal()
    try:
        from .committees.service import user_has_any_committee_access
        return bool(user_has_any_committee_access(db, str(user_id)))
    except Exception:
        return False
    finally:
        db.close()

# Đăng ký vào Jinja của main
templates.env.globals["user_unit_names"] = user_unit_names
templates.env.globals["pending_user_approval_count"] = pending_user_approval_count
templates.env.globals["work_access"] = get_work_access_from_session
templates.env.globals["current_user_has_committee"] = current_user_has_committee

# ===== Nạp routers (giữ nguyên cấu trúc) =====
from .routers import auth, account, account_secrets, units, files, plans, grants
from .routers import tasks as tasks_router
from .routers import inbox as inbox_router
from .routers import admin_users  # nếu dự án có
from .routers import committees as committees_router
from .routers import chat as chat_router
from .routers import chat_api as chat_api_router
from .routers import draft_approval as draft_approval_router
from .routers import meetings as meetings_router
from .routers import evaluation as evaluation_router
from .routers import leave_schedule as leave_schedule_router
from .routers import work_positions as work_positions_router
from app.routers import dashboard  # theo yêu cầu

def include_router_with_log(rtr, prefix: str, tags: list[str], module_name: str):
    app.include_router(rtr, prefix=prefix, tags=tags)
    try:
        print(f"[main] Đã nạp router: {module_name}")
    except Exception:
        pass

include_router_with_log(auth.router, "/auth", ["auth"], "app.routers.auth")
include_router_with_log(account.router, "", ["account"], "app.routers.account")
include_router_with_log(account_secrets.router, "", ["account_secrets"], "app.routers.account_secrets")
include_router_with_log(units.router, "/units", ["units"], "app.routers.units")
include_router_with_log(files.router, "/files", ["files"], "app.routers.files")
include_router_with_log(draft_approval_router.router, "/draft-approvals", ["draft_approvals"], "app.routers.draft_approval")
include_router_with_log(plans.router, "/plans", ["plans"], "app.routers.plans")
include_router_with_log(grants.router, "/grants", ["grants"], "app.routers.grants")
include_router_with_log(committees_router.router, "/committees", ["committees"], "app.routers.committees")

# Chat nội bộ - giai đoạn 1

# Chat nội bộ - giai đoạn 1
include_router_with_log(chat_router.router, "", ["chat"], "app.routers.chat")
include_router_with_log(chat_api_router.router, "", ["chat_api"], "app.routers.chat_api")
include_router_with_log(meetings_router.router, "", ["meetings"], "app.routers.meetings")
include_router_with_log(leave_schedule_router.router, "", ["leave_schedule"], "app.routers.leave_schedule")
include_router_with_log(work_positions_router.router, "", ["work_positions"], "app.routers.work_positions")

# Ưu tiên dashboard trước tasks
include_router_with_log(dashboard.router, "", ["dashboard"], "app.routers.dashboard")
include_router_with_log(tasks_router.router, "", ["tasks"], "app.routers.tasks")
include_router_with_log(inbox_router.router, "", ["inbox"], "app.routers.inbox")
include_router_with_log(evaluation_router.router, "/evaluation", ["evaluation"], "app.routers.evaluation")

# Mount router quản trị (nếu có)
app.include_router(admin_users.router, prefix="/admin")

# ===== GẮN helper vào Jinja của các router khác (đặc biệt là account) =====
# Mỗi router thường tự có biến `templates = Jinja2Templates(...)`. Ta không sửa router,
# chỉ nối thêm global vào env nếu tồn tại.
for _mod in (
    account,
    account_secrets,
    units,
    files,
    plans,
    grants,
    committees_router,
    tasks_router,
    inbox_router,
    dashboard,
    chat_router,
    draft_approval_router,
    meetings_router,
    evaluation_router,
    leave_schedule_router,
    work_positions_router,
):
    _tpl = getattr(_mod, "templates", None)
    if _tpl is not None and hasattr(_tpl, "env") and hasattr(_tpl.env, "globals"):
        try:
            _tpl.env.globals["user_unit_names"] = user_unit_names
            _tpl.env.globals["pending_user_approval_count"] = pending_user_approval_count
            _tpl.env.globals["work_access"] = get_work_access_from_session
            _tpl.env.globals["current_user_has_committee"] = current_user_has_committee
            _tpl.env.filters["format_vn_dt"] = format_vn_dt
        except Exception:
            pass


@app.on_event("startup")
def startup_initialize_system():
    initialize_system()

# ===== Chuyển hướng trang gốc =====
@app.get("/", include_in_schema=False)
def root_redirect(request: Request):
    user_id = request.session.get("user_id") if hasattr(request, "session") else None
    if not user_id:
        return RedirectResponse(url="/auth/login", status_code=303)
    return RedirectResponse(url="/plans", status_code=303)

# ===== LOGIN/LOGOUT redirect để khớp navbar =====
@app.get("/login", include_in_schema=False)
def login_redirect_get():
    return RedirectResponse(url="/auth/login", status_code=307)

@app.post("/login", include_in_schema=False)
def login_redirect_post():
    return RedirectResponse(url="/auth/login", status_code=307)

@app.get("/logout", include_in_schema=False)
def logout_redirect_get():
    return RedirectResponse(url="/auth/logout", status_code=307)

@app.post("/logout", include_in_schema=False)
def logout_redirect_post():
    return RedirectResponse(url="/auth/logout", status_code=307)

# ===== Favicon (tránh 404) =====
@app.get("/favicon.ico", include_in_schema=False)
def favicon():
    cand = os.path.join(static_dir, "images", "favicon.ico")
    if os.path.exists(cand):
        return FileResponse(cand, media_type="image/x-icon")
    raise HTTPException(status_code=404, detail="Not Found")
