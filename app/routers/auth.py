from __future__ import annotations

from datetime import datetime
from typing import List, Optional
import logging
import os

from anyio import from_thread

from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import func
from sqlalchemy.orm import Session

from app.database import get_db
from app.models import (
    BlockCode,
    RoleCode,
    Roles,
    Units,
    UnitCategory,
    UnitStatus,
    UserRoles,
    Users,
    UserStatus,
    UserUnitMemberships,
)
from app.org_catalog import POSITION_DEFS, positions_for_block, is_position_allowed_for_block
from app.security.crypto import hash_password, verify_password
from app.config import settings
from app.chat.realtime import manager

router = APIRouter()
templates = Jinja2Templates(directory="app/templates")
logger = logging.getLogger(__name__)


def _get_or_create_role(db: Session, code: str) -> Roles:
    code_up = str(getattr(code, "value", code)).upper()
    role = db.query(Roles).filter(func.upper(func.coalesce(Roles.code, "")) == code_up).first()
    if role:
        return role
    role = Roles(code=code_up, name=code_up)
    db.add(role)
    db.commit()
    db.refresh(role)
    return role


def _load_role_codes_for_user(db: Session, user_id: str) -> List[str]:
    rows = (
        db.query(Roles.code)
        .join(UserRoles, UserRoles.role_id == Roles.id)
        .filter(UserRoles.user_id == user_id)
        .all()
    )
    return [str(getattr(c, "value", c)).upper() for (c,) in rows if c is not None]


def _write_role_flags_to_session(request: Request, role_codes: List[str]) -> None:
    request.session["roles"] = role_codes
    request.session["is_admin"] = "ROLE_ADMIN" in role_codes
    request.session["is_admin_or_leader"] = bool(set(role_codes) & {"ROLE_ADMIN", "ROLE_LANH_DAO"})


def _admin_leader_user_ids(db: Session) -> list[str]:
    rows = (
        db.query(Users.id)
        .join(UserRoles, UserRoles.user_id == Users.id)
        .join(Roles, Roles.id == UserRoles.role_id)
        .filter(func.upper(func.coalesce(Roles.code, "")).in_(["ROLE_ADMIN", "ROLE_LANH_DAO"]))
        .distinct()
        .all()
    )
    return [str(user_id) for (user_id,) in rows if user_id]


def _notify_user_registered_realtime_sync(db: Session, user: Users, *, auto_approve: bool) -> None:
    """
    Phát realtime khi có user đăng ký tài khoản.
    Route register_post hiện là def, nên dùng from_thread.run thay vì đổi route sang async def.
    """
    target_user_ids = _admin_leader_user_ids(db)
    if not target_user_ids:
        return

    pending_count = (
        db.query(Users)
        .filter(Users.status == UserStatus.PENDING_APPROVAL)
        .count()
    )

    payload = {
        "module": "work",
        "type": "users_manage_changed",
        "action": "user_registered_auto_approved" if auto_approve else "user_registered",
        "pending_approval_count": pending_count,
        "changed_user_id": str(user.id or ""),
        "actor_user_id": str(user.id or ""),
        "requires_session_refresh": False,
    }

    try:
        from_thread.run(manager.notify_users_json, target_user_ids, payload)
    except Exception:
        logger.exception("Không phát được realtime khi user đăng ký mới.")


def _is_auto_approved_position(position_code: str) -> bool:
    code = (position_code or "").strip().upper()
    return code in {
        "NHAN_VIEN",
        "BAC_SI",
        "DUOC_SI",
        "DIEU_DUONG",
        "KY_THUAT_VIEN",
        "THU_KY_Y_KHOA",
        "HO_LY",
        "QL_CHAT_LUONG",
        "QL_KY_THUAT",
        "QL_AN_TOAN",
        "QL_VAT_TU",
        "QL_TRANG_THIET_BI",
        "QL_MOI_TRUONG",
        "QL_CNTT",
    }

def _is_valid_username_format(username: str) -> bool:
    value = (username or "").strip()
    if not value:
        return False

    if value.count("_") != 1:
        return False

    left, right = value.split("_", 1)
    if not left or not right:
        return False

    if not right.isdigit():
        return False

    return left.replace("Đ", "D").replace("đ", "d").isalpha()

def _grant_roles_for_position(db: Session, user: Users, position_code: str) -> None:
    cfg = POSITION_DEFS.get((position_code or "").strip().upper(), {})
    role_codes = []
    for code in cfg.get("official_roles", []):
        code_str = str(getattr(code, "value", code)).upper()
        if code_str not in role_codes:
            role_codes.append(code_str)

    for role_code in role_codes:
        role = db.query(Roles).filter(Roles.code == role_code).first()
        if not role:
            continue
        existed = db.query(UserRoles).filter(
            UserRoles.user_id == user.id,
            UserRoles.role_id == role.id
        ).first()
        if not existed:
            db.add(UserRoles(user_id=user.id, role_id=role.id))


def _grant_memberships_for_registered_user(db: Session, user: Users, unit: Units, subunit_obj: Optional[Units]) -> None:
    primary_unit_id = subunit_obj.id if subunit_obj else unit.id

    existed_primary = db.query(UserUnitMemberships).filter(
        UserUnitMemberships.user_id == user.id,
        UserUnitMemberships.unit_id == primary_unit_id
    ).first()
    if not existed_primary:
        db.add(UserUnitMemberships(user_id=user.id, unit_id=primary_unit_id, is_primary=True))

    if subunit_obj:
        existed_parent = db.query(UserUnitMemberships).filter(
            UserUnitMemberships.user_id == user.id,
            UserUnitMemberships.unit_id == unit.id
        ).first()
        if not existed_parent:
            db.add(UserUnitMemberships(user_id=user.id, unit_id=unit.id, is_primary=False))
            
def _active_units_for_register(db: Session):
    rows = (
        db.query(Units)
        .filter(Units.trang_thai == UnitStatus.ACTIVE)
        .order_by(Units.cap_do.asc(), Units.order_index.asc(), Units.ten_don_vi.asc())
        .all()
    )
    return rows


def _serialize_units_for_register(db: Session):
    units = _active_units_for_register(db)
    level1 = []  # Phòng hoặc Khoa
    level2 = []  # Tổ/Nhóm hoặc Đơn vị thuộc Khoa

    for u in units:
        block_code = getattr(getattr(u, "block_code", None), "value", getattr(u, "block_code", "")) or ""
        unit_category = getattr(getattr(u, "unit_category", None), "value", getattr(u, "unit_category", "")) or ""

        item = {
            "id": u.id,
            "name": u.ten_don_vi,
            "cap_do": u.cap_do,
            "parent_id": u.parent_id,
            "block_code": str(block_code),
            "unit_category": str(unit_category),
        }

        # Cấp 1 của form đăng ký:
        # - Khối hành chính: PHONG
        # - Khối chuyên môn: KHOA
        if unit_category in {"PHONG", "KHOA"}:
            level1.append(item)
        elif unit_category == "SUBUNIT":
            level2.append(item)

    return level1, level2


def _render_register(request: Request, db: Session, error: Optional[str] = None, success: Optional[str] = None,
                     form_data: Optional[dict] = None, status_code: int = 200):
    level1, level2 = _serialize_units_for_register(db)
    return templates.TemplateResponse(
        "register.html",
        {
            "request": request,
            "error": error,
            "success": success,
            "form_data": form_data or {},
            "app_name": settings.APP_NAME,
            "company_name": settings.COMPANY_NAME,
            "level1_units": level1,
            "level2_units": level2,
            "positions_hanh_chinh": positions_for_block("HANH_CHINH"),
            "positions_chuyen_mon": positions_for_block("CHUYEN_MON"),
        },
        status_code=status_code,
    )


@router.get("/login", response_class=HTMLResponse)
def login_get(request: Request):
    return templates.TemplateResponse(
        "login.html",
        {
            "request": request,
            "error": None,
            "app_name": settings.APP_NAME,
            "company_name": settings.COMPANY_NAME,
        },
    )


@router.post("/login")
def login_post(
    request: Request,
    username: str = Form(...),
    password: str = Form(...),
    db: Session = Depends(get_db),
):
    u = db.query(Users).filter(Users.username == username).first()
    if not u or not verify_password(password, u.password_hash):
        return templates.TemplateResponse(
            "login.html",
            {"request": request, "error": "Sai tài khoản hoặc mật khẩu", "app_name": settings.APP_NAME, "company_name": settings.COMPANY_NAME},
            status_code=401,
        )
    if u.status != UserStatus.ACTIVE:
        return templates.TemplateResponse(
            "login.html",
            {"request": request, "error": "Tài khoản chưa được phê duyệt hoặc đang bị khóa.", "app_name": settings.APP_NAME, "company_name": settings.COMPANY_NAME},
            status_code=403,
        )

    request.session["user_id"] = u.id
    request.session["username"] = u.username
    _write_role_flags_to_session(request, _load_role_codes_for_user(db, u.id))
    return RedirectResponse(url="/dashboard", status_code=302)


@router.get("/logout")
def logout(request: Request):
    request.session.clear()
    return RedirectResponse(url="/login", status_code=307)


@router.get("/register", response_class=HTMLResponse)
def register_get(request: Request, db: Session = Depends(get_db)):
    return _render_register(request, db)


@router.post("/register", response_class=HTMLResponse)
def register_post(
    request: Request,
    full_name: str = Form(...),
    username: str = Form(...),
    email: Optional[str] = Form(None),
    phone: Optional[str] = Form(None),
    password: str = Form(...),
    confirm_password: str = Form(...),
    block_code: str = Form(...),
    unit_id: str = Form(...),
    subunit_id: Optional[str] = Form(None),
    position_code: str = Form(...),
    db: Session = Depends(get_db),
):
    form_data = {
        "full_name": full_name or "",
        "username": username or "",
        "email": email or "",
        "phone": phone or "",
        "block_code": block_code or "",
        "unit_id": unit_id or "",
        "subunit_id": subunit_id or "",
        "position_code": position_code or "",
    }

    username = (username or "").strip()
    full_name = (full_name or "").strip()
    block_code = (block_code or "").strip().upper()
    unit_id = (unit_id or "").strip()
    subunit_id = (subunit_id or "").strip()
    position_code = (position_code or "").strip().upper()

    if not username or not full_name:
        return _render_register(request, db, error="Họ tên và tên đăng nhập là bắt buộc.", form_data=form_data, status_code=400)
    if not _is_valid_username_format(username):
        return _render_register(
            request,
            db,
            error="Tên đăng nhập phải đúng định dạng Chữ_Số. Ví dụ: Quang_123.",
            form_data=form_data,
            status_code=400,
        )
    if password != confirm_password:
        return _render_register(request, db, error="Mật khẩu nhập lại không khớp.", form_data=form_data, status_code=400)
    if db.query(Users).filter(Users.username == username).first():
        return _render_register(request, db, error="Tên đăng nhập đã tồn tại.", form_data=form_data, status_code=400)
    if block_code not in {BlockCode.HANH_CHINH.value, BlockCode.CHUYEN_MON.value}:
        return _render_register(request, db, error="Khối đăng ký không hợp lệ.", form_data=form_data, status_code=400)
    if not is_position_allowed_for_block(position_code, block_code):
        return _render_register(request, db, error="Vị trí đăng ký không phù hợp với khối đã chọn.", form_data=form_data, status_code=400)

    unit = db.get(Units, unit_id)
    if not unit or unit.trang_thai != UnitStatus.ACTIVE:
        return _render_register(request, db, error="Phòng/Khoa đăng ký không hợp lệ.", form_data=form_data, status_code=400)

    unit_block_code = getattr(getattr(unit, "block_code", None), "value", getattr(unit, "block_code", "")) or ""
    unit_category = getattr(getattr(unit, "unit_category", None), "value", getattr(unit, "unit_category", "")) or ""

    if unit_category not in {"PHONG", "KHOA"}:
        return _render_register(request, db, error="Đơn vị được chọn phải là Phòng hoặc Khoa.", form_data=form_data, status_code=400)

    if str(unit_block_code) != block_code:
        return _render_register(request, db, error="Đơn vị được chọn không thuộc khối đã chọn.", form_data=form_data, status_code=400)

    subunit_obj = None
    if subunit_id:
        subunit_obj = db.get(Units, subunit_id)
        if not subunit_obj or subunit_obj.trang_thai != UnitStatus.ACTIVE:
            return _render_register(request, db, error="Tổ/Nhóm/Đơn vị trực thuộc đăng ký không hợp lệ.", form_data=form_data, status_code=400)

        subunit_category = getattr(getattr(subunit_obj, "unit_category", None), "value", getattr(subunit_obj, "unit_category", "")) or ""
        if subunit_category != "SUBUNIT":
            return _render_register(request, db, error="Đơn vị trực thuộc được chọn không hợp lệ.", form_data=form_data, status_code=400)

        if str(getattr(subunit_obj, "parent_id", "") or "") != unit.id:
            return _render_register(request, db, error="Tổ/Nhóm/Đơn vị trực thuộc không thuộc đúng Phòng/Khoa đã chọn.", form_data=form_data, status_code=400)

    auto_approve = _is_auto_approved_position(position_code)

    u = Users(
        full_name=full_name,
        username=username,
        email=(email or "").strip() or None,
        phone=(phone or "").strip() or None,
        password_hash=hash_password(password),
        status=UserStatus.ACTIVE if auto_approve else UserStatus.PENDING_APPROVAL,
        requested_block_code=block_code,
        requested_unit_id=unit.id,
        requested_subunit_id=subunit_obj.id if subunit_obj else None,
        requested_position_code=position_code,
        approved_block_code=block_code if auto_approve else None,
        approved_unit_id=unit.id if auto_approve else None,
        approved_subunit_id=subunit_obj.id if (auto_approve and subunit_obj) else None,
        approved_position_code=position_code if auto_approve else None,
        approved_at=datetime.utcnow() if auto_approve else None,
    )
    db.add(u)
    db.flush()

    if auto_approve:
        _grant_roles_for_position(db, u, position_code)
        _grant_memberships_for_registered_user(db, u, unit, subunit_obj)

    db.commit()
    db.refresh(u)

    _notify_user_registered_realtime_sync(db, u, auto_approve=auto_approve)

    return _render_register(
        request,
        db,
        success=(
            "Đăng ký thành công. Tài khoản đã được kích hoạt ngay theo vị trí chuyên môn."
            if auto_approve
            else "Đăng ký thành công. Tài khoản đang chờ admin phê duyệt vị trí và đơn vị chính thức."
        ),
        form_data={},
        status_code=200,
    )