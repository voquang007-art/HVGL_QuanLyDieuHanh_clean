# app/routers/account.py
from fastapi import APIRouter, Request, Depends, Form, HTTPException, status, Query
from starlette.responses import RedirectResponse
from starlette.templating import Jinja2Templates
from sqlalchemy.orm import Session
from sqlalchemy import and_, func
import os

# Sửa đúng cấp import tương đối (trước đây là .security.deps gây lỗi)
from ..security.deps import get_db, login_required, user_has_any_role
from ..models import (
    Users, UserStatus, Roles, RoleCode, Tasks,
    UserRoles, UserUnitMemberships, Units
)
from ..config import settings
from ..security.crypto import verify_password, hash_password
from ..org_catalog import POSITION_DEFS, POSITION_LABELS
from ..chat.realtime import manager

router = APIRouter()
templates = Jinja2Templates(directory=os.path.join(os.path.dirname(__file__), "..", "templates"))

# ================== TIỆN ÍCH QUYỀN ==================
def _require_admin_or_leader(user: Users, db: Session):
    if not user_has_any_role(user, db, [RoleCode.ROLE_ADMIN, RoleCode.ROLE_LANH_DAO]):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Chỉ Admin hoặc Lãnh đạo được truy cập.")

# Nhóm role “vị trí” cho phép điều chỉnh
_POSITION_ROLE_CODES = [
    RoleCode.ROLE_LANH_DAO,

    RoleCode.ROLE_TONG_GIAM_DOC,
    RoleCode.ROLE_PHO_TONG_GIAM_DOC_THUONG_TRUC,
    RoleCode.ROLE_PHO_TONG_GIAM_DOC,

    RoleCode.ROLE_GIAM_DOC,
    RoleCode.ROLE_PHO_GIAM_DOC_TRUC,
    RoleCode.ROLE_PHO_GIAM_DOC,

    RoleCode.ROLE_TRUONG_PHONG,
    RoleCode.ROLE_PHO_PHONG,
    RoleCode.ROLE_TO_TRUONG,
    RoleCode.ROLE_PHO_TO,

    RoleCode.ROLE_TRUONG_KHOA,
    RoleCode.ROLE_PHO_TRUONG_KHOA,

    RoleCode.ROLE_BAC_SI,
    RoleCode.ROLE_DUOC_SI,
    RoleCode.ROLE_DIEU_DUONG_TRUONG,
    RoleCode.ROLE_KY_THUAT_VIEN_TRUONG,
    RoleCode.ROLE_DIEU_DUONG,
    RoleCode.ROLE_KY_THUAT_VIEN,
    RoleCode.ROLE_THU_KY_Y_KHOA,
    RoleCode.ROLE_HO_LY,

    RoleCode.ROLE_QL_CHAT_LUONG,
    RoleCode.ROLE_QL_KY_THUAT,
    RoleCode.ROLE_QL_AN_TOAN,
    RoleCode.ROLE_QL_VAT_TU,
    RoleCode.ROLE_QL_TRANG_THIET_BI,
    RoleCode.ROLE_QL_MOI_TRUONG,
    RoleCode.ROLE_QL_CNTT,

    RoleCode.ROLE_TRUONG_NHOM,
    RoleCode.ROLE_PHO_NHOM,
    RoleCode.ROLE_TRUONG_DON_VI,
    RoleCode.ROLE_PHO_DON_VI,
    RoleCode.ROLE_DIEU_DUONG_TRUONG_DON_VI,
    RoleCode.ROLE_KY_THUAT_VIEN_TRUONG_DON_VI,
    RoleCode.ROLE_NHAN_VIEN,
]
_POSITION_MAP = {
    "TONG_GIAM_DOC": RoleCode.ROLE_TONG_GIAM_DOC,
    "PHO_TONG_GIAM_DOC_THUONG_TRUC": RoleCode.ROLE_PHO_TONG_GIAM_DOC_THUONG_TRUC,
    "PHO_TONG_GIAM_DOC": RoleCode.ROLE_PHO_TONG_GIAM_DOC,

    "GIAM_DOC": RoleCode.ROLE_GIAM_DOC,
    "PHO_GIAM_DOC_TRUC": RoleCode.ROLE_PHO_GIAM_DOC_TRUC,
    "PHO_GIAM_DOC": RoleCode.ROLE_PHO_GIAM_DOC,

    "TRUONG_PHONG": RoleCode.ROLE_TRUONG_PHONG,
    "PHO_PHONG": RoleCode.ROLE_PHO_PHONG,
    "TO_TRUONG": RoleCode.ROLE_TO_TRUONG,
    "PHO_TO": RoleCode.ROLE_PHO_TO,

    "TRUONG_KHOA": RoleCode.ROLE_TRUONG_KHOA,
    "PHO_KHOA": RoleCode.ROLE_PHO_TRUONG_KHOA,

    "BAC_SI": RoleCode.ROLE_BAC_SI,
    "DUOC_SI": RoleCode.ROLE_DUOC_SI,
    "DIEU_DUONG_TRUONG": RoleCode.ROLE_DIEU_DUONG_TRUONG,
    "KY_THUAT_VIEN_TRUONG": RoleCode.ROLE_KY_THUAT_VIEN_TRUONG,
    "DIEU_DUONG": RoleCode.ROLE_DIEU_DUONG,
    "KY_THUAT_VIEN": RoleCode.ROLE_KY_THUAT_VIEN,
    "THU_KY_Y_KHOA": RoleCode.ROLE_THU_KY_Y_KHOA,
    "HO_LY": RoleCode.ROLE_HO_LY,

    "QL_CHAT_LUONG": RoleCode.ROLE_QL_CHAT_LUONG,
    "QL_KY_THUAT": RoleCode.ROLE_QL_KY_THUAT,
    "QL_AN_TOAN": RoleCode.ROLE_QL_AN_TOAN,
    "QL_VAT_TU": RoleCode.ROLE_QL_VAT_TU,
    "QL_TRANG_THIET_BI": RoleCode.ROLE_QL_TRANG_THIET_BI,
    "QL_MOI_TRUONG": RoleCode.ROLE_QL_MOI_TRUONG,
    "QL_CNTT": RoleCode.ROLE_QL_CNTT,

    "TRUONG_NHOM": RoleCode.ROLE_TRUONG_NHOM,
    "PHO_NHOM": RoleCode.ROLE_PHO_NHOM,
    "TRUONG_DON_VI": RoleCode.ROLE_TRUONG_DON_VI,
    "PHO_DON_VI": RoleCode.ROLE_PHO_DON_VI,
    "DIEU_DUONG_TRUONG_DON_VI": RoleCode.ROLE_DIEU_DUONG_TRUONG_DON_VI,
    "KY_THUAT_VIEN_TRUONG_DON_VI": RoleCode.ROLE_KY_THUAT_VIEN_TRUONG_DON_VI,
    "NHAN_VIEN": RoleCode.ROLE_NHAN_VIEN,
}

_POSITION_LABELS = POSITION_LABELS

_SPECIAL_EXTRA_ROLE_CODES = [
    RoleCode.ROLE_ADMIN,
    RoleCode.ROLE_LANH_DAO,

    RoleCode.ROLE_TONG_GIAM_DOC,
    RoleCode.ROLE_PHO_TONG_GIAM_DOC_THUONG_TRUC,
    RoleCode.ROLE_PHO_TONG_GIAM_DOC,

    RoleCode.ROLE_GIAM_DOC,
    RoleCode.ROLE_PHO_GIAM_DOC_TRUC,
    RoleCode.ROLE_PHO_GIAM_DOC,
]

_SPECIAL_EXTRA_ROLE_LABELS = {
    "ROLE_ADMIN": "Admin hệ thống",
    "ROLE_LANH_DAO": "HĐTV / Lãnh đạo",

    "ROLE_TONG_GIAM_DOC": "Tổng Giám đốc",
    "ROLE_PHO_TONG_GIAM_DOC_THUONG_TRUC": "Phó Tổng Giám đốc thường trực",
    "ROLE_PHO_TONG_GIAM_DOC": "Phó Tổng Giám đốc",

    "ROLE_GIAM_DOC": "Giám đốc",
    "ROLE_PHO_GIAM_DOC_TRUC": "Phó Giám đốc trực",
    "ROLE_PHO_GIAM_DOC": "Phó Giám đốc",
}

_ROLECODE_TO_POSITION_KEY = {
    str(RoleCode.ROLE_TONG_GIAM_DOC): "TONG_GIAM_DOC",
    str(RoleCode.ROLE_PHO_TONG_GIAM_DOC_THUONG_TRUC): "PHO_TONG_GIAM_DOC_THUONG_TRUC",
    str(RoleCode.ROLE_PHO_TONG_GIAM_DOC): "PHO_TONG_GIAM_DOC",

    str(RoleCode.ROLE_GIAM_DOC): "GIAM_DOC",
    str(RoleCode.ROLE_PHO_GIAM_DOC_TRUC): "PHO_GIAM_DOC_TRUC",
    str(RoleCode.ROLE_PHO_GIAM_DOC): "PHO_GIAM_DOC",

    str(RoleCode.ROLE_TRUONG_PHONG): "TRUONG_PHONG",
    str(RoleCode.ROLE_PHO_PHONG): "PHO_PHONG",
    str(RoleCode.ROLE_TO_TRUONG): "TO_TRUONG",
    str(RoleCode.ROLE_PHO_TO): "PHO_TO",

    str(RoleCode.ROLE_TRUONG_KHOA): "TRUONG_KHOA",
    str(RoleCode.ROLE_PHO_TRUONG_KHOA): "PHO_KHOA",

    str(RoleCode.ROLE_BAC_SI): "BAC_SI",
    str(RoleCode.ROLE_DUOC_SI): "DUOC_SI",
    str(RoleCode.ROLE_DIEU_DUONG_TRUONG): "DIEU_DUONG_TRUONG",
    str(RoleCode.ROLE_KY_THUAT_VIEN_TRUONG): "KY_THUAT_VIEN_TRUONG",
    str(RoleCode.ROLE_DIEU_DUONG): "DIEU_DUONG",
    str(RoleCode.ROLE_KY_THUAT_VIEN): "KY_THUAT_VIEN",
    str(RoleCode.ROLE_THU_KY_Y_KHOA): "THU_KY_Y_KHOA",
    str(RoleCode.ROLE_HO_LY): "HO_LY",

    str(RoleCode.ROLE_QL_CHAT_LUONG): "QL_CHAT_LUONG",
    str(RoleCode.ROLE_QL_KY_THUAT): "QL_KY_THUAT",
    str(RoleCode.ROLE_QL_AN_TOAN): "QL_AN_TOAN",
    str(RoleCode.ROLE_QL_VAT_TU): "QL_VAT_TU",
    str(RoleCode.ROLE_QL_TRANG_THIET_BI): "QL_TRANG_THIET_BI",
    str(RoleCode.ROLE_QL_MOI_TRUONG): "QL_MOI_TRUONG",
    str(RoleCode.ROLE_QL_CNTT): "QL_CNTT",
    str(RoleCode.ROLE_TRUONG_DON_VI): "TRUONG_DON_VI",
    str(RoleCode.ROLE_PHO_DON_VI): "PHO_DON_VI",
    str(RoleCode.ROLE_DIEU_DUONG_TRUONG_DON_VI): "DIEU_DUONG_TRUONG_DON_VI",
    str(RoleCode.ROLE_KY_THUAT_VIEN_TRUONG_DON_VI): "KY_THUAT_VIEN_TRUONG_DON_VI",
    str(RoleCode.ROLE_TRUONG_NHOM): "TRUONG_NHOM",
    str(RoleCode.ROLE_PHO_NHOM): "PHO_NHOM",

    str(RoleCode.ROLE_NHAN_VIEN): "NHAN_VIEN",
}

def _get_role_code_of_user(db: Session, user: Users):
    row = (
        db.query(Roles.code)
        .join(UserRoles, UserRoles.role_id == Roles.id)
        .filter(UserRoles.user_id == user.id)
        .filter(Roles.code.in_(_POSITION_ROLE_CODES))
        .first()
    )
    return row[0] if row else None

def _position_role_codes_only(position_key: str) -> list[str]:
    cfg = POSITION_DEFS.get((position_key or "").strip().upper(), {})
    out: list[str] = []
    for code in cfg.get("official_roles", []):
        code_str = str(getattr(code, "value", code)).upper()
        if code_str not in out:
            out.append(code_str)
    return out
    
    
def _unit_category_value(unit: Units | None) -> str:
    if not unit:
        return ""
    return str(getattr(getattr(unit, "unit_category", None), "value", getattr(unit, "unit_category", "")) or "")

def _unit_block_value(unit: Units | None) -> str:
    if not unit:
        return ""
    return str(getattr(getattr(unit, "block_code", None), "value", getattr(unit, "block_code", "")) or "")

def _resolve_unit_scope(db: Session, primary_unit_id: str) -> tuple[Units, Units | None]:
    primary_unit = db.get(Units, primary_unit_id)
    if not primary_unit:
        raise HTTPException(status_code=404, detail="Không tìm thấy đơn vị hiện hành.")

    if _unit_category_value(primary_unit) == "SUBUNIT":
        parent_unit = db.get(Units, getattr(primary_unit, "parent_id", None))
        if not parent_unit:
            raise HTTPException(status_code=400, detail="Đơn vị trực thuộc đang thiếu đơn vị cha.")
        return parent_unit, primary_unit

    return primary_unit, None

def _format_unit_display(unit: Units | None, subunit: Units | None = None) -> str:
    if unit is None and subunit is None:
        return "-"
    if unit is not None and subunit is not None:
        return f"{getattr(unit, 'ten_don_vi', '') or '-'} / {getattr(subunit, 'ten_don_vi', '') or '-'}"
    return getattr(unit or subunit, "ten_don_vi", None) or "-"

def _admin_leader_user_ids(db: Session) -> list[str]:
    rows = (
        db.query(UserRoles.user_id)
        .join(Roles, Roles.id == UserRoles.role_id)
        .filter(Roles.code.in_([RoleCode.ROLE_ADMIN, RoleCode.ROLE_LANH_DAO]))
        .distinct()
        .all()
    )
    return [str(user_id) for (user_id,) in rows if user_id]



def _role_code_value(role_code) -> str:
    return str(getattr(role_code, "value", role_code) or "").upper()


def _get_or_create_role_by_code(db: Session, role_code: str) -> Roles:
    code = _role_code_value(role_code)
    role = (
        db.query(Roles)
        .filter(func.upper(func.coalesce(Roles.code, "")) == code)
        .first()
    )
    if role:
        return role

    role = Roles(code=code, name=_SPECIAL_EXTRA_ROLE_LABELS.get(code, code))
    db.add(role)
    db.flush()
    return role


def _user_role_code_set(db: Session, user_id: str) -> set[str]:
    rows = (
        db.query(Roles.code)
        .join(UserRoles, UserRoles.role_id == Roles.id)
        .filter(UserRoles.user_id == user_id)
        .all()
    )
    return {_role_code_value(code) for (code,) in rows if code}


def _ensure_user_role_by_code(db: Session, user: Users, role_code: str) -> None:
    role = _get_or_create_role_by_code(db, role_code)
    existed = (
        db.query(UserRoles)
        .filter(UserRoles.user_id == user.id, UserRoles.role_id == role.id)
        .first()
    )
    if not existed:
        db.add(UserRoles(user_id=user.id, role_id=role.id))
        db.flush()


def _remove_user_role_by_code(db: Session, user: Users, role_code: str) -> None:
    code = _role_code_value(role_code)
    role = (
        db.query(Roles)
        .filter(func.upper(func.coalesce(Roles.code, "")) == code)
        .first()
    )
    if not role:
        return

    db.query(UserRoles).filter(
        UserRoles.user_id == user.id,
        UserRoles.role_id == role.id,
    ).delete(synchronize_session=False)
    db.flush()


def _find_special_unit(db: Session, *, unit_category: str, name_keywords: list[str]) -> Units | None:
    units = db.query(Units).all()
    category = str(unit_category or "").upper().strip()
    keywords = [str(x or "").lower().strip() for x in name_keywords if str(x or "").strip()]

    for unit in units:
        current_category = _unit_category_value(unit)
        unit_name = str(getattr(unit, "ten_don_vi", "") or "").lower()
        if category and current_category != category:
            continue
        if keywords and not any(keyword in unit_name for keyword in keywords):
            continue
        return unit

    return None


def _ensure_secondary_membership(db: Session, user: Users, unit: Units | None) -> None:
    if not unit or not getattr(unit, "id", None):
        return

    existed = (
        db.query(UserUnitMemberships)
        .filter(
            UserUnitMemberships.user_id == user.id,
            UserUnitMemberships.unit_id == unit.id,
        )
        .first()
    )
    if existed:
        return

    db.add(UserUnitMemberships(user_id=user.id, unit_id=unit.id, is_primary=False))
    db.flush()


def _remove_secondary_membership_if_not_needed(db: Session, user: Users, unit: Units | None) -> None:
    if not unit or not getattr(unit, "id", None):
        return

    db.query(UserUnitMemberships).filter(
        UserUnitMemberships.user_id == user.id,
        UserUnitMemberships.unit_id == unit.id,
        UserUnitMemberships.is_primary == False,  # noqa: E712
    ).delete(synchronize_session=False)
    db.flush()


def _sync_special_role_memberships(db: Session, user: Users) -> None:
    role_codes = _user_role_code_set(db, user.id)

    hdtv_roles = {
        "ROLE_LANH_DAO",
        "ROLE_TONG_GIAM_DOC",
        "ROLE_PHO_TONG_GIAM_DOC_THUONG_TRUC",
        "ROLE_PHO_TONG_GIAM_DOC",
    }
    bgd_roles = {
        "ROLE_GIAM_DOC",
        "ROLE_PHO_GIAM_DOC_TRUC",
        "ROLE_PHO_GIAM_DOC",
    }

    hdtv_unit = _find_special_unit(db, unit_category="ROOT", name_keywords=["hđtv", "hdtv"])
    bgd_unit = _find_special_unit(db, unit_category="EXECUTIVE", name_keywords=["bgđ", "bgd"])

    if role_codes & hdtv_roles:
        _ensure_secondary_membership(db, user, hdtv_unit)
    else:
        _remove_secondary_membership_if_not_needed(db, user, hdtv_unit)

    if role_codes & bgd_roles:
        _ensure_secondary_membership(db, user, bgd_unit)
    else:
        _remove_secondary_membership_if_not_needed(db, user, bgd_unit)

async def _notify_users_manage_changed(db: Session, *, actor_user_id: str | None = None, changed_user_id: str | None = None, action: str = "") -> None:
    """
    Phát realtime khi Admin/Lãnh đạo duyệt user, đổi vị trí, gán/gỡ vai trò, điều chuyển đơn vị.

    Mục tiêu:
    - Admin/Lãnh đạo cập nhật badge xét duyệt user.
    - User vừa được đổi quyền tự refresh session để thấy đúng Plans/Tasks/Inbox.
    - Các user đang mở Plans/Tasks/Inbox tự reload theo websocket, không cần F5.
    """
    pending_count = db.query(Users).filter(Users.status == UserStatus.PENDING_APPROVAL).count()

    target_user_ids = set(_admin_leader_user_ids(db))

    # Gửi cho chính user bị thay đổi quyền để browser tự refresh session.
    if changed_user_id:
        target_user_ids.add(str(changed_user_id))

    # Gửi cho toàn bộ user active để các trang Plans/Tasks/Inbox đang mở tự cập nhật phạm vi.
    try:
        active_rows = db.query(Users.id).filter(Users.status == UserStatus.ACTIVE).all()
        for (uid,) in active_rows:
            if uid:
                target_user_ids.add(str(uid))
    except Exception:
        pass

    if actor_user_id:
        target_user_ids.add(str(actor_user_id))

    payload = {
        "module": "work",
        "type": "users_manage_changed",
        "action": action or "changed",
        "pending_approval_count": pending_count,
        "changed_user_id": str(changed_user_id or ""),
        "actor_user_id": str(actor_user_id or ""),
        "requires_session_refresh": True,
    }
    await manager.notify_users_json(target_user_ids, payload)
    
@router.get("/account/refresh-session")
def refresh_account_session(request: Request, next: str = Query("/dashboard"), db: Session = Depends(get_db)):
    """
    Làm mới session quyền của user hiện tại sau khi Admin duyệt/gán vị trí/gán vai trò.
    Dùng cho realtime, tránh user phải logout/login hoặc bấm F5 thủ công.
    """
    current_user = login_required(request, db)
    current_user = db.get(Users, current_user.id)

    if not current_user or current_user.status != UserStatus.ACTIVE:
        request.session.clear()
        return RedirectResponse(url="/login", status_code=302)

    role_codes = sorted(_user_role_code_set(db, current_user.id))
    request.session["user_id"] = current_user.id
    request.session["username"] = current_user.username
    request.session["roles"] = role_codes
    request.session["is_admin"] = "ROLE_ADMIN" in role_codes
    request.session["is_admin_or_leader"] = bool(set(role_codes) & {"ROLE_ADMIN", "ROLE_LANH_DAO"})

    safe_next = (next or "/dashboard").strip()
    if not safe_next.startswith("/"):
        safe_next = "/dashboard"

    return RedirectResponse(url=safe_next, status_code=302)
    

def _validate_position_unit_scope(position_key: str, approved_unit: Units, approved_subunit: Units | None) -> None:
    unit_cat = _unit_category_value(approved_unit)
    sub_cat = _unit_category_value(approved_subunit)

    if position_key in {"TONG_GIAM_DOC", "PHO_TONG_GIAM_DOC_THUONG_TRUC", "PHO_TONG_GIAM_DOC"}:
        if unit_cat != "ROOT":
            raise HTTPException(status_code=400, detail="Các vị trí HĐTV phải gán cho đơn vị HĐTV.")
        if approved_subunit is not None:
            raise HTTPException(status_code=400, detail="Các vị trí HĐTV không được gán xuống đơn vị con.")

    elif position_key in {"GIAM_DOC", "PHO_GIAM_DOC_TRUC", "PHO_GIAM_DOC"}:
        if unit_cat != "EXECUTIVE":
            raise HTTPException(status_code=400, detail="Các vị trí BGĐ phải gán cho đơn vị BGĐ.")
        if approved_subunit is not None:
            raise HTTPException(status_code=400, detail="Các vị trí BGĐ không được gán xuống đơn vị con.")

    elif position_key in {"TRUONG_PHONG", "PHO_PHONG"}:
        if unit_cat != "PHONG":
            raise HTTPException(status_code=400, detail="Trưởng/Phó phòng phải gán cho đơn vị Phòng.")
        if approved_subunit is not None:
            raise HTTPException(status_code=400, detail="Trưởng/Phó phòng không được gán đơn vị chính là Tổ/Nhóm.")

    elif position_key in {"TO_TRUONG", "PHO_TO"}:
        if unit_cat != "PHONG":
            raise HTTPException(status_code=400, detail="Tổ trưởng/Tổ phó phải chọn Phòng cha.")
        if approved_subunit is None or sub_cat != "SUBUNIT":
            raise HTTPException(status_code=400, detail="Tổ trưởng/Tổ phó phải gán xuống Tổ/Nhóm trực thuộc.")

    elif position_key in {"TRUONG_KHOA", "PHO_KHOA", "KY_THUAT_VIEN_TRUONG", "QL_CHAT_LUONG", "QL_KY_THUAT", "QL_AN_TOAN", "QL_VAT_TU", "QL_TRANG_THIET_BI", "QL_MOI_TRUONG", "QL_CNTT"}:
        if unit_cat != "KHOA":
            raise HTTPException(status_code=400, detail="Vị trí này phải gán cho đơn vị Khoa.")
        if approved_subunit is not None:
            raise HTTPException(status_code=400, detail="Vị trí này không được gán đơn vị chính là đơn vị thuộc Khoa.")

    elif position_key in {"TRUONG_NHOM", "PHO_NHOM"}:
        if unit_cat != "KHOA":
            raise HTTPException(status_code=400, detail="Trưởng/Phó nhóm phải chọn Khoa cha.")
        if approved_subunit is None or sub_cat != "SUBUNIT":
            raise HTTPException(status_code=400, detail="Trưởng/Phó nhóm phải gán xuống Nhóm/Đơn vị trực thuộc.")
            
    elif position_key in {"TRUONG_DON_VI", "PHO_DON_VI", "DIEU_DUONG_TRUONG_DON_VI", "KY_THUAT_VIEN_TRUONG_DON_VI"}:
        if unit_cat != "KHOA":
            raise HTTPException(status_code=400, detail="Vị trí này phải chọn Khoa cha.")
        if approved_subunit is None or sub_cat != "SUBUNIT":
            raise HTTPException(status_code=400, detail="Vị trí này phải gán xuống Đơn vị thuộc Khoa.")

    elif position_key in {"THU_KY_Y_KHOA"}:
        if unit_cat != "KHOA":
            raise HTTPException(status_code=400, detail="Thư ký y khoa phải chọn Khoa cha.")
        if approved_subunit is not None and sub_cat != "SUBUNIT":
            raise HTTPException(status_code=400, detail="Đơn vị trực thuộc không hợp lệ.")

    elif position_key in {"BAC_SI", "DIEU_DUONG_TRUONG", "DIEU_DUONG", "KY_THUAT_VIEN", "HO_LY", "NHAN_VIEN"}:
        if unit_cat not in {"PHONG", "KHOA"}:
            raise HTTPException(status_code=400, detail="Đơn vị cha phải là Phòng hoặc Khoa.")
        if approved_subunit is not None and sub_cat != "SUBUNIT":
            raise HTTPException(status_code=400, detail="Đơn vị trực thuộc không hợp lệ.")


def _rebuild_user_memberships_for_position(db: Session, user: Users, role_code: RoleCode, primary_unit_id: str) -> None:
    unit = db.get(Units, primary_unit_id)
    if not unit:
        raise HTTPException(status_code=404, detail="Không tìm thấy đơn vị chính.")

    db.query(UserUnitMemberships).filter(UserUnitMemberships.user_id == user.id).delete(synchronize_session=False)

    memberships_to_add = [(primary_unit_id, True)]
    if int(getattr(unit, "cap_do", 0) or 0) >= 3 and getattr(unit, "parent_id", None):
        memberships_to_add.append((unit.parent_id, False))

    seen = set()
    for unit_id, is_primary in memberships_to_add:
        if not unit_id or unit_id in seen:
            continue
        seen.add(unit_id)
        db.add(UserUnitMemberships(user_id=user.id, unit_id=unit_id, is_primary=is_primary))

    _sync_special_role_memberships(db, user)

def _set_user_position(db: Session, user: Users, role_code: RoleCode) -> None:
    # Gỡ role vị trí chính, nhưng giữ các role kiêm nhiệm đặc biệt như Admin/HĐTV/BGĐ.
    special_codes = {_role_code_value(x) for x in _SPECIAL_EXTRA_ROLE_CODES}
    removable_codes = [
        rc for rc in _POSITION_ROLE_CODES
        if _role_code_value(rc) not in special_codes
    ]

    role_ids = [r.id for r in db.query(Roles).filter(Roles.code.in_(removable_codes)).all()]
    if role_ids:
        db.query(UserRoles).filter(
            UserRoles.user_id == user.id,
            UserRoles.role_id.in_(role_ids)
        ).delete(synchronize_session=False)
        db.flush()

    role_obj = db.query(Roles).filter(Roles.code == role_code).first()
    if not role_obj:
        raise HTTPException(status_code=400, detail="Mã vị trí không hợp lệ trong hệ thống.")

    existed = db.query(UserRoles).filter(
        UserRoles.user_id == user.id,
        UserRoles.role_id == role_obj.id
    ).first()
    if not existed:
        db.add(UserRoles(user_id=user.id, role_id=role_obj.id))

    _sync_special_role_memberships(db, user)

def _transfer_user_unit(db: Session, user: Users, new_unit_id: str) -> None:
    """
    Điều chuyển đơn vị chính:
    - Cập nhật đơn vị chính nếu Users có field lưu trực tiếp.
    - Đồng thời CHUẨN HOÁ LẠI toàn bộ membership hiện hành theo vị trí hiện tại.
    - Không giữ membership lịch sử của đơn vị cũ trong bảng membership hiện hành.
    """
    if hasattr(user, "don_vi_chinh_id"):
        user.don_vi_chinh_id = new_unit_id
    elif hasattr(user, "unit_id"):
        user.unit_id = new_unit_id

    current_role_code = _get_role_code_of_user(db, user)
    if not current_role_code:
        raise HTTPException(status_code=400, detail="Người dùng chưa có vị trí hiện hành để chuẩn hoá đơn vị.")

    _rebuild_user_memberships_for_position(db, user, current_role_code, new_unit_id)

def _decorate_manage_users(db: Session, users: list[Users]) -> list[Users]:
    if not users:
        return users

    user_ids = [u.id for u in users]
    if not user_ids:
        return users

    role_rows = (
        db.query(UserRoles.user_id, Roles.code, Roles.name)
        .join(Roles, Roles.id == UserRoles.role_id)
        .filter(UserRoles.user_id.in_(user_ids))
        .filter(Roles.code.in_(_POSITION_ROLE_CODES))
        .all()
    )

    role_map = {}
    for user_id, role_code, role_name in role_rows:
        code_str = str(role_code)
        pos_key = _ROLECODE_TO_POSITION_KEY.get(code_str)
        if pos_key and user_id not in role_map:
            role_map[user_id] = {
                "position_key": pos_key,
                "position_label": _POSITION_LABELS.get(pos_key, role_name or code_str),
            }

    mem_rows = (
        db.query(
            UserUnitMemberships.user_id,
            UserUnitMemberships.unit_id,
            UserUnitMemberships.is_primary,
            Units.ten_don_vi,
            Units.cap_do,
            Units.parent_id,
            Units.block_code,
            Units.unit_category,
        )
        .join(Units, Units.id == UserUnitMemberships.unit_id)
        .filter(UserUnitMemberships.user_id.in_(user_ids))
        .order_by(
            UserUnitMemberships.user_id.asc(),
            UserUnitMemberships.is_primary.desc(),
            Units.cap_do.desc(),
            Units.ten_don_vi.asc(),
        )
        .all()
    )

    parent_ids = {str(parent_id) for *_, parent_id, __block, __cat in mem_rows if parent_id}
    parent_name_map = {}
    if parent_ids:
        parent_rows = db.query(Units.id, Units.ten_don_vi).filter(Units.id.in_(list(parent_ids))).all()
        parent_name_map = {str(unit_id): (ten_don_vi or "") for unit_id, ten_don_vi in parent_rows}

    unit_map = {}
    for user_id, unit_id, is_primary, ten_don_vi, cap_do, parent_id, block_code, unit_category in mem_rows:
        if user_id not in unit_map:
            unit_map[user_id] = {
                "unit_id": unit_id,
                "unit_name": ten_don_vi or "",
                "unit_cap_do": cap_do,
                "parent_id": parent_id,
                "parent_name": parent_name_map.get(str(parent_id), "") if parent_id else "",
                "block_code": str(getattr(block_code, "value", block_code) or ""),
                "unit_category": str(getattr(unit_category, "value", unit_category) or ""),
            }

    special_role_rows = (
        db.query(UserRoles.user_id, Roles.code, Roles.name)
        .join(Roles, Roles.id == UserRoles.role_id)
        .filter(UserRoles.user_id.in_(user_ids))
        .all()
    )

    special_role_map: dict[str, list[dict[str, str]]] = {}
    allowed_special_codes = {_role_code_value(x) for x in _SPECIAL_EXTRA_ROLE_CODES}

    for user_id, role_code, role_name in special_role_rows:
        code_str = _role_code_value(role_code)
        if code_str not in allowed_special_codes:
            continue

        if user_id not in special_role_map:
            special_role_map[user_id] = []

        special_role_map[user_id].append({
            "code": code_str,
            "label": _SPECIAL_EXTRA_ROLE_LABELS.get(code_str, role_name or code_str),
        })

    for u in users:
        role_info = role_map.get(u.id, {})

        approved_position_key = str(getattr(u, "approved_position_code", "") or "").upper().strip()
        if approved_position_key in _POSITION_LABELS:
            role_info = {
                "position_key": approved_position_key,
                "position_label": _POSITION_LABELS.get(approved_position_key, approved_position_key),
            }

        unit_info = unit_map.get(u.id, {})
        main_role_code = ""
        if role_info.get("position_key"):
            main_role = _POSITION_MAP.get(str(role_info.get("position_key") or "").upper())
            main_role_code = _role_code_value(main_role) if main_role else ""

        extra_roles = []
        for item in special_role_map.get(u.id, []):
            if item.get("code") == main_role_code:
                continue
            extra_roles.append(item)

        current_unit_name = unit_info.get("unit_name") or ""
        current_parent_name = unit_info.get("parent_name") or ""
        if unit_info.get("unit_category") == "SUBUNIT" and current_parent_name and current_unit_name:
            current_unit_display = f"{current_parent_name} / {current_unit_name}"
        else:
            current_unit_display = current_unit_name or "-"

        setattr(u, "current_position_key", role_info.get("position_key", ""))
        setattr(u, "current_position_label", role_info.get("position_label", "-"))
        setattr(u, "current_unit_id", unit_info.get("unit_id", ""))
        setattr(u, "current_unit_name", current_unit_display)
        setattr(u, "current_block_code", unit_info.get("block_code", ""))
        setattr(u, "current_unit_category", unit_info.get("unit_category", ""))
        setattr(u, "extra_role_rows", extra_roles)        

        requested_unit = getattr(u, "requested_unit", None)
        requested_subunit = getattr(u, "requested_subunit", None)
        setattr(u, "requested_unit_name", _format_unit_display(requested_unit, requested_subunit))

    return users

# ================== HỒ SƠ CÁ NHÂN ==================
@router.get("/account")
def my_account(request: Request, db: Session = Depends(get_db)):
    user = login_required(request, db)
    return templates.TemplateResponse("account.html", {
        "request": request,
        "app_name": settings.APP_NAME,
        "company_name": settings.COMPANY_NAME,
        "user": user
    })


@router.post("/account/change-password")
def change_password(
    request: Request,
    current_password: str = Form(...),
    new_password: str = Form(...),
    confirm_new_password: str = Form(...),
    db: Session = Depends(get_db),
):
    user = login_required(request, db)

    current_password = (current_password or "").strip()
    new_password = (new_password or "").strip()
    confirm_new_password = (confirm_new_password or "").strip()

    if not current_password or not new_password or not confirm_new_password:
        return templates.TemplateResponse("account.html", {
            "request": request,
            "app_name": settings.APP_NAME,
            "company_name": settings.COMPANY_NAME,
            "user": user,
            "change_password_error": "Vui lòng nhập đầy đủ các trường đổi mật khẩu."
        }, status_code=400)

    if not verify_password(current_password, user.password_hash):
        return templates.TemplateResponse("account.html", {
            "request": request,
            "app_name": settings.APP_NAME,
            "company_name": settings.COMPANY_NAME,
            "user": user,
            "change_password_error": "Mật khẩu hiện tại không đúng."
        }, status_code=400)

    if new_password != confirm_new_password:
        return templates.TemplateResponse("account.html", {
            "request": request,
            "app_name": settings.APP_NAME,
            "company_name": settings.COMPANY_NAME,
            "user": user,
            "change_password_error": "Mật khẩu mới và xác nhận mật khẩu mới không khớp."
        }, status_code=400)

    if current_password == new_password:
        return templates.TemplateResponse("account.html", {
            "request": request,
            "app_name": settings.APP_NAME,
            "company_name": settings.COMPANY_NAME,
            "user": user,
            "change_password_error": "Mật khẩu mới không được trùng mật khẩu hiện tại."
        }, status_code=400)

    user.password_hash = hash_password(new_password)
    db.add(user)
    db.commit()

    return templates.TemplateResponse("account.html", {
        "request": request,
        "app_name": settings.APP_NAME,
        "company_name": settings.COMPANY_NAME,
        "user": user,
        "change_password_success": "Đổi mật khẩu thành công."
    })
    
# ================== QUẢN TRỊ NGƯỜI DÙNG ==================
@router.get("/account/users")
def users_manage(request: Request, db: Session = Depends(get_db)):
    user = login_required(request, db)
    _require_admin_or_leader(user, db)

    pendings = (
        db.query(Users)
        .filter(Users.status == UserStatus.PENDING_APPROVAL)
        .order_by(Users.created_at.desc())
        .all()
    )
    actives = (
        db.query(Users)
        .filter(Users.status == UserStatus.ACTIVE)
        .order_by(Users.created_at.desc())
        .all()
    )
    locked = (
        db.query(Users)
        .filter(Users.status == UserStatus.LOCKED)
        .order_by(Users.created_at.desc())
        .all()
    )
    all_units = (
        db.query(Units)
        .order_by(Units.cap_do.asc(), Units.ten_don_vi.asc())
        .all()
    )
    all_units_payload = [
        {
            "id": un.id,
            "name": un.ten_don_vi,
            "cap_do": int(getattr(un, "cap_do", 0) or 0),
            "parent_id": getattr(un, "parent_id", None),
            "block_code": _unit_block_value(un),
            "unit_category": _unit_category_value(un),
        }
        for un in all_units
    ]

    actives = _decorate_manage_users(db, actives)
    locked = _decorate_manage_users(db, locked)
    pendings = _decorate_manage_users(db, pendings)
    for u in pendings:
        setattr(u, "requested_position_label", _POSITION_LABELS.get((getattr(u, "requested_position_code", "") or "").upper(), "-"))

    extra_role_options = [
        {
            "code": _role_code_value(role_code),
            "label": _SPECIAL_EXTRA_ROLE_LABELS.get(_role_code_value(role_code), _role_code_value(role_code)),
        }
        for role_code in _SPECIAL_EXTRA_ROLE_CODES
    ]

    return templates.TemplateResponse("users_manage.html", {
        "request": request,
        "app_name": settings.APP_NAME,
        "company_name": settings.COMPANY_NAME,
        "pendings": pendings,
        "actives": actives,
        "locked": locked,
        "all_units": all_units,
        "all_units_payload": all_units_payload,
        "position_defs": POSITION_DEFS,
        "extra_role_options": extra_role_options,
    })


@router.post("/account/users/approve")
async def approve_user(
    request: Request,
    user_id: str = Form(...),
    approved_position_code: str = Form(...),
    approved_unit_id: str = Form(...),
    approved_subunit_id: str = Form(""),
    db: Session = Depends(get_db),
):
    me = login_required(request, db)
    _require_admin_or_leader(me, db)

    u = db.get(Users, user_id)
    if not u:
        raise HTTPException(status_code=404, detail="Không tìm thấy người dùng.")

    pos_key = (approved_position_code or "").strip().upper()
    role_code = _POSITION_MAP.get(pos_key)
    if not role_code:
        raise HTTPException(status_code=400, detail="Vị trí phê duyệt không hợp lệ.")

    approved_unit_id = (approved_unit_id or "").strip()
    approved_subunit_id = (approved_subunit_id or "").strip()

    requested_unit_id = str(getattr(u, "requested_unit_id", "") or "").strip()
    requested_subunit_id = str(getattr(u, "requested_subunit_id", "") or "").strip()

    requires_subunit = pos_key in {
        "TO_TRUONG", "PHO_TO",
        "TRUONG_NHOM", "PHO_NHOM",
        "TRUONG_DON_VI", "PHO_DON_VI",
    }

    # Nếu form không gửi được đơn vị chính thì fallback về đơn vị user đã đăng ký
    if not approved_unit_id:
        approved_unit_id = requested_unit_id

    if not approved_unit_id:
        raise HTTPException(status_code=400, detail="Chưa chọn đơn vị phê duyệt.")

    unit = db.get(Units, approved_unit_id)
    if not unit:
        raise HTTPException(
            status_code=404,
            detail=f"Không tìm thấy đơn vị chính (ID={approved_unit_id})."
        )

    subunit = None

    # Chỉ lấy đơn vị con khi vị trí thực sự cần đơn vị con
    if requires_subunit and not approved_subunit_id:
        approved_subunit_id = requested_subunit_id

    if not requires_subunit:
        approved_subunit_id = ""

    if approved_subunit_id:
        subunit = db.get(Units, approved_subunit_id)
        if not subunit:
            raise HTTPException(status_code=400, detail="Không tìm thấy Tổ/Nhóm/Đơn vị trực thuộc.")
        if str(getattr(subunit, "parent_id", "") or "") != approved_unit_id:
            raise HTTPException(status_code=400, detail="Đơn vị trực thuộc không thuộc đúng đơn vị cha đã chọn.")

    _validate_position_unit_scope(pos_key, unit, subunit)

    primary_unit_id = approved_subunit_id if approved_subunit_id else approved_unit_id

    _set_user_position(db, u, role_code)
    _rebuild_user_memberships_for_position(db, u, role_code, primary_unit_id)

    u.status = UserStatus.ACTIVE
    u.approved_block_code = getattr(u, "requested_block_code", None)
    u.approved_unit_id = approved_unit_id
    u.approved_subunit_id = approved_subunit_id or None
    u.approved_position_code = pos_key
    u.approved_by_user_id = me.id
    u.approved_at = func.now()

    db.add(u)
    db.commit()
    await _notify_users_manage_changed(db, actor_user_id=me.id, changed_user_id=u.id, action="approve")
    return RedirectResponse(url="/account/users", status_code=302)

# ----- KÍCH HOẠT / KHOÁ / MỞ / XOÁ -----

@router.post("/account/users/activate")
async def activate_user(request: Request, user_id: str = Form(...), db: Session = Depends(get_db)):
    me = login_required(request, db); _require_admin_or_leader(me, db)
    u = db.get(Users, user_id)
    if not u: raise HTTPException(status_code=404, detail="Không tìm thấy người dùng.")
    u.status = UserStatus.ACTIVE
    db.add(u); db.commit()
    await _notify_users_manage_changed(db, actor_user_id=me.id, changed_user_id=u.id, action="activate")
    return RedirectResponse(url="/account/users", status_code=302)

@router.post("/account/users/lock")
async def lock_user(request: Request, user_id: str = Form(...), db: Session = Depends(get_db)):
    me = login_required(request, db); _require_admin_or_leader(me, db)
    u = db.get(Users, user_id)
    if not u: raise HTTPException(status_code=404, detail="Không tìm thấy người dùng.")
    if u.id == me.id: raise HTTPException(status_code=400, detail="Không thể khóa chính mình.")
    u.status = UserStatus.LOCKED
    db.add(u); db.commit()
    await _notify_users_manage_changed(db, actor_user_id=me.id, changed_user_id=u.id, action="lock")
    return RedirectResponse(url="/account/users", status_code=302)

@router.post("/account/users/unlock")
async def unlock_user(request: Request, user_id: str = Form(...), db: Session = Depends(get_db)):
    me = login_required(request, db); _require_admin_or_leader(me, db)
    u = db.get(Users, user_id)
    if not u: raise HTTPException(status_code=404, detail="Không tìm thấy người dùng.")
    u.status = UserStatus.ACTIVE
    db.add(u); db.commit()
    await _notify_users_manage_changed(db, actor_user_id=me.id, changed_user_id=u.id, action="unlock")
    return RedirectResponse(url="/account/users", status_code=302)

@router.post("/account/users/delete")
async def delete_user(request: Request, user_id: str = Form(...), db: Session = Depends(get_db)):
    me = login_required(request, db); _require_admin_or_leader(me, db)
    u = db.get(Users, user_id)
    if not u: raise HTTPException(status_code=404, detail="Không tìm thấy người dùng.")
    if u.id == me.id: raise HTTPException(status_code=400, detail="Không thể xóa chính mình.")
    if user_has_any_role(u, db, [RoleCode.ROLE_ADMIN, RoleCode.ROLE_LANH_DAO]):
        raise HTTPException(status_code=403, detail="Không được xóa tài khoản có vai trò Admin/Lãnh đạo.")
    # Ràng buộc tối thiểu với Tasks
    ref1 = db.query(Tasks.id).filter(Tasks.created_by == u.id).first()
    assigned_field = getattr(Tasks, "assigned_to_user_id", None)
    ref2 = db.query(Tasks.id).filter(assigned_field == u.id).first() if assigned_field is not None else None
    if ref1 or ref2:
        raise HTTPException(status_code=400, detail="Không thể xóa: còn dữ liệu nhiệm vụ liên quan.")
    deleted_user_id = u.id
    db.delete(u); db.commit()
    await _notify_users_manage_changed(db, actor_user_id=me.id, changed_user_id=deleted_user_id, action="delete")
    return RedirectResponse(url="/account/users", status_code=302)


# ----- VAI TRÒ KIÊM NHIỆM ĐẶC BIỆT -----
@router.post("/account/users/extra-role/add")
async def add_user_extra_role(
    request: Request,
    user_id: str = Form(...),
    extra_role_code: str = Form(...),
    db: Session = Depends(get_db),
):
    me = login_required(request, db)
    _require_admin_or_leader(me, db)

    u = db.get(Users, user_id)
    if not u:
        raise HTTPException(status_code=404, detail="Không tìm thấy người dùng.")

    role_code = _role_code_value(extra_role_code)
    allowed_codes = {_role_code_value(x) for x in _SPECIAL_EXTRA_ROLE_CODES}
    if role_code not in allowed_codes:
        raise HTTPException(status_code=400, detail="Vai trò kiêm nhiệm không hợp lệ.")

    _ensure_user_role_by_code(db, u, role_code)
    _sync_special_role_memberships(db, u)

    db.add(u)
    db.commit()

    await _notify_users_manage_changed(db, actor_user_id=me.id, changed_user_id=u.id, action="extra_role_add")
    return RedirectResponse(url="/account/users", status_code=302)


@router.post("/account/users/extra-role/remove")
async def remove_user_extra_role(
    request: Request,
    user_id: str = Form(...),
    extra_role_code: str = Form(...),
    db: Session = Depends(get_db),
):
    me = login_required(request, db)
    _require_admin_or_leader(me, db)

    u = db.get(Users, user_id)
    if not u:
        raise HTTPException(status_code=404, detail="Không tìm thấy người dùng.")

    role_code = _role_code_value(extra_role_code)
    allowed_codes = {_role_code_value(x) for x in _SPECIAL_EXTRA_ROLE_CODES}
    if role_code not in allowed_codes:
        raise HTTPException(status_code=400, detail="Vai trò kiêm nhiệm không hợp lệ.")

    current_position_key = str(getattr(u, "approved_position_code", "") or "").upper().strip()
    main_role = _POSITION_MAP.get(current_position_key)
    main_role_code = _role_code_value(main_role) if main_role else ""

    if role_code == main_role_code:
        raise HTTPException(status_code=400, detail="Không được gỡ vai trò đang là vị trí chính. Muốn thay đổi phải dùng chức năng Đổi vị trí.")

    _remove_user_role_by_code(db, u, role_code)
    _sync_special_role_memberships(db, u)

    db.add(u)
    db.commit()

    await _notify_users_manage_changed(db, actor_user_id=me.id, changed_user_id=u.id, action="extra_role_remove")
    return RedirectResponse(url="/account/users", status_code=302)


# ----- ĐIỀU CHỈNH VỊ TRÍ -----
@router.post("/account/users/position")
async def set_position(request: Request, user_id: str = Form(...), new_role: str = Form(...), db: Session = Depends(get_db)):
    me = login_required(request, db); _require_admin_or_leader(me, db)
    u = db.get(Users, user_id)
    if not u:
        raise HTTPException(status_code=404, detail="Không tìm thấy người dùng.")

    new_role = (new_role or "").strip().upper()
    role_code = _POSITION_MAP.get(new_role)
    if not role_code:
        raise HTTPException(status_code=400, detail="Giá trị vị trí không hợp lệ.")

    current_primary = (
        db.query(UserUnitMemberships.unit_id)
        .filter(UserUnitMemberships.user_id == u.id, UserUnitMemberships.is_primary == True)  # noqa: E712
        .first()
    )
    if current_primary and current_primary[0]:
        approved_unit, approved_subunit = _resolve_unit_scope(db, current_primary[0])
        _validate_position_unit_scope(new_role, approved_unit, approved_subunit)

    _set_user_position(db, u, role_code)

    if current_primary and current_primary[0]:
        _rebuild_user_memberships_for_position(db, u, role_code, current_primary[0])

    _sync_special_role_memberships(db, u)

    db.commit()
    await _notify_users_manage_changed(db, actor_user_id=me.id, changed_user_id=u.id, action="position")
    return RedirectResponse(url="/account/users", status_code=302)

# ----- ĐIỀU CHUYỂN ĐƠN VỊ -----
@router.post("/account/users/unit-transfer")
async def unit_transfer(
    request: Request,
    user_id: str = Form(...),
    new_unit_id: str = Form(...),
    db: Session = Depends(get_db),
):
    me = login_required(request, db); _require_admin_or_leader(me, db)
    u = db.get(Users, user_id)
    if not u: raise HTTPException(status_code=404, detail="Không tìm thấy người dùng.")
    target_primary_unit = db.get(Units, new_unit_id)
    if not target_primary_unit: raise HTTPException(status_code=404, detail="Không tìm thấy đơn vị.")

    current_role_code = _get_role_code_of_user(db, u)
    position_key = _ROLECODE_TO_POSITION_KEY.get(str(current_role_code), "") if current_role_code else ""
    if not position_key:
        raise HTTPException(status_code=400, detail="Người dùng chưa có vị trí hiện hành để điều chuyển.")

    approved_unit, approved_subunit = _resolve_unit_scope(db, new_unit_id)
    _validate_position_unit_scope(position_key, approved_unit, approved_subunit)

    _transfer_user_unit(db, u, new_unit_id)
    if hasattr(u, "approved_unit_id"):
        u.approved_unit_id = approved_unit.id
    if hasattr(u, "approved_subunit_id"):
        u.approved_subunit_id = approved_subunit.id if approved_subunit else None

    _sync_special_role_memberships(db, u)

    db.add(u); db.commit()
    await _notify_users_manage_changed(db, actor_user_id=me.id, changed_user_id=u.id, action="unit_transfer")
    return RedirectResponse(url="/account/users", status_code=302)

# ----- SỬA THÔNG TIN (EDIT / UPDATE) -----
@router.get("/account/users/edit")
def edit_user_screen(request: Request, user_id: str = Query(...), db: Session = Depends(get_db)):
    me = login_required(request, db); _require_admin_or_leader(me, db)
    u = db.get(Users, user_id)
    if not u: raise HTTPException(status_code=404, detail="Không tìm thấy người dùng.")
    return templates.TemplateResponse("user_edit.html", {"request": request, "u": u})

@router.post("/account/users/update")
async def update_user(
    request: Request,
    user_id: str = Form(...),
    full_name: str = Form(""),
    email: str = Form(""),
    phone: str = Form(""),
    username: str = Form(...),
    db: Session = Depends(get_db),
):
    me = login_required(request, db); _require_admin_or_leader(me, db)
    u = db.get(Users, user_id)
    if not u: raise HTTPException(status_code=404, detail="Không tìm thấy người dùng.")

    full_name = (full_name or "").strip()
    email = (email or "").strip()
    phone = (phone or "").strip()
    username = (username or "").strip()
    if not username:
        raise HTTPException(status_code=400, detail="Tên đăng nhập không được để trống.")

    # Nếu đổi username → kiểm tra trùng
    if username != getattr(u, "username", ""):
        existed = db.query(Users).filter(and_(Users.username == username, Users.id != u.id)).first()
        if existed:
            raise HTTPException(status_code=400, detail="Tên đăng nhập đã tồn tại.")
        u.username = username

    if hasattr(u, "full_name"): u.full_name = full_name
    if hasattr(u, "email"): u.email = email
    if hasattr(u, "phone"): u.phone = phone

    db.add(u); db.commit()
    await _notify_users_manage_changed(db, actor_user_id=me.id, changed_user_id=u.id, action="update")
    return RedirectResponse(url="/account/users", status_code=302)
