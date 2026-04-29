from fastapi import APIRouter, Request, Depends, Form, HTTPException, status
from sqlalchemy.orm import Session
from sqlalchemy import func
from starlette.responses import RedirectResponse, JSONResponse, StreamingResponse
from starlette.templating import Jinja2Templates
import os, re, json, asyncio

from ..security.deps import get_db, login_required
from ..security.scope import accessible_unit_ids, is_all_units_access
from ..models import (
    BlockCode,
    UnitCategory,
    Units,
    UnitStatus,
    Roles,
    RoleCode,
    UserUnitMemberships,
    UserRoles,
)
from ..config import settings
from ..chat.realtime import manager

router = APIRouter()
templates = Jinja2Templates(directory=os.path.join(os.path.dirname(__file__), "..", "templates"))

_register_unit_watchers: set[asyncio.Queue] = set()


def build_path(parent: Units | None, name: str) -> str:
    def slugify(s: str) -> str:
        s = s.strip().lower()
        s = re.sub(r"[^a-z0-9]+", "-", s)
        return s.strip("-")

    base = "/org"
    if parent:
        return f"{parent.path}/{slugify(name)}"
    return f"{base}/{slugify(name)}"


def _rebuild_descendant_paths(db: Session, parent_unit: Units) -> None:
    children = (
        db.query(Units)
        .filter(Units.parent_id == parent_unit.id)
        .order_by(Units.cap_do, Units.order_index, Units.ten_don_vi)
        .all()
    )

    for child in children:
        child.path = build_path(parent_unit, child.ten_don_vi)
        db.add(child)
        _rebuild_descendant_paths(db, child)


def _has_active_children(db: Session, unit_id: str) -> bool:
    return (
        db.query(Units)
        .filter(Units.parent_id == unit_id, Units.trang_thai == UnitStatus.ACTIVE)
        .first()
        is not None
    )


def _load_role_codes_for_user(db: Session, user) -> set[str]:
    rows = (
        db.query(Roles.code)
        .join(UserRoles, UserRoles.role_id == Roles.id)
        .filter(UserRoles.user_id == user.id)
        .all()
    )
    codes = set()
    for (c,) in rows:
        if c is None:
            continue
        codes.add(str(getattr(c, "value", c)).upper())
    return codes


def _is_admin_or_leader_codes(role_codes: set[str]) -> bool:
    return ("ROLE_ADMIN" in role_codes) or ("ROLE_LANH_DAO" in role_codes)


def _is_truong_phong_codes(role_codes: set[str]) -> bool:
    return "ROLE_TRUONG_PHONG" in role_codes


def _is_pho_phong_codes(role_codes: set[str]) -> bool:
    return "ROLE_PHO_PHONG" in role_codes


def _can_create_unit(role_codes: set[str]) -> bool:
    return (
        _is_admin_or_leader_codes(role_codes)
        or _is_truong_phong_codes(role_codes)
        or _is_pho_phong_codes(role_codes)
        or _is_truong_khoa_codes(role_codes)
    )

def _is_truong_khoa_codes(role_codes: set[str]) -> bool:
    return "ROLE_TRUONG_KHOA" in role_codes

def _membership_units_of_user(db: Session, user_id: str) -> list[Units]:
    rows = (
        db.query(Units)
        .join(UserUnitMemberships, UserUnitMemberships.unit_id == Units.id)
        .filter(UserUnitMemberships.user_id == user_id)
        .filter(Units.trang_thai == UnitStatus.ACTIVE)
        .order_by(Units.cap_do.asc(), Units.order_index.asc(), Units.ten_don_vi.asc())
        .all()
    )

    dedup = {}
    for u in rows:
        if u and getattr(u, "id", None):
            dedup[u.id] = u
    return list(dedup.values())


def _room_scope_ids_for_truong_phong(db: Session, user_id: str) -> set[str]:
    """
    Trưởng/Phó phòng:
    - thấy Phòng của mình
    - thấy các Tổ/Nhóm hành chính trực thuộc Phòng của mình
    """
    units = _membership_units_of_user(db, user_id)

    phong_ids = set()
    for u in units:
        if getattr(u, "unit_category", None) == UnitCategory.PHONG:
            phong_ids.add(u.id)
        elif (
            getattr(u, "unit_category", None) == UnitCategory.SUBUNIT
            and getattr(u, "block_code", None) == BlockCode.HANH_CHINH
            and getattr(u, "parent_id", None)
        ):
            phong_ids.add(u.parent_id)

    if not phong_ids:
        return set()

    child_ids = {
        r[0]
        for r in db.query(Units.id)
        .filter(
            Units.parent_id.in_(list(phong_ids)),
            Units.trang_thai == UnitStatus.ACTIVE,
            Units.unit_category == UnitCategory.SUBUNIT,
            Units.block_code == BlockCode.HANH_CHINH,
        )
        .all()
    }

    return set(phong_ids) | set(child_ids)


def _allowed_room_ids_for_creator(db: Session, user, role_codes: set[str]) -> set[str]:
    """
    Phòng được phép dùng làm đơn vị cha khi tạo Tổ/Nhóm hành chính.
    - Admin/Lãnh đạo: toàn bộ PHONG ACTIVE
    - Trưởng/Phó phòng: chỉ PHONG của mình
    """
    if _is_admin_or_leader_codes(role_codes):
        return {
            r[0]
            for r in db.query(Units.id)
            .filter(
                Units.trang_thai == UnitStatus.ACTIVE,
                Units.unit_category == UnitCategory.PHONG,
                Units.block_code == BlockCode.HANH_CHINH,
            )
            .all()
        }

    if _is_truong_phong_codes(role_codes) or _is_pho_phong_codes(role_codes):
        room_ids = set()
        for u in _membership_units_of_user(db, user.id):
            if getattr(u, "unit_category", None) == UnitCategory.PHONG:
                room_ids.add(u.id)
            elif (
                getattr(u, "unit_category", None) == UnitCategory.SUBUNIT
                and getattr(u, "block_code", None) == BlockCode.HANH_CHINH
                and getattr(u, "parent_id", None)
            ):
                room_ids.add(u.parent_id)
        return room_ids

    return set()


def _allowed_khoa_ids_for_creator(db: Session, user, role_codes: set[str]) -> set[str]:
    """
    Khoa được phép dùng làm đơn vị cha khi tạo Nhóm thuộc Khoa.
    - Admin/Lãnh đạo: toàn bộ KHOA ACTIVE
    - Trưởng khoa: chỉ Khoa của mình
    """
    if _is_admin_or_leader_codes(role_codes):
        return {
            r[0]
            for r in db.query(Units.id)
            .filter(
                Units.trang_thai == UnitStatus.ACTIVE,
                Units.unit_category == UnitCategory.KHOA,
                Units.block_code == BlockCode.CHUYEN_MON,
            )
            .all()
        }

    if _is_truong_khoa_codes(role_codes):
        khoa_ids = set()
        for u in _membership_units_of_user(db, user.id):
            if (
                getattr(u, "unit_category", None) == UnitCategory.KHOA
                and getattr(u, "block_code", None) == BlockCode.CHUYEN_MON
            ):
                khoa_ids.add(u.id)
            elif (
                getattr(u, "unit_category", None) == UnitCategory.SUBUNIT
                and getattr(u, "block_code", None) == BlockCode.CHUYEN_MON
                and getattr(u, "parent_id", None)
            ):
                khoa_ids.add(u.parent_id)
        return khoa_ids

    return set()

def _visible_unit_ids_for_units_tab(db: Session, user, role_codes: set[str]) -> set[str]:
    """
    Quy tắc:
    - Admin/HĐTV: thấy toàn bộ
    - Trưởng phòng: thấy phòng mình + các tổ/nhóm hành chính trực thuộc
    - Phó phòng trở xuống: chỉ thấy các đơn vị mình đang thuộc
    """
    if _is_admin_or_leader_codes(role_codes):
        return {
            r[0]
            for r in db.query(Units.id)
            .filter(Units.trang_thai == UnitStatus.ACTIVE)
            .all()
        }

    if _is_truong_phong_codes(role_codes):
        return _room_scope_ids_for_truong_phong(db, user.id)

    return {u.id for u in _membership_units_of_user(db, user.id) if getattr(u, "id", None)}


def _can_manage_unit_row(db: Session, user, role_codes: set[str], unit_obj: Units) -> bool:
    """
    Quyền thao tác Đổi / Thu hồi:
    - Admin / HĐTV: toàn quyền
    - Trưởng phòng:
        + KHÔNG được đổi tên / thu hồi chính Phòng của mình
        + VẪN được thao tác với các Tổ/Nhóm hành chính trực thuộc Phòng của mình
    - Phó phòng trở xuống: không có quyền
    """
    if not unit_obj:
        return False

    if _is_admin_or_leader_codes(role_codes):
        return True

    if _is_truong_phong_codes(role_codes):
        if unit_obj.id not in _room_scope_ids_for_truong_phong(db, user.id):
            return False

        if (
            getattr(unit_obj, "unit_category", None) == UnitCategory.PHONG
            and getattr(unit_obj, "block_code", None) == BlockCode.HANH_CHINH
        ):
            return False

        return True

    return False


def _root_options_for_form(db: Session, user) -> list[Units]:
    """
    Đơn vị cha cấp gốc: HĐTV.
    Dùng cho:
    - tạo BGĐ
    - tạo Phòng
    """
    q = db.query(Units).filter(
        Units.trang_thai == UnitStatus.ACTIVE,
        Units.cap_do == 1,
        Units.unit_category == UnitCategory.ROOT,
    )

    if not is_all_units_access(db, user):
        ids = accessible_unit_ids(db, user)
        if not ids:
            return []
        q = q.filter(Units.id.in_(list(ids)))

    return q.order_by(Units.order_index, Units.ten_don_vi).all()


def _executive_options_for_form(db: Session, user) -> list[Units]:
    """
    BGĐ - dùng làm cha của Khoa.
    """
    q = db.query(Units).filter(
        Units.trang_thai == UnitStatus.ACTIVE,
        Units.unit_category == UnitCategory.EXECUTIVE,
    )

    if not is_all_units_access(db, user):
        ids = accessible_unit_ids(db, user)
        if not ids:
            return []
        q = q.filter(Units.id.in_(list(ids)))

    return q.order_by(Units.order_index, Units.ten_don_vi).all()


def _phong_options_for_form(db: Session, user, role_codes: set[str]) -> list[Units]:
    """
    Phòng - dùng làm cha của Tổ/Nhóm hành chính.
    """
    if _is_admin_or_leader_codes(role_codes):
        return (
            db.query(Units)
            .filter(
                Units.trang_thai == UnitStatus.ACTIVE,
                Units.unit_category == UnitCategory.PHONG,
                Units.block_code == BlockCode.HANH_CHINH,
            )
            .order_by(Units.order_index, Units.ten_don_vi)
            .all()
        )

    allowed_room_ids = _allowed_room_ids_for_creator(db, user, role_codes)
    if not allowed_room_ids:
        return []

    return (
        db.query(Units)
        .filter(
            Units.trang_thai == UnitStatus.ACTIVE,
            Units.unit_category == UnitCategory.PHONG,
            Units.block_code == BlockCode.HANH_CHINH,
            Units.id.in_(list(allowed_room_ids)),
        )
        .order_by(Units.order_index, Units.ten_don_vi)
        .all()
    )


def _khoa_options_for_form(db: Session, user, role_codes: set[str]) -> list[Units]:
    """
    Khoa - dùng làm cha của đơn vị thuộc Khoa.
    - Admin/Lãnh đạo: thấy toàn bộ Khoa
    - Trưởng khoa: chỉ thấy Khoa của mình
    """
    if _is_admin_or_leader_codes(role_codes):
        q = db.query(Units).filter(
            Units.trang_thai == UnitStatus.ACTIVE,
            Units.unit_category == UnitCategory.KHOA,
            Units.block_code == BlockCode.CHUYEN_MON,
        )

        if not is_all_units_access(db, user):
            ids = accessible_unit_ids(db, user)
            if not ids:
                return []
            q = q.filter(Units.id.in_(list(ids)))

        return q.order_by(Units.order_index, Units.ten_don_vi).all()

    allowed_khoa_ids = _allowed_khoa_ids_for_creator(db, user, role_codes)
    if not allowed_khoa_ids:
        return []

    return (
        db.query(Units)
        .filter(
            Units.trang_thai == UnitStatus.ACTIVE,
            Units.unit_category == UnitCategory.KHOA,
            Units.block_code == BlockCode.CHUYEN_MON,
            Units.id.in_(list(allowed_khoa_ids)),
        )
        .order_by(Units.order_index, Units.ten_don_vi)
        .all()
    )


def _build_register_public_options(db: Session) -> dict:
    level1_units = [
        {
            "id": u.id,
            "name": u.ten_don_vi,
            "block_code": str(getattr(getattr(u, "block_code", None), "value", getattr(u, "block_code", "")) or ""),
            "unit_category": str(getattr(getattr(u, "unit_category", None), "value", getattr(u, "unit_category", "")) or ""),
        }
        for u in db.query(Units)
        .filter(
            Units.trang_thai == UnitStatus.ACTIVE,
            Units.unit_category.in_([UnitCategory.PHONG, UnitCategory.KHOA]),
        )
        .order_by(Units.order_index.asc(), Units.ten_don_vi.asc())
        .all()
    ]

    level2_units = [
        {
            "id": u.id,
            "name": u.ten_don_vi,
            "parent_id": u.parent_id,
            "block_code": str(getattr(getattr(u, "block_code", None), "value", getattr(u, "block_code", "")) or ""),
            "unit_category": str(getattr(getattr(u, "unit_category", None), "value", getattr(u, "unit_category", "")) or ""),
        }
        for u in db.query(Units)
        .filter(
            Units.trang_thai == UnitStatus.ACTIVE,
            Units.unit_category == UnitCategory.SUBUNIT,
        )
        .order_by(Units.order_index.asc(), Units.ten_don_vi.asc())
        .all()
    ]

    return {
        "level1_units": level1_units,
        "level2_units": level2_units,
    }


async def _notify_register_units_changed(db: Session, action: str, unit_id: str | None = None) -> None:
    payload = {
        "type": "units_changed",
        "action": action,
        "unit_id": str(unit_id or ""),
        **_build_register_public_options(db),
    }

    for q in list(_register_unit_watchers):
        try:
            q.put_nowait(payload)
        except Exception:
            _register_unit_watchers.discard(q)

    try:
        await manager.notify_users_json([], payload)
    except Exception:
        pass


@router.get('/options-public')
def register_unit_options_public(db: Session = Depends(get_db)):
    return JSONResponse(_build_register_public_options(db))


@router.get('/events')
async def register_unit_events(request: Request):
    queue: asyncio.Queue = asyncio.Queue()
    _register_unit_watchers.add(queue)

    async def event_stream():
        try:
            yield ': connected\n\n'
            while True:
                if await request.is_disconnected():
                    break
                try:
                    payload = await asyncio.wait_for(queue.get(), timeout=25)
                    yield f"event: units_changed\ndata: {json.dumps(payload, ensure_ascii=False)}\n\n"
                except asyncio.TimeoutError:
                    yield ': keepalive\n\n'
        finally:
            _register_unit_watchers.discard(queue)

    return StreamingResponse(
        event_stream(),
        media_type='text/event-stream',
        headers={
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive',
            'X-Accel-Buffering': 'no',
        },
    )

def _unit_type_label(u: Units) -> str:
    cat = getattr(u, "unit_category", None)
    block = getattr(u, "block_code", None)

    if cat == UnitCategory.ROOT:
        return "HĐTV"
    if cat == UnitCategory.EXECUTIVE:
        return "BGĐ"
    if cat == UnitCategory.PHONG:
        return "Phòng"
    if cat == UnitCategory.KHOA:
        return "Khoa"
    if cat == UnitCategory.SUBUNIT and block == BlockCode.HANH_CHINH:
        return "Tổ / Nhóm"
    if cat == UnitCategory.SUBUNIT and block == BlockCode.CHUYEN_MON:
        return "Đơn vị thuộc Khoa"
    return f"Cấp {getattr(u, 'cap_do', '')}"


@router.get("")
def list_units(request: Request, db: Session = Depends(get_db)):
    user = login_required(request, db)
    role_codes = _load_role_codes_for_user(db, user)

    visible_ids = _visible_unit_ids_for_units_tab(db, user, role_codes)

    if not visible_ids:
        units = []
    else:
        units = (
            db.query(Units)
            .filter(Units.trang_thai == UnitStatus.ACTIVE, Units.id.in_(list(visible_ids)))
            .order_by(Units.cap_do, Units.order_index, Units.ten_don_vi)
            .all()
        )

    parent_ids = {u.parent_id for u in units if getattr(u, "parent_id", None)}
    parent_map = {}
    if parent_ids:
        parents = db.query(Units).filter(Units.id.in_(list(parent_ids))).all()
        parent_map = {p.id: p for p in parents}

    unit_rows = []
    for u in units:
        parent_name = ""
        if getattr(u, "parent_id", None):
            p = parent_map.get(u.parent_id)
            if p and getattr(p, "ten_don_vi", None):
                parent_name = p.ten_don_vi

        unit_rows.append(
            {
                "id": u.id,
                "ten_don_vi": u.ten_don_vi,
                "cap_do": u.cap_do,
                "unit_type_label": _unit_type_label(u),
                "parent_id": u.parent_id,
                "parent_name": parent_name,
                "path": u.path,
                "trang_thai": u.trang_thai.value if getattr(u, "trang_thai", None) else "",
                "can_manage": _can_manage_unit_row(db, user, role_codes, u),
            }
        )

    can_create = _can_create_unit(role_codes)

    if _is_admin_or_leader_codes(role_codes):
        create_scope = "ALL"
    elif _is_truong_phong_codes(role_codes) or _is_pho_phong_codes(role_codes):
        create_scope = "TEAM_ONLY"
    elif _is_truong_khoa_codes(role_codes):
        create_scope = "KHOA_TEAM_ONLY"
    else:
        create_scope = "NONE"

    root_options = _root_options_for_form(db, user) if can_create else []
    executive_options = _executive_options_for_form(db, user) if _is_admin_or_leader_codes(role_codes) else []
    phong_options = _phong_options_for_form(db, user, role_codes) if can_create else []
    khoa_options = _khoa_options_for_form(db, user, role_codes) if can_create else []

    return templates.TemplateResponse(
        "units.html",
        {
            "request": request,
            "app_name": settings.APP_NAME,
            "company_name": settings.COMPANY_NAME,
            "units": unit_rows,
            "root_options": root_options,
            "executive_options": executive_options,
            "phong_options": phong_options,
            "khoa_options": khoa_options,
            "can_create": can_create,
            "create_scope": create_scope,
        },
    )


@router.post("/create")
async def create_unit(
    request: Request,
    ten_don_vi: str = Form(...),
    unit_kind: str = Form(...),
    parent_id: str | None = Form(None),
    db: Session = Depends(get_db),
):
    user = login_required(request, db)
    role_codes = _load_role_codes_for_user(db, user)

    if not _can_create_unit(role_codes):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Bạn không có quyền thêm đơn vị.")

    ten_don_vi = (ten_don_vi or "").strip()
    unit_kind = (unit_kind or "").strip().upper()

    if not ten_don_vi:
        raise HTTPException(status_code=400, detail="Tên đơn vị không được để trống.")

    parent = db.get(Units, parent_id) if parent_id else None

    # Trưởng/Phó phòng chỉ được thêm Tổ/Nhóm hành chính thuộc phòng của mình
    if (_is_truong_phong_codes(role_codes) or _is_pho_phong_codes(role_codes)) and not _is_admin_or_leader_codes(role_codes):
        if unit_kind != "HANH_CHINH_SUBUNIT":
            raise HTTPException(status_code=403, detail="Bạn chỉ được thêm Tổ/Nhóm thuộc Phòng của mình.")
        if not parent:
            raise HTTPException(status_code=400, detail="Tổ/Nhóm bắt buộc phải chọn Phòng cha.")
        allowed_room_ids = _allowed_room_ids_for_creator(db, user, role_codes)
        if parent.id not in allowed_room_ids:
            raise HTTPException(status_code=403, detail="Bạn chỉ được thêm Tổ/Nhóm thuộc Phòng của mình.")
    
    # Trưởng khoa chỉ được thêm Nhóm thuộc Khoa của mình
    if _is_truong_khoa_codes(role_codes) and not _is_admin_or_leader_codes(role_codes):
        if unit_kind != "CHUYEN_MON_SUBUNIT":
            raise HTTPException(status_code=403, detail="Bạn chỉ được thêm Nhóm thuộc Khoa của mình.")
        if not parent:
            raise HTTPException(status_code=400, detail="Nhóm thuộc Khoa bắt buộc phải chọn Khoa cha.")
        allowed_khoa_ids = _allowed_khoa_ids_for_creator(db, user, role_codes)
        if parent.id not in allowed_khoa_ids:
            raise HTTPException(status_code=403, detail="Bạn chỉ được thêm Nhóm thuộc Khoa của mình.")
    
    # Map loại đơn vị -> cap_do / block / category / rule cha
    if unit_kind == "EXECUTIVE":
        cap_do = 2
        real_block = BlockCode.CHUYEN_MON
        real_category = UnitCategory.EXECUTIVE
        if not parent:
            raise HTTPException(status_code=400, detail="BGĐ bắt buộc phải chọn đơn vị cha là HĐTV.")
        if getattr(parent, "unit_category", None) != UnitCategory.ROOT:
            raise HTTPException(status_code=400, detail="Đơn vị cha của BGĐ phải là HĐTV.")

    elif unit_kind == "PHONG":
        cap_do = 2
        real_block = BlockCode.HANH_CHINH
        real_category = UnitCategory.PHONG
        if not parent:
            raise HTTPException(status_code=400, detail="Phòng bắt buộc phải chọn đơn vị cha là HĐTV.")
        if getattr(parent, "unit_category", None) != UnitCategory.ROOT:
            raise HTTPException(status_code=400, detail="Đơn vị cha của Phòng phải là HĐTV.")

    elif unit_kind == "HANH_CHINH_SUBUNIT":
        cap_do = 3
        real_block = BlockCode.HANH_CHINH
        real_category = UnitCategory.SUBUNIT
        if not parent:
            raise HTTPException(status_code=400, detail="Tổ/Nhóm bắt buộc phải chọn đơn vị cha là Phòng.")
        if getattr(parent, "unit_category", None) != UnitCategory.PHONG:
            raise HTTPException(status_code=400, detail="Đơn vị cha của Tổ/Nhóm phải là Phòng.")
        if getattr(parent, "block_code", None) != BlockCode.HANH_CHINH:
            raise HTTPException(status_code=400, detail="Tổ/Nhóm chỉ được thuộc khối hành chính.")

    elif unit_kind == "KHOA":
        cap_do = 3
        real_block = BlockCode.CHUYEN_MON
        real_category = UnitCategory.KHOA
        if not parent:
            raise HTTPException(status_code=400, detail="Khoa bắt buộc phải chọn đơn vị cha là BGĐ.")
        if getattr(parent, "unit_category", None) != UnitCategory.EXECUTIVE:
            raise HTTPException(status_code=400, detail="Đơn vị cha của Khoa phải là BGĐ.")

    elif unit_kind == "CHUYEN_MON_SUBUNIT":
        cap_do = 4
        real_block = BlockCode.CHUYEN_MON
        real_category = UnitCategory.SUBUNIT
        if not parent:
            raise HTTPException(status_code=400, detail="Đơn vị thuộc Khoa bắt buộc phải chọn đơn vị cha là Khoa.")
        if getattr(parent, "unit_category", None) != UnitCategory.KHOA:
            raise HTTPException(status_code=400, detail="Đơn vị cha phải là Khoa.")
        if getattr(parent, "block_code", None) != BlockCode.CHUYEN_MON:
            raise HTTPException(status_code=400, detail="Đơn vị này chỉ được thuộc khối chuyên môn.")

    else:
        raise HTTPException(status_code=400, detail="Loại đơn vị không hợp lệ.")

    existed = (
        db.query(Units)
        .filter(
            Units.trang_thai == UnitStatus.ACTIVE,
            Units.parent_id == (parent.id if parent else None),
            func.lower(func.trim(Units.ten_don_vi)) == ten_don_vi.lower(),
        )
        .first()
    )
    if existed:
        raise HTTPException(status_code=400, detail="Đơn vị này đã tồn tại trong cùng phạm vi cha.")

    path = build_path(parent, ten_don_vi)

    u = Units(
        ten_don_vi=ten_don_vi,
        cap_do=cap_do,
        parent_id=(parent.id if parent else None),
        path=path,
        block_code=real_block,
        unit_category=real_category,
    )
    db.add(u)
    db.commit()
    await _notify_register_units_changed(db, action="create", unit_id=u.id)
    return RedirectResponse(url="/units", status_code=302)


@router.post("/rename")
async def rename_unit(
    request: Request,
    unit_id: str = Form(...),
    ten_don_vi_moi: str = Form(...),
    db: Session = Depends(get_db),
):
    user = login_required(request, db)
    role_codes = _load_role_codes_for_user(db, user)

    u = db.get(Units, unit_id)
    if not u:
        return RedirectResponse(url="/units", status_code=302)

    if not _can_manage_unit_row(db, user, role_codes, u):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Bạn không có quyền đổi tên đơn vị này.")

    ten_don_vi_moi = (ten_don_vi_moi or "").strip()
    if not ten_don_vi_moi:
        raise HTTPException(status_code=400, detail="Tên đơn vị mới không được để trống.")

    parent = db.get(Units, u.parent_id) if u.parent_id else None

    u.ten_don_vi = ten_don_vi_moi
    u.path = build_path(parent, ten_don_vi_moi)
    db.add(u)
    _rebuild_descendant_paths(db, u)

    db.commit()
    await _notify_register_units_changed(db, action="rename", unit_id=u.id)
    return RedirectResponse(url="/units", status_code=302)


@router.post("/retire")
async def retire_unit(
    request: Request,
    unit_id: str = Form(...),
    reason: str = Form(""),
    db: Session = Depends(get_db),
):
    user = login_required(request, db)
    role_codes = _load_role_codes_for_user(db, user)

    u = db.get(Units, unit_id)
    if not u:
        return RedirectResponse(url="/units", status_code=302)

    if not _can_manage_unit_row(db, user, role_codes, u):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Bạn không có quyền thu hồi đơn vị này.")

    reason = (reason or "").strip()

    if _has_active_children(db, u.id):
        raise HTTPException(
            status_code=400,
            detail="Không thể thu hồi đơn vị này vì còn đơn vị con đang hoạt động.",
        )

    u.trang_thai = UnitStatus.RETIRED
    db.add(u)
    db.commit()
    return RedirectResponse(url="/units", status_code=302)