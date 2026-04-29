from fastapi import APIRouter, Request, Depends, Form, HTTPException, status
from starlette.responses import RedirectResponse
from starlette.templating import Jinja2Templates
from sqlalchemy.orm import Session
import os, datetime

from ..security.deps import get_db, login_required, user_has_any_role
from ..models import (
    VisibilityGrants,
    VisibilityUserGrants,
    VisibilityMode,
    Units,
    Users,
    UserStatus,
    UserUnitMemberships,
    RoleCode,
    UnitStatus,
)
from ..config import settings

router = APIRouter()
templates = Jinja2Templates(directory=os.path.join(os.path.dirname(__file__), "..", "templates"))
def _load_grantable_units(db: Session) -> list[dict]:
    units = (
        db.query(Units)
        .filter(Units.trang_thai == UnitStatus.ACTIVE)
        .filter(Units.cap_do.in_([2, 3]))
        .order_by(Units.cap_do.asc(), Units.order_index.asc(), Units.ten_don_vi.asc())
        .all()
    )

    parent_ids = {u.parent_id for u in units if getattr(u, "parent_id", None)}
    parent_map = {}
    if parent_ids:
        parents = db.query(Units).filter(Units.id.in_(list(parent_ids))).all()
        parent_map = {p.id: p for p in parents}

    options = []
    for u in units:
        if getattr(u, "cap_do", None) == 2:
            label = f"Phòng: {u.ten_don_vi}"
        elif getattr(u, "cap_do", None) == 3:
            parent_name = ""
            p = parent_map.get(getattr(u, "parent_id", None))
            if p and getattr(p, "ten_don_vi", None):
                parent_name = p.ten_don_vi
            label = f"Tổ: {u.ten_don_vi}"
            if parent_name:
                label += f" (thuộc {parent_name})"
        else:
            continue

        options.append({
            "id": u.id,
            "label": label,
        })

    return options

def _load_grantable_users(db: Session) -> list[dict]:
    users = (
        db.query(Users)
        .filter(Users.status == UserStatus.ACTIVE)
        .order_by(Users.full_name.asc(), Users.username.asc())
        .all()
    )

    rows = (
        db.query(
            UserUnitMemberships.user_id,
            UserUnitMemberships.unit_id,
            UserUnitMemberships.is_primary,
            Units.ten_don_vi,
            Units.cap_do,
        )
        .join(Units, Units.id == UserUnitMemberships.unit_id)
        .all()
    )

    by_user = {}
    for user_id, unit_id, is_primary, ten_don_vi, cap_do in rows:
        by_user.setdefault(user_id, []).append({
            "unit_id": unit_id,
            "is_primary": bool(is_primary),
            "ten_don_vi": ten_don_vi or "",
            "cap_do": cap_do,
        })

    options = []
    for u in users:
        mems = by_user.get(u.id, [])
        primary = next((m for m in mems if m.get("is_primary")), mems[0] if mems else None)
        unit_label = primary["ten_don_vi"] if primary else ""
        full_name = (getattr(u, "full_name", None) or getattr(u, "username", None) or "").strip()
        label = full_name
        if unit_label:
            label += f" - {unit_label}"

        options.append({
            "id": u.id,
            "label": label,
        })

    return options


def _decorate_user_grants(db: Session, grants: list[VisibilityUserGrants]) -> list[dict]:
    user_ids = [g.grantee_user_id for g in grants if getattr(g, "grantee_user_id", None)]
    unit_ids = [g.target_unit_id for g in grants if getattr(g, "target_unit_id", None)]

    user_map = {}
    if user_ids:
        rows = db.query(Users).filter(Users.id.in_(list(set(user_ids)))).all()
        user_map = {u.id: ((getattr(u, "full_name", None) or getattr(u, "username", None) or "").strip()) for u in rows}

    unit_map = {}
    if unit_ids:
        units = db.query(Units).filter(Units.id.in_(list(set(unit_ids)))).all()
        unit_map = {u.id: u for u in units}

    parent_ids = {u.parent_id for u in unit_map.values() if getattr(u, "parent_id", None)}
    parent_map = {}
    if parent_ids:
        parents = db.query(Units).filter(Units.id.in_(list(parent_ids))).all()
        parent_map = {p.id: p for p in parents}

    rows = []
    for g in grants:
        u = unit_map.get(g.target_unit_id)
        if u:
            if getattr(u, "cap_do", None) == 2:
                unit_label = f"Phòng: {u.ten_don_vi}"
            elif getattr(u, "cap_do", None) == 3:
                p = parent_map.get(getattr(u, "parent_id", None))
                parent_name = getattr(p, "ten_don_vi", "") if p else ""
                unit_label = f"Tổ: {u.ten_don_vi}"
                if parent_name:
                    unit_label += f" (thuộc {parent_name})"
            else:
                unit_label = u.ten_don_vi
        else:
            unit_label = g.target_unit_id or "-"

        rows.append({
            "id": g.id,
            "grantee_user_name": user_map.get(g.grantee_user_id, g.grantee_user_id or "-"),
            "target_unit_label": unit_label,
            "mode": g.mode.value if getattr(g, "mode", None) else "-",
            "effective_from": g.effective_from,
            "effective_to": g.effective_to,
        })

    return rows
    
def _decorate_grants(db: Session, grants: list[VisibilityGrants]) -> list[dict]:
    unit_ids = [g.grantee_unit_id for g in grants if getattr(g, "grantee_unit_id", None)]
    unit_map = {}

    if unit_ids:
        units = db.query(Units).filter(Units.id.in_(list(set(unit_ids)))).all()
        unit_map = {u.id: u for u in units}

    parent_ids = {u.parent_id for u in unit_map.values() if getattr(u, "parent_id", None)}
    parent_map = {}
    if parent_ids:
        parents = db.query(Units).filter(Units.id.in_(list(parent_ids))).all()
        parent_map = {p.id: p for p in parents}

    rows = []
    for g in grants:
        u = unit_map.get(g.grantee_unit_id)
        if u:
            if getattr(u, "cap_do", None) == 2:
                unit_label = f"Phòng: {u.ten_don_vi}"
            elif getattr(u, "cap_do", None) == 3:
                parent_name = ""
                p = parent_map.get(getattr(u, "parent_id", None))
                if p and getattr(p, "ten_don_vi", None):
                    parent_name = p.ten_don_vi
                unit_label = f"Tổ: {u.ten_don_vi}"
                if parent_name:
                    unit_label += f" (thuộc {parent_name})"
            else:
                unit_label = u.ten_don_vi
        else:
            unit_label = g.grantee_unit_id or "-"

        rows.append({
            "id": g.id,
            "grantee_unit_id": g.grantee_unit_id,
            "grantee_unit_label": unit_label,
            "mode": g.mode.value if getattr(g, "mode", None) else "-",
            "effective_from": g.effective_from,
            "effective_to": g.effective_to,
        })

    return rows

def _parse_dt(s: str):
    try:
        return datetime.datetime.fromisoformat(s) if s else None
    except Exception:
        return None
        
@router.get("")
def list_grants(request: Request, db: Session = Depends(get_db)):
    user = login_required(request, db)
    if not user_has_any_role(user, db, [RoleCode.ROLE_ADMIN]):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Chỉ Admin được truy cập.")

    grants = db.query(VisibilityGrants).order_by(VisibilityGrants.effective_from.desc(), VisibilityGrants.id.desc()).all()
    grant_rows = _decorate_grants(db, grants)

    user_grants = (
        db.query(VisibilityUserGrants)
        .order_by(VisibilityUserGrants.effective_from.desc(), VisibilityUserGrants.id.desc())
        .all()
    )
    user_grant_rows = _decorate_user_grants(db, user_grants)

    unit_options = _load_grantable_units(db)
    user_options = _load_grantable_users(db)

    return templates.TemplateResponse("grants.html", {
        "request": request,
        "app_name": settings.APP_NAME,
        "company_name": settings.COMPANY_NAME,
        "grants": grant_rows,
        "user_grants": user_grant_rows,
        "unit_options": unit_options,
        "user_options": user_options,
    })

@router.post("/add")
def add_grant(
    request: Request,
    grantee_unit_id: str = Form(...),
    mode: str = Form(...),
    effective_from: str = Form(""),
    effective_to: str = Form(""),
    db: Session = Depends(get_db),
):
    user = login_required(request, db)
    if not user_has_any_role(user, db, [RoleCode.ROLE_ADMIN]):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Chỉ Admin được thao tác.")

    target_unit = db.get(Units, grantee_unit_id)
    if not target_unit:
        raise HTTPException(status_code=404, detail="Không tìm thấy đơn vị được cấp quyền.")
    if getattr(target_unit, "trang_thai", None) != UnitStatus.ACTIVE:
        raise HTTPException(status_code=400, detail="Đơn vị được cấp quyền không còn hoạt động.")
    if getattr(target_unit, "cap_do", None) not in (2, 3):
        raise HTTPException(status_code=400, detail="Chỉ cho cấp quyền cho đơn vị cấp Phòng hoặc Tổ.")

    mode = (mode or "").strip().upper()
    allowed_modes = {"VIEW_ALL", "FILES_ONLY", "PLANS_ONLY", "EVALUATION_ONLY"}
    if mode not in allowed_modes:
        raise HTTPException(status_code=400, detail="Loại quyền không hợp lệ.")

    dt_from = _parse_dt(effective_from)
    dt_to = _parse_dt(effective_to)

    if dt_from and dt_to and dt_to < dt_from:
        raise HTTPException(status_code=400, detail="Ngày hiệu lực đến không được nhỏ hơn ngày hiệu lực từ.")

    existing = (
        db.query(VisibilityGrants)
        .filter(VisibilityGrants.grantee_unit_id == grantee_unit_id)
        .filter(VisibilityGrants.mode == VisibilityMode(mode))
        .filter(VisibilityGrants.effective_from == dt_from)
        .filter(VisibilityGrants.effective_to == dt_to)
        .first()
    )
    if existing:
        return RedirectResponse(url="/grants", status_code=302)

    g = VisibilityGrants(
        grantee_unit_id=grantee_unit_id,
        mode=VisibilityMode(mode),
        effective_from=dt_from,
        effective_to=dt_to,
    )
    db.add(g)
    db.commit()
    return RedirectResponse(url="/grants", status_code=302)

@router.post("/add-user")
def add_user_grant(
    request: Request,
    grantee_user_id: str = Form(...),
    target_unit_id: str = Form(...),
    mode: str = Form(...),
    effective_from: str = Form(""),
    effective_to: str = Form(""),
    db: Session = Depends(get_db),
):
    user = login_required(request, db)
    if not user_has_any_role(user, db, [RoleCode.ROLE_ADMIN]):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Chỉ Admin được thao tác.")

    target_user = db.get(Users, grantee_user_id)
    if not target_user:
        raise HTTPException(status_code=404, detail="Không tìm thấy user được cấp quyền.")
    if getattr(target_user, "status", None) != UserStatus.ACTIVE:
        raise HTTPException(status_code=400, detail="User được cấp quyền không còn hoạt động.")

    target_unit = db.get(Units, target_unit_id)
    if not target_unit:
        raise HTTPException(status_code=404, detail="Không tìm thấy đơn vị/phạm vi được cấp quyền.")
    if getattr(target_unit, "trang_thai", None) != UnitStatus.ACTIVE:
        raise HTTPException(status_code=400, detail="Đơn vị/phạm vi không còn hoạt động.")
    if getattr(target_unit, "cap_do", None) not in (2, 3):
        raise HTTPException(status_code=400, detail="Chỉ cho cấp quyền trong phạm vi Phòng hoặc Tổ.")

    mode = (mode or "").strip().upper()
    allowed_modes = {"VIEW_ALL", "FILES_ONLY", "PLANS_ONLY", "EVALUATION_ONLY"}
    if mode not in allowed_modes:
        raise HTTPException(status_code=400, detail="Loại quyền không hợp lệ.")

    dt_from = _parse_dt(effective_from)
    dt_to = _parse_dt(effective_to)

    if dt_from and dt_to and dt_to < dt_from:
        raise HTTPException(status_code=400, detail="Ngày hiệu lực đến không được nhỏ hơn ngày hiệu lực từ.")

    existing = (
        db.query(VisibilityUserGrants)
        .filter(VisibilityUserGrants.grantee_user_id == grantee_user_id)
        .filter(VisibilityUserGrants.target_unit_id == target_unit_id)
        .filter(VisibilityUserGrants.mode == VisibilityMode(mode))
        .filter(VisibilityUserGrants.effective_from == dt_from)
        .filter(VisibilityUserGrants.effective_to == dt_to)
        .first()
    )
    if existing:
        return RedirectResponse(url="/grants", status_code=302)

    g = VisibilityUserGrants(
        grantee_user_id=grantee_user_id,
        target_unit_id=target_unit_id,
        mode=VisibilityMode(mode),
        effective_from=dt_from,
        effective_to=dt_to,
    )
    db.add(g)
    db.commit()
    return RedirectResponse(url="/grants", status_code=302)


@router.post("/delete-user")
def delete_user_grant(request: Request, grant_id: str = Form(...), db: Session = Depends(get_db)):
    user = login_required(request, db)
    if not user_has_any_role(user, db, [RoleCode.ROLE_ADMIN]):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Chỉ Admin được thao tác.")

    g = db.get(VisibilityUserGrants, grant_id)
    if g:
        db.delete(g)
        db.commit()

    return RedirectResponse(url="/grants", status_code=302)

@router.post("/delete")
def delete_grant(request: Request, grant_id: str = Form(...), db: Session = Depends(get_db)):
    user = login_required(request, db)
    if not user_has_any_role(user, db, [RoleCode.ROLE_ADMIN]):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Chỉ Admin được thao tác.")
    g = db.get(VisibilityGrants, grant_id)
    if g:
        db.delete(g); db.commit()
    return RedirectResponse(url="/grants", status_code=302)
