from __future__ import annotations
from sqlalchemy.orm import Session
from datetime import datetime
from typing import Set, List

from ..models import (
    Users, Units, UnitStatus,
    UserRoles, Roles, RoleCode, ScopeCode,
    UserUnitMemberships, VisibilityGrants, VisibilityMode
)

def _now_utc():
    return datetime.utcnow()

def user_role_codes(db: Session, user: Users) -> Set[RoleCode]:
    urs = db.query(UserRoles).filter(UserRoles.user_id == user.id).all()
    codes: Set[RoleCode] = set()
    for ur in urs:
        r = db.get(Roles, ur.role_id)
        if r:
            codes.add(r.code)
    return codes

def user_scopes(db: Session, user: Users) -> Set[ScopeCode]:
    urs = db.query(UserRoles).filter(UserRoles.user_id == user.id).all()
    return {ur.scope_code for ur in urs if ur.scope_code}

def user_primary_units(db: Session, user: Users) -> List[Units]:
    mems = db.query(UserUnitMemberships).filter(UserUnitMemberships.user_id == user.id).all()
    prims = [m for m in mems if getattr(m, "is_primary", True)]
    ids = [m.unit_id for m in (prims or mems)]
    if not ids:
        return []
    return db.query(Units).filter(Units.id.in_(ids)).all()

def has_view_all_grant(db: Session, user: Users) -> bool:
    prim_units = user_primary_units(db, user)
    if not prim_units:
        return False
    prim_ids = [u.id for u in prim_units]
    now = _now_utc()
    grants = db.query(VisibilityGrants).filter(
        VisibilityGrants.grantee_unit_id.in_(prim_ids),
        VisibilityGrants.mode == VisibilityMode.VIEW_ALL
    ).all()
    for g in grants:
        if (g.effective_from is None or g.effective_from <= now) and (g.effective_to is None or g.effective_to >= now):
            return True
    return False

def is_all_units_access(db: Session, user: Users) -> bool:
    codes = user_role_codes(db, user)
    if RoleCode.ROLE_ADMIN in codes:
        return True
    scopes = user_scopes(db, user)
    if ScopeCode.ALL_UNITS in scopes:
        return True
    if has_view_all_grant(db, user):
        return True
    if RoleCode.ROLE_LANH_DAO in codes:
        # Lãnh đạo mặc định xem hết
        return True
    return False

def _units_under_paths(db: Session, paths: List[str]) -> List[Units]:
    res: dict[str, Units] = {}
    for p in paths:
        for u in db.query(Units).filter(
            Units.trang_thai == UnitStatus.ACTIVE,
            Units.path.like(f"{p}%")
        ).all():
            res[u.id] = u
    return list(res.values())

def accessible_units(db: Session, user: Users) -> List[Units]:
    """
    Trả về danh sách Units user được xem:
    - Admin/Lãnh đạo/ALL_UNITS/Grant VIEW_ALL: toàn hệ thống.
    - Vai trò Tổ (TO_TRUONG/PHO_TO): CHỈ chính Tổ (OWN_UNIT).
    - Vai trò Phòng (TRUONG_PHONG/PHO_PHONG): CÂY CON phòng mình (OWN_UNIT_TREE).
    - Khác: theo scope gán (OWN_UNIT hoặc OWN_UNIT_TREE).
    """
    # Toàn hệ thống
    if is_all_units_access(db, user):
        return db.query(Units).filter(Units.trang_thai == UnitStatus.ACTIVE).all()

    prims = user_primary_units(db, user)
    if not prims:
        return []

    codes = user_role_codes(db, user)
    scopes = user_scopes(db, user)
    paths = [u.path for u in prims if u.path]

    # Tổ: chỉ chính tổ (cap_do = 4)
    if RoleCode.ROLE_TO_TRUONG in codes or RoleCode.ROLE_PHO_TO in codes:
        # Lọc đúng các đơn vị cấp tổ trong primary memberships
        return prims  # OWN_UNIT

    # Phòng: cây con của phòng (không thấy ngang cấp)
    if RoleCode.ROLE_TRUONG_PHONG in codes or RoleCode.ROLE_PHO_PHONG in codes:
        if paths:
            return _units_under_paths(db, paths)  # OWN_UNIT_TREE theo path phòng
        return prims

    # Mặc định: theo scope gán
    if ScopeCode.OWN_UNIT_TREE in scopes and paths:
        return _units_under_paths(db, paths)
    # OWN_UNIT (hoặc không có scope cụ thể): chỉ chính đơn vị
    return prims

def accessible_unit_ids(db: Session, user: Users) -> Set[str]:
    return {u.id for u in accessible_units(db, user)}
