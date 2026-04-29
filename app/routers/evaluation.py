from __future__ import annotations

import os
import re
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Sequence, Tuple

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse, PlainTextResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session

from ..config import settings
from ..models import (
    CommitteeMembers,
    Committees,
    PlanItems,
    Plans,
    RoleCode,
    Roles,
    TaskStatus,
    Tasks,
    UnitStatus,
    Units,
    UserRoles,
    Users,
    UserStatus,
    UserUnitMemberships,
)
from ..security.deps import get_db, login_required, user_has_any_role
from ..security.scope import is_all_units_access

router = APIRouter(tags=["evaluation"])
templates = Jinja2Templates(directory="app/templates")


# ===== constants =====
_HC_BOARD_CODES = {"ROLE_ADMIN", "ROLE_LANH_DAO"}
_HC_DEPARTMENT_MANAGER_CODES = {"ROLE_TRUONG_PHONG", "ROLE_PHO_PHONG"}
_HC_TEAM_MANAGER_CODES = {"ROLE_TO_TRUONG", "ROLE_PHO_TO"}
_HC_STAFF_CODES = {"ROLE_NHAN_VIEN"}

_CM_BGD_CODES = {"ROLE_GIAM_DOC", "ROLE_PHO_GIAM_DOC_TRUC", "ROLE_PHO_GIAM_DOC"}
_CM_TOP_CODES = _HC_BOARD_CODES | _CM_BGD_CODES
_CM_KHOA_LEADER_CODES = {"ROLE_TRUONG_KHOA", "ROLE_PHO_TRUONG_KHOA"}
_CM_KHOA_HEAD_CODES = {"ROLE_DIEU_DUONG_TRUONG", "ROLE_KY_THUAT_VIEN_TRUONG"}
_CM_DONVI_LEADER_CODES = {"ROLE_TRUONG_DON_VI", "ROLE_PHO_DON_VI"}
_CM_DONVI_HEAD_CODES = {"ROLE_DIEU_DUONG_TRUONG_DON_VI", "ROLE_KY_THUAT_VIEN_TRUONG_DON_VI"}
_CM_NHOM_LEADER_CODES = {"ROLE_TRUONG_NHOM", "ROLE_PHO_NHOM"}
_CM_BAC_SI_CODES = {"ROLE_BAC_SI"}
_CM_NON_APPROVER_CODES = {
    "ROLE_DIEU_DUONG",
    "ROLE_KY_THUAT_VIEN",
    "ROLE_DUOC_SI",
    "ROLE_THU_KY_Y_KHOA",
    "ROLE_HO_LY",
    "ROLE_QL_CHAT_LUONG",
    "ROLE_QL_KY_THUAT",
    "ROLE_QL_AN_TOAN",
    "ROLE_QL_VAT_TU",
    "ROLE_QL_TRANG_THIET_BI",
    "ROLE_QL_MOI_TRUONG",
    "ROLE_QL_CNTT",
    "ROLE_NHAN_VIEN",
}
_STATUS_LABELS = [
    "Chưa thực hiện",
    "Mới triển khai bước đầu",
    "Đang thực hiện",
    "Đã hoàn thành",
    "Chuyển kỳ sau",
]

PLAN_SCOPE_COMMITTEE = "COMMITTEE"
TASK_SCOPE_COMMITTEE = "COMMITTEE"
COMMITTEE_MANAGER_ROLES = {"TRUONG_BAN", "PHO_TRUONG_BAN"}

# ===== helpers quyền / đơn vị =====
def _role_codes(db: Session, user_id: str) -> set[str]:
    rows = (
        db.query(Roles.code)
        .join(UserRoles, UserRoles.role_id == Roles.id)
        .filter(UserRoles.user_id == user_id)
        .all()
    )
    out: set[str] = set()
    for (code,) in rows:
        out.add(str(getattr(code, "value", code)).upper())
    return out

def _primary_position_code(user: Users) -> str:
    return str(getattr(user, "approved_position_code", "") or "").strip().upper()


def _primary_position_is(user: Users, position_codes: set[str]) -> bool:
    return _primary_position_code(user) in {str(x or "").strip().upper() for x in position_codes}


def _unit_block_code(unit: Optional[Units]) -> str:
    if not unit:
        return ""
    return str(getattr(getattr(unit, "block_code", None), "value", getattr(unit, "block_code", "")) or "").upper()


def _unit_category_code(unit: Optional[Units]) -> str:
    if not unit:
        return ""
    return str(getattr(getattr(unit, "unit_category", None), "value", getattr(unit, "unit_category", "")) or "").upper()


def _user_memberships(db: Session, user_id: str) -> List[UserUnitMemberships]:
    return db.query(UserUnitMemberships).filter(UserUnitMemberships.user_id == user_id).all()


def _user_primary_units(db: Session, user_id: str) -> List[Units]:
    mems = _user_memberships(db, user_id)
    prims = [m for m in mems if getattr(m, "is_primary", True)]
    ids = [m.unit_id for m in (prims or mems) if getattr(m, "unit_id", None)]
    if not ids:
        return []
    return db.query(Units).filter(Units.id.in_(ids)).all()


def _first_primary_unit(db: Session, user_id: str) -> Optional[Units]:
    units = _user_primary_units(db, user_id)
    return units[0] if units else None


def _parent_unit(db: Session, unit_id: Optional[str]) -> Optional[Units]:
    if not unit_id:
        return None
    unit = db.get(Units, unit_id)
    if not unit or not getattr(unit, "parent_id", None):
        return None
    return db.get(Units, unit.parent_id)


def _khoa_of_unit(db: Session, unit: Optional[Units]) -> Optional[Units]:
    if not unit:
        return None
    if _unit_category_code(unit) == "KHOA":
        return unit
    if _unit_category_code(unit) == "SUBUNIT" and getattr(unit, "parent_id", None):
        parent = db.get(Units, unit.parent_id)
        if parent and _unit_category_code(parent) == "KHOA":
            return parent
    return None


def _unit_children(db: Session, parent_ids: Sequence[str]) -> List[Units]:
    if not parent_ids:
        return []
    return (
        db.query(Units)
        .filter(Units.parent_id.in_(list(parent_ids)), Units.trang_thai == UnitStatus.ACTIVE)
        .order_by(Units.cap_do.asc(), Units.order_index.asc(), Units.ten_don_vi.asc())
        .all()
    )


def _unit_member_user_ids(db: Session, unit_ids: Sequence[str], *, primary_only: bool = False) -> List[str]:
    if not unit_ids:
        return []
    q = db.query(UserUnitMemberships.user_id).filter(UserUnitMemberships.unit_id.in_(list(unit_ids)))
    if primary_only and hasattr(UserUnitMemberships, "is_primary"):
        q = q.filter(UserUnitMemberships.is_primary == True)  # noqa: E712
    rows = q.distinct().all()
    return [r[0] for r in rows if r and r[0]]


def _active_users_map(db: Session, user_ids: Sequence[str]) -> Dict[str, Users]:
    if not user_ids:
        return {}
    rows = (
        db.query(Users)
        .filter(Users.id.in_(list(user_ids)), Users.status == UserStatus.ACTIVE)
        .all()
    )
    return {u.id: u for u in rows}


def _user_display_name(u: Optional[Users]) -> str:
    if not u:
        return ""
    return (getattr(u, "full_name", None) or getattr(u, "username", None) or "").strip()


def _user_primary_unit_name_map(db: Session, user_ids: Sequence[str]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    if not user_ids:
        return out
    mems = (
        db.query(UserUnitMemberships)
        .filter(UserUnitMemberships.user_id.in_(list(user_ids)))
        .all()
    )
    by_user: Dict[str, List[UserUnitMemberships]] = {}
    for m in mems:
        by_user.setdefault(m.user_id, []).append(m)
    unit_ids = [m.unit_id for m in mems if getattr(m, "unit_id", None)]
    units = {}
    if unit_ids:
        units = {u.id: u for u in db.query(Units).filter(Units.id.in_(list(set(unit_ids)))).all()}
    for uid in user_ids:
        rows = by_user.get(uid, [])
        row = next((x for x in rows if getattr(x, "is_primary", True)), rows[0] if rows else None)
        if row and row.unit_id in units:
            out[uid] = getattr(units[row.unit_id], "ten_don_vi", "") or ""
    return out


def _dedup_option_items(items: List[Dict[str, str]]) -> List[Dict[str, str]]:
    seen = set()
    out = []
    for item in items:
        key = (item.get("id", ""), item.get("name", ""), item.get("parent_name", ""), item.get("unit_name", ""))
        if key in seen:
            continue
        seen.add(key)
        out.append(item)
    return out



def _committee_is_active_for_evaluation(committee: Optional[Committees]) -> bool:
    if not committee:
        return False
    if str(getattr(committee, "status", "") or "").upper() != "ACTIVE":
        return False
    if not bool(getattr(committee, "is_active", False)):
        return False
    return True


def _committee_ids_where_user_is_manager(db: Session, user_id: str) -> List[str]:
    rows = (
        db.query(CommitteeMembers.committee_id)
        .join(Committees, Committees.id == CommitteeMembers.committee_id)
        .filter(CommitteeMembers.user_id == user_id)
        .filter(CommitteeMembers.is_active == True)  # noqa: E712
        .filter(CommitteeMembers.committee_role.in_(list(COMMITTEE_MANAGER_ROLES)))
        .filter(Committees.status == "ACTIVE", Committees.is_active == True)  # noqa: E712
        .distinct()
        .all()
    )
    return [str(row[0]) for row in rows if row and row[0]]


def _committee_ids_where_user_is_member(db: Session, user_id: str) -> List[str]:
    rows = (
        db.query(CommitteeMembers.committee_id)
        .join(Committees, Committees.id == CommitteeMembers.committee_id)
        .filter(CommitteeMembers.user_id == user_id)
        .filter(CommitteeMembers.is_active == True)  # noqa: E712
        .filter(Committees.status == "ACTIVE", Committees.is_active == True)  # noqa: E712
        .distinct()
        .all()
    )
    return [str(row[0]) for row in rows if row and row[0]]


def _committee_options_for_evaluation(db: Session, user: Users) -> List[Dict[str, str]]:
    """
    Danh sách Ban được phép đánh giá:
    - Admin/HĐTV: xem toàn bộ Ban đang hoạt động.
    - BGĐ: chỉ thấy Ban managed_by = BGD và Ban mà mình là thành viên.
    - Trưởng ban/Phó trưởng ban: thấy Ban mình quản lý.
    - Thành viên thường: không mở quyền đánh giá toàn Ban.
    """
    codes = _role_codes(db, user.id)
    committee_ids = set(_committee_ids_where_user_is_manager(db, user.id))

    if _is_board(db, user):
        rows = (
            db.query(Committees.id)
            .filter(Committees.status == "ACTIVE", Committees.is_active == True)  # noqa: E712
            .all()
        )
        committee_ids.update(str(row[0]) for row in rows if row and row[0])

    elif bool(codes & _CM_BGD_CODES):
        rows = (
            db.query(Committees.id)
            .filter(Committees.status == "ACTIVE", Committees.is_active == True)  # noqa: E712
            .filter(Committees.managed_by == "BGD")
            .all()
        )
        committee_ids.update(str(row[0]) for row in rows if row and row[0])
        committee_ids.update(_committee_ids_where_user_is_member(db, user.id))

    if not committee_ids:
        return []

    committees = (
        db.query(Committees)
        .filter(Committees.id.in_(list(committee_ids)))
        .filter(Committees.status == "ACTIVE", Committees.is_active == True)  # noqa: E712
        .order_by(Committees.name.asc())
        .all()
    )

    return [
        {
            "id": committee.id,
            "name": committee.name,
            "unit_name": "Ban kiêm nhiệm",
            "parent_name": "",
        }
        for committee in committees
    ]

# ===== helpers role =====
def _is_hc_board_codes(codes: set[str]) -> bool:
    return bool(_HC_BOARD_CODES & codes)


def _is_board(db: Session, user: Users) -> bool:
    return is_all_units_access(db, user)


def _is_mgr_phong(db: Session, user: Users) -> bool:
    return user_has_any_role(user, db, [RoleCode.ROLE_TRUONG_PHONG, RoleCode.ROLE_PHO_PHONG])


def _is_mgr_to(db: Session, user: Users) -> bool:
    return user_has_any_role(user, db, [RoleCode.ROLE_TO_TRUONG, RoleCode.ROLE_PHO_TO])


def _is_cm_top_codes(codes: set[str]) -> bool:
    return bool(_CM_TOP_CODES & codes)


def _is_cm_khoa_leader_codes(codes: set[str]) -> bool:
    return bool(_CM_KHOA_LEADER_CODES & codes)


def _is_cm_khoa_head_codes(codes: set[str]) -> bool:
    return bool(_CM_KHOA_HEAD_CODES & codes)


def _is_cm_donvi_leader_codes(codes: set[str]) -> bool:
    return bool(_CM_DONVI_LEADER_CODES & codes)


def _is_cm_donvi_head_codes(codes: set[str]) -> bool:
    return bool(_CM_DONVI_HEAD_CODES & codes)


def _is_cm_nhom_leader_codes(codes: set[str]) -> bool:
    return bool(_CM_NHOM_LEADER_CODES & codes)


def _is_cm_bac_si_codes(codes: set[str]) -> bool:
    return bool(_CM_BAC_SI_CODES & codes)


def _is_cm_non_approver_codes(codes: set[str]) -> bool:
    return bool(_CM_NON_APPROVER_CODES & codes)


def _is_department_manager_codes(codes: set[str]) -> bool:
    return bool(_HC_DEPARTMENT_MANAGER_CODES & codes)


def _is_team_manager_codes(codes: set[str]) -> bool:
    return bool(_HC_TEAM_MANAGER_CODES & codes)


def _is_staff_codes(codes: set[str]) -> bool:
    return bool(_HC_STAFF_CODES & codes)


def _is_cm_non_bacsi_non_approver_codes(codes: set[str]) -> bool:
    return _is_cm_non_approver_codes(codes) and not _is_cm_bac_si_codes(codes)


def _can_access_evaluation(db: Session, user: Users) -> bool:
    codes = _role_codes(db, user.id)
    return any(
        [
            _is_board(db, user),
            _is_mgr_phong(db, user),
            _is_mgr_to(db, user),
            _is_cm_top_codes(codes),
            _is_cm_khoa_leader_codes(codes),
            _is_cm_khoa_head_codes(codes),
            _is_cm_donvi_leader_codes(codes),
            _is_cm_donvi_head_codes(codes),
            _is_cm_nhom_leader_codes(codes),
            bool(_committee_options_for_evaluation(db, user)),
        ]
    )


# ===== helpers scope hành chính =====
def _pick_managed_department_ids(db: Session, user: Users) -> List[str]:
    phong_ids: List[str] = []
    for unit in _user_primary_units(db, user.id):
        cap = getattr(unit, "cap_do", None)
        if cap == 2 and _unit_block_code(unit) == "HANH_CHINH":
            phong_ids.append(unit.id)
        elif cap == 3 and _unit_block_code(unit) == "HANH_CHINH" and getattr(unit, "parent_id", None):
            phong_ids.append(unit.parent_id)
    return sorted({x for x in phong_ids if x})


def _pick_managed_team_ids(db: Session, user: Users) -> List[str]:
    team_ids: List[str] = []
    for unit in _user_primary_units(db, user.id):
        if getattr(unit, "cap_do", None) == 3 and _unit_block_code(unit) == "HANH_CHINH":
            team_ids.append(unit.id)
    return sorted({x for x in team_ids if x})


def _users_for_team_manager(db: Session, user: Users) -> List[Dict[str, str]]:
    team_ids = _pick_managed_team_ids(db, user)
    member_ids = [uid for uid in _unit_member_user_ids(db, team_ids) if uid != user.id]
    users_map = _active_users_map(db, member_ids)
    unit_name_map = _user_primary_unit_name_map(db, list(users_map.keys()))
    items: List[Dict[str, str]] = []
    for uid, target in users_map.items():
        codes = _role_codes(db, uid)
        if not _is_staff_codes(codes):
            continue
        items.append({"id": uid, "name": _user_display_name(target), "unit_name": unit_name_map.get(uid, "")})
    items.sort(key=lambda x: (x["unit_name"], x["name"]))
    return items


def _users_for_department_manager(db: Session, user: Users) -> List[Dict[str, str]]:
    phong_ids = _pick_managed_department_ids(db, user)
    team_units = _unit_children(db, phong_ids)
    team_ids = [u.id for u in team_units if _unit_block_code(u) == "HANH_CHINH" and getattr(u, "cap_do", None) == 3]
    all_unit_ids = list(dict.fromkeys(phong_ids + team_ids))
    member_ids = [uid for uid in _unit_member_user_ids(db, all_unit_ids) if uid != user.id]
    users_map = _active_users_map(db, member_ids)
    unit_name_map = _user_primary_unit_name_map(db, list(users_map.keys()))
    items: List[Dict[str, str]] = []
    for uid, target in users_map.items():
        codes = _role_codes(db, uid)
        if _is_department_manager_codes(codes):
            continue
        if not (_is_team_manager_codes(codes) or _is_staff_codes(codes)):
            continue
        items.append({"id": uid, "name": _user_display_name(target), "unit_name": unit_name_map.get(uid, "")})
    items.sort(key=lambda x: (x["unit_name"], x["name"]))
    return items


def _users_for_board(db: Session) -> List[Dict[str, str]]:
    rows = db.query(Users).filter(Users.status == UserStatus.ACTIVE).order_by(Users.full_name.asc(), Users.username.asc()).all()
    unit_name_map = _user_primary_unit_name_map(db, [u.id for u in rows])
    items: List[Dict[str, str]] = []
    for target in rows:
        codes = _role_codes(db, target.id)
        if not _is_department_manager_codes(codes):
            continue
        items.append({"id": target.id, "name": _user_display_name(target), "unit_name": unit_name_map.get(target.id, "")})
    items.sort(key=lambda x: (x["unit_name"], x["name"]))
    return items


def _team_units_for_department_manager(db: Session, user: Users) -> List[Dict[str, str]]:
    phong_ids = _pick_managed_department_ids(db, user)
    items = []
    for unit in _unit_children(db, phong_ids):
        if _unit_block_code(unit) != "HANH_CHINH" or getattr(unit, "cap_do", None) != 3:
            continue
        parent = db.get(Units, getattr(unit, "parent_id", None)) if getattr(unit, "parent_id", None) else None
        items.append({"id": unit.id, "name": getattr(unit, "ten_don_vi", "") or "", "parent_name": getattr(parent, "ten_don_vi", "") if parent else ""})
    items.sort(key=lambda x: (x["parent_name"], x["name"]))
    return items


def _department_units_for_board(db: Session) -> List[Dict[str, str]]:
    rows = (
        db.query(Units)
        .filter(Units.cap_do == 2, Units.trang_thai == UnitStatus.ACTIVE)
        .all()
    )
    rows = [u for u in rows if _unit_block_code(u) == "HANH_CHINH"]
    rows.sort(key=lambda u: getattr(u, "ten_don_vi", "") or "")
    return [{"id": u.id, "name": getattr(u, "ten_don_vi", "") or ""} for u in rows]


# ===== helpers scope chuyên môn =====
def _cm_primary_unit_of_user(db: Session, user_id: str) -> Optional[Units]:
    units = _user_primary_units(db, user_id)
    for unit in units:
        if _unit_block_code(unit) == "CHUYEN_MON":
            return unit

    mems = _user_memberships(db, user_id)
    unit_ids = [m.unit_id for m in mems if getattr(m, "unit_id", None)]
    if not unit_ids:
        return None

    rows = db.query(Units).filter(Units.id.in_(list(set(unit_ids)))).all()
    for unit in rows:
        if _unit_block_code(unit) == "CHUYEN_MON":
            return unit
    return None

def _cm_khoa_scope_of_user(db: Session, user_id: str) -> Optional[Units]:
    unit = _cm_primary_unit_of_user(db, user_id)
    if not unit:
        return None

    if _unit_category_code(unit) == "KHOA":
        return unit

    if _unit_category_code(unit) == "SUBUNIT" and getattr(unit, "parent_id", None):
        parent = db.get(Units, unit.parent_id)
        if parent and _unit_block_code(parent) == "CHUYEN_MON" and _unit_category_code(parent) == "KHOA":
            return parent

    return None


def _cm_same_primary_unit_user_ids(db: Session, unit_id: str) -> List[str]:
    return _unit_member_user_ids(db, [unit_id], primary_only=False)


def _cm_same_khoa_user_ids(db: Session, khoa_id: str) -> List[str]:
    ids = set(_cm_same_primary_unit_user_ids(db, khoa_id))
    for child in _unit_children(db, [khoa_id]):
        if _unit_block_code(child) != "CHUYEN_MON":
            continue
        ids.update(_cm_same_primary_unit_user_ids(db, child.id))
    return sorted(ids)


def _infer_cm_subunit_kind(db: Session, unit_id: str) -> str:
    if not unit_id:
        return ""

    user_ids = _unit_member_user_ids(db, [unit_id], primary_only=False)
    if not user_ids:
        return ""

    codes_all: set[str] = set()
    for uid in user_ids:
        codes_all |= _role_codes(db, uid)

    if codes_all & (_CM_DONVI_LEADER_CODES | _CM_DONVI_HEAD_CODES):
        return "DONVI"
    if codes_all & _CM_NHOM_LEADER_CODES:
        return "NHOM"
    return ""


def _cm_users_for_top(db: Session) -> List[Dict[str, str]]:
    rows = db.query(Users).filter(Users.status == UserStatus.ACTIVE).order_by(Users.full_name.asc(), Users.username.asc()).all()
    unit_name_map = _user_primary_unit_name_map(db, [u.id for u in rows])
    items: List[Dict[str, str]] = []
    wanted = _CM_KHOA_LEADER_CODES | _CM_DONVI_LEADER_CODES
    for target in rows:
        codes = _role_codes(db, target.id)
        if not (codes & wanted):
            continue
        unit = _cm_primary_unit_of_user(db, target.id)
        if not unit:
            continue
        items.append({"id": target.id, "name": _user_display_name(target), "unit_name": unit_name_map.get(target.id, "")})
    items.sort(key=lambda x: (x["unit_name"], x["name"]))
    return items


def _cm_units_for_top(db: Session) -> List[Dict[str, str]]:
    rows = db.query(Units).filter(Units.trang_thai == UnitStatus.ACTIVE).all()
    items: List[Dict[str, str]] = []
    for unit in rows:
        if _unit_block_code(unit) != "CHUYEN_MON":
            continue
        cat = _unit_category_code(unit)
        if cat == "KHOA":
            parent = db.get(Units, getattr(unit, "parent_id", None)) if getattr(unit, "parent_id", None) else None
            items.append({"id": unit.id, "name": getattr(unit, "ten_don_vi", "") or "", "parent_name": getattr(parent, "ten_don_vi", "") if parent else ""})
        elif cat == "SUBUNIT" and _infer_cm_subunit_kind(db, unit.id) == "DONVI":
            parent = db.get(Units, getattr(unit, "parent_id", None)) if getattr(unit, "parent_id", None) else None
            items.append({"id": unit.id, "name": getattr(unit, "ten_don_vi", "") or "", "parent_name": getattr(parent, "ten_don_vi", "") if parent else ""})
    items.sort(key=lambda x: (x["parent_name"], x["name"]))
    return items


def _cm_users_for_khoa_leader(db: Session, user: Users) -> List[Dict[str, str]]:
    khoa = _cm_khoa_scope_of_user(db, user.id)
    if not khoa:
        return []

    items: List[Dict[str, str]] = []

    # 1. Người trực tiếp thuộc Khoa: Bác sĩ, Điều dưỡng trưởng, KTV trưởng.
    direct_ids = [uid for uid in _cm_same_primary_unit_user_ids(db, khoa.id) if uid != user.id]
    direct_users = _active_users_map(db, direct_ids)
    direct_unit_name_map = _user_primary_unit_name_map(db, list(direct_users.keys()))

    for uid, target in direct_users.items():
        codes = _role_codes(db, uid)
        if not (codes & (_CM_KHOA_HEAD_CODES | _CM_BAC_SI_CODES)):
            continue

        items.append(
            {
                "id": uid,
                "name": _user_display_name(target),
                "unit_name": direct_unit_name_map.get(uid, ""),
            }
        )

    # 2. Người thuộc đơn vị trực thuộc Khoa: Trưởng đơn vị, Phó đơn vị, Bác sĩ.
    for child in _unit_children(db, [khoa.id]):
        if _unit_block_code(child) != "CHUYEN_MON":
            continue
        if _unit_category_code(child) != "SUBUNIT":
            continue
        if _infer_cm_subunit_kind(db, child.id) != "DONVI":
            continue

        child_ids = [uid for uid in _cm_same_primary_unit_user_ids(db, child.id) if uid != user.id]
        child_users = _active_users_map(db, child_ids)
        child_unit_name_map = _user_primary_unit_name_map(db, list(child_users.keys()))

        for uid, target in child_users.items():
            codes = _role_codes(db, uid)
            if not (codes & (_CM_DONVI_LEADER_CODES | _CM_BAC_SI_CODES)):
                continue

            items.append(
                {
                    "id": uid,
                    "name": _user_display_name(target),
                    "unit_name": child_unit_name_map.get(uid, ""),
                }
            )

    items.sort(key=lambda x: (x["unit_name"], x["name"]))
    return _dedup_option_items(items)


def _cm_users_for_khoa_head(db: Session, user: Users) -> List[Dict[str, str]]:
    khoa = _cm_khoa_scope_of_user(db, user.id)
    if not khoa:
        return []

    unit_name_map: Dict[str, str] = {}
    items: List[Dict[str, str]] = []

    direct_ids = [uid for uid in _cm_same_primary_unit_user_ids(db, khoa.id) if uid != user.id]
    direct_users = _active_users_map(db, direct_ids)
    unit_name_map.update(_user_primary_unit_name_map(db, list(direct_users.keys())))
    for uid, target in direct_users.items():
        codes = _role_codes(db, uid)
        if _is_cm_non_bacsi_non_approver_codes(codes):
            items.append(
                {
                    "id": uid,
                    "name": _user_display_name(target),
                    "unit_name": unit_name_map.get(uid, ""),
                }
            )

    for child in _unit_children(db, [khoa.id]):
        if _unit_block_code(child) != "CHUYEN_MON" or _infer_cm_subunit_kind(db, child.id) != "NHOM":
            continue
        child_ids = [uid for uid in _cm_same_primary_unit_user_ids(db, child.id) if uid != user.id]
        child_users = _active_users_map(db, child_ids)
        unit_name_map.update(_user_primary_unit_name_map(db, list(child_users.keys())))
        for uid, target in child_users.items():
            codes = _role_codes(db, uid)
            if not _is_cm_nhom_leader_codes(codes):
                continue
            items.append(
                {
                    "id": uid,
                    "name": _user_display_name(target),
                    "unit_name": unit_name_map.get(uid, ""),
                }
            )

    items.sort(key=lambda x: (x["unit_name"], x["name"]))
    return _dedup_option_items(items)


def _cm_nhom_units_for_khoa_head(db: Session, user: Users) -> List[Dict[str, str]]:
    khoa = _cm_khoa_scope_of_user(db, user.id)
    if not khoa:
        return []

    items: List[Dict[str, str]] = []
    for child in _unit_children(db, [khoa.id]):
        if _unit_block_code(child) != "CHUYEN_MON" or _infer_cm_subunit_kind(db, child.id) != "NHOM":
            continue
        parent = db.get(Units, getattr(child, "parent_id", None)) if getattr(child, "parent_id", None) else None
        items.append(
            {
                "id": child.id,
                "name": getattr(child, "ten_don_vi", "") or "",
                "parent_name": getattr(parent, "ten_don_vi", "") if parent else "",
            }
        )
    items.sort(key=lambda x: (x["parent_name"], x["name"]))
    return items


def _cm_units_for_khoa_leader(db: Session, user: Users) -> List[Dict[str, str]]:
    khoa = _cm_khoa_scope_of_user(db, user.id)
    if not khoa:
        return []

    items: List[Dict[str, str]] = [
        {
            "id": khoa.id,
            "name": getattr(khoa, "ten_don_vi", "") or "",
            "parent_name": "",
        }
    ]

    for child in _unit_children(db, [khoa.id]):
        if _unit_block_code(child) != "CHUYEN_MON":
            continue
        if _unit_category_code(child) != "SUBUNIT":
            continue
        if _infer_cm_subunit_kind(db, child.id) != "DONVI":
            continue

        items.append(
            {
                "id": child.id,
                "name": getattr(child, "ten_don_vi", "") or "",
                "parent_name": getattr(khoa, "ten_don_vi", "") or "",
            }
        )

    items.sort(key=lambda x: (x["parent_name"], x["name"]))
    return _dedup_option_items(items)

def _cm_users_for_donvi_leader(db: Session, user: Users) -> List[Dict[str, str]]:
    donvi = _cm_primary_unit_of_user(db, user.id)
    if not donvi or _infer_cm_subunit_kind(db, donvi.id) != "DONVI":
        return []
    member_ids = [uid for uid in _cm_same_primary_unit_user_ids(db, donvi.id) if uid != user.id]
    users_map = _active_users_map(db, member_ids)
    unit_name_map = _user_primary_unit_name_map(db, list(users_map.keys()))
    items: List[Dict[str, str]] = []
    wanted = _CM_DONVI_HEAD_CODES | _CM_BAC_SI_CODES
    for uid, target in users_map.items():
        codes = _role_codes(db, uid)
        if not (codes & wanted):
            continue
        items.append({"id": uid, "name": _user_display_name(target), "unit_name": unit_name_map.get(uid, "")})
    items.sort(key=lambda x: (x["unit_name"], x["name"]))
    return items


def _cm_users_for_donvi_head(db: Session, user: Users) -> List[Dict[str, str]]:
    donvi = _cm_primary_unit_of_user(db, user.id)
    if not donvi or _infer_cm_subunit_kind(db, donvi.id) != "DONVI":
        return []
    member_ids = [uid for uid in _cm_same_primary_unit_user_ids(db, donvi.id) if uid != user.id]
    users_map = _active_users_map(db, member_ids)
    unit_name_map = _user_primary_unit_name_map(db, list(users_map.keys()))
    items: List[Dict[str, str]] = []
    for uid, target in users_map.items():
        codes = _role_codes(db, uid)
        if _is_cm_non_bacsi_non_approver_codes(codes):
            items.append({"id": uid, "name": _user_display_name(target), "unit_name": unit_name_map.get(uid, "")})
    items.sort(key=lambda x: (x["unit_name"], x["name"]))
    return items


def _cm_users_for_nhom_leader(db: Session, user: Users) -> List[Dict[str, str]]:
    nhom = _cm_primary_unit_of_user(db, user.id)
    if not nhom or _infer_cm_subunit_kind(db, nhom.id) != "NHOM":
        return []
    member_ids = [uid for uid in _cm_same_primary_unit_user_ids(db, nhom.id) if uid != user.id]
    users_map = _active_users_map(db, member_ids)
    unit_name_map = _user_primary_unit_name_map(db, list(users_map.keys()))
    items: List[Dict[str, str]] = []
    for uid, target in users_map.items():
        codes = _role_codes(db, uid)
        if _is_cm_non_bacsi_non_approver_codes(codes):
            items.append({"id": uid, "name": _user_display_name(target), "unit_name": unit_name_map.get(uid, "")})
    items.sort(key=lambda x: (x["unit_name"], x["name"]))
    return items


# ===== helpers thời gian =====
def _coerce_int(value: Optional[str], default: int) -> int:
    try:
        return int(str(value or "").strip())
    except Exception:
        return default


def _build_period(period_type: str, year: int, month: int, quarter: int) -> Dict[str, Any]:
    ptype = (period_type or "month").strip().lower()
    if ptype not in {"month", "quarter", "year"}:
        ptype = "month"

    if ptype == "month":
        months = [max(1, min(month, 12))]
        label = f"Tháng {months[0]:02d}/{year}"
        return {"type": ptype, "year": year, "months": months, "label": label}

    if ptype == "quarter":
        q = max(1, min(quarter, 4))
        months = [(q - 1) * 3 + 1, (q - 1) * 3 + 2, (q - 1) * 3 + 3]
        label = f"Quý {q} năm {year}"
        return {"type": ptype, "year": year, "quarter": q, "months": months, "label": label}

    months = list(range(1, 13))
    label = f"Năm {year}"
    return {"type": ptype, "year": year, "months": months, "label": label}


def _task_marker_date(task: Tasks) -> date:
    dt = getattr(task, "due_date", None) or getattr(task, "created_at", None) or datetime.utcnow()
    return dt.date() if isinstance(dt, datetime) else datetime.utcnow().date()


def _plan_matches_period(plan: Plans, period: Dict[str, Any]) -> bool:
    return int(getattr(plan, "year", 0) or 0) == int(period["year"]) and int(getattr(plan, "month", 0) or 0) in set(period["months"])


def _task_matches_period(task: Tasks, period: Dict[str, Any]) -> bool:
    d = _task_marker_date(task)
    return d.year == int(period["year"]) and d.month in set(period["months"])


# ===== helpers task =====
def _task_assignee_user_id(task: Tasks) -> Optional[str]:
    return getattr(task, "assigned_to_user_id", None) or getattr(task, "assignee_id", None) or getattr(task, "assigned_user_id", None)


def _task_creator_user_id(task: Tasks) -> Optional[str]:
    return getattr(task, "created_by", None) or getattr(task, "creator_user_id", None) or getattr(task, "owner_user_id", None)


def _empty_task_stats() -> Dict[str, int]:
    return {
        "total": 0,
        "done": 0,
        "overdue": 0,
        "done_early": 0,
        "done_on_time": 0,
    }


def _is_task_closed(status_value: Any) -> bool:
    key = str(getattr(status_value, "value", status_value) or "").upper()
    return key in {
        TaskStatus.DONE.value,
        TaskStatus.CLOSED.value,
        TaskStatus.CANCELLED.value,
        TaskStatus.REJECTED.value,
    }


def _task_completed_at(task: Tasks) -> Optional[datetime]:
    for field in ("closed_at", "submitted_at", "finished_at", "done_at", "completed_at", "updated_at"):
        value = getattr(task, field, None)
        if isinstance(value, datetime):
            return value
    return None


def _accumulate_task_stats(stats: Dict[str, int], task: Tasks) -> None:
    stats["total"] += 1
    is_closed = _is_task_closed(getattr(task, "status", None))
    due_date = getattr(task, "due_date", None)

    if is_closed:
        stats["done"] += 1
        completed_at = _task_completed_at(task)
        if isinstance(due_date, datetime) and isinstance(completed_at, datetime):
            due_day = due_date.date()
            done_day = completed_at.date()
            if done_day < due_day:
                stats["done_early"] += 1
            elif done_day == due_day:
                stats["done_on_time"] += 1

    if isinstance(due_date, datetime) and due_date.date() < datetime.utcnow().date() and not is_closed:
        stats["overdue"] += 1


def _same_unit(unit: Optional[Units], other: Optional[Units]) -> bool:
    return bool(unit and other and str(getattr(unit, "id", "")) == str(getattr(other, "id", "")))


def _same_khoa(db: Session, left: Optional[Units], right: Optional[Units]) -> bool:
    return _same_unit(_khoa_of_unit(db, left), _khoa_of_unit(db, right))


def _task_eval_bucket(db: Session, task: Tasks) -> Optional[Dict[str, Any]]:
    assignee_id = _task_assignee_user_id(task)
    creator_id = _task_creator_user_id(task)
    if not assignee_id or not creator_id:
        return None

    assignee = db.get(Users, assignee_id)
    creator = db.get(Users, creator_id)
    if not assignee or not creator:
        return None

    assignee_codes = _role_codes(db, assignee.id)
    creator_codes = _role_codes(db, creator.id)
    assignee_unit = _first_primary_unit(db, assignee.id)
    creator_unit = _first_primary_unit(db, creator.id)
    if not assignee_unit:
        return None

    assignee_cap = getattr(assignee_unit, "cap_do", None)
    assignee_unit_id = getattr(assignee_unit, "id", None)
    assignee_parent_id = getattr(assignee_unit, "parent_id", None)

    # ===== Hành chính giữ nguyên =====
    if _is_staff_codes(assignee_codes):
        if assignee_cap == 3 and assignee_unit_id:
            creator_team_ids = set(_pick_managed_team_ids(db, creator))
            if _is_team_manager_codes(creator_codes) and assignee_unit_id in creator_team_ids:
                return {"individual_user_id": assignee.id, "unit_kind": None, "unit_id": None}

        if assignee_cap == 2 and assignee_unit_id:
            creator_dept_ids = set(_pick_managed_department_ids(db, creator))
            if _is_department_manager_codes(creator_codes) and assignee_unit_id in creator_dept_ids:
                return {"individual_user_id": assignee.id, "unit_kind": None, "unit_id": None}
        return None

    if _is_team_manager_codes(assignee_codes):
        team_id = assignee_unit_id if assignee_cap == 3 else None
        dept_id = assignee_parent_id if assignee_cap == 3 else assignee_unit_id
        if not team_id or not dept_id:
            return None
        creator_dept_ids = set(_pick_managed_department_ids(db, creator))
        if _is_department_manager_codes(creator_codes) and dept_id in creator_dept_ids:
            return {"individual_user_id": assignee.id, "unit_kind": "TO", "unit_id": team_id}
        return None

    if _is_department_manager_codes(assignee_codes):
        dept_id = assignee_unit_id if assignee_cap == 2 else assignee_parent_id
        if not dept_id:
            return None
        if _is_hc_board_codes(creator_codes):
            return {"individual_user_id": assignee.id, "unit_kind": "PHONG", "unit_id": dept_id}
        return None

    # ===== Chuyên môn =====
    assignee_block = _unit_block_code(assignee_unit)
    assignee_category = _unit_category_code(assignee_unit)
    assignee_subunit_kind = _infer_cm_subunit_kind(db, assignee_unit.id) if assignee_category == "SUBUNIT" else ""

    # Trưởng khoa / Phó khoa: chỉ task do HĐTV/BGĐ toàn cục giao, tính cá nhân + Khoa
    if _is_cm_khoa_leader_codes(assignee_codes):
        if assignee_block == "CHUYEN_MON" and assignee_category == "KHOA" and _is_cm_top_codes(creator_codes):
            return {"individual_user_id": assignee.id, "unit_kind": "KHOA", "unit_id": assignee_unit.id}
        return None

    # Điều dưỡng trưởng / KTV trưởng cấp khoa và Bác sĩ trực thuộc Khoa: do Trưởng/Phó khoa cùng Khoa giao
    if assignee_block == "CHUYEN_MON" and assignee_category == "KHOA" and (
        _is_cm_khoa_head_codes(assignee_codes) or _is_cm_bac_si_codes(assignee_codes)
    ):
        if _is_cm_khoa_leader_codes(creator_codes) and _same_khoa(db, assignee_unit, creator_unit):
            return {"individual_user_id": assignee.id, "unit_kind": None, "unit_id": None}
        return None

    # Vị trí không xét duyệt thuộc Khoa (trừ Bác sĩ): do Điều dưỡng trưởng/KTV trưởng cấp Khoa giao
    if assignee_block == "CHUYEN_MON" and assignee_category == "KHOA" and _is_cm_non_bacsi_non_approver_codes(assignee_codes):
        if _is_cm_khoa_head_codes(creator_codes) and _same_khoa(db, assignee_unit, creator_unit):
            return {"individual_user_id": assignee.id, "unit_kind": None, "unit_id": None}
        return None

    # Trưởng đơn vị / Phó đơn vị: do Trưởng/Phó khoa cùng Khoa giao; tính cá nhân + Đơn vị
    if assignee_block == "CHUYEN_MON" and assignee_category == "SUBUNIT" and assignee_subunit_kind == "DONVI" and _is_cm_donvi_leader_codes(assignee_codes):
        if _is_cm_khoa_leader_codes(creator_codes) and _same_khoa(db, assignee_unit, creator_unit):
            return {"individual_user_id": assignee.id, "unit_kind": "DONVI", "unit_id": assignee_unit.id}
        return None

    # Bác sĩ, Điều dưỡng trưởng đơn vị, KTV trưởng đơn vị: do Trưởng/Phó đơn vị cùng đơn vị giao
    if assignee_block == "CHUYEN_MON" and assignee_category == "SUBUNIT" and assignee_subunit_kind == "DONVI" and (
        _is_cm_bac_si_codes(assignee_codes) or _is_cm_donvi_head_codes(assignee_codes)
    ):
        if _is_cm_donvi_leader_codes(creator_codes) and _same_unit(assignee_unit, creator_unit):
            return {"individual_user_id": assignee.id, "unit_kind": None, "unit_id": None}
        return None

    # Vị trí không xét duyệt thuộc Đơn vị (trừ Bác sĩ): do Điều dưỡng trưởng/KTV trưởng đơn vị giao
    if assignee_block == "CHUYEN_MON" and assignee_category == "SUBUNIT" and assignee_subunit_kind == "DONVI" and _is_cm_non_bacsi_non_approver_codes(assignee_codes):
        if _is_cm_donvi_head_codes(creator_codes) and _same_unit(assignee_unit, creator_unit):
            return {"individual_user_id": assignee.id, "unit_kind": None, "unit_id": None}
        return None

    # Trưởng nhóm / Phó nhóm: do Điều dưỡng trưởng/KTV trưởng cấp Khoa giao
    if assignee_block == "CHUYEN_MON" and assignee_category == "SUBUNIT" and assignee_subunit_kind == "NHOM" and _is_cm_nhom_leader_codes(assignee_codes):
        if _is_cm_khoa_head_codes(creator_codes) and _same_khoa(db, assignee_unit, creator_unit):
            return {"individual_user_id": assignee.id, "unit_kind": "NHOM", "unit_id": assignee_unit.id}
        return None

    # Vị trí không xét duyệt thuộc Nhóm: do Trưởng nhóm / Phó nhóm cùng nhóm giao
    if assignee_block == "CHUYEN_MON" and assignee_category == "SUBUNIT" and assignee_subunit_kind == "NHOM" and _is_cm_non_bacsi_non_approver_codes(assignee_codes):
        if _is_cm_nhom_leader_codes(creator_codes) and _same_unit(assignee_unit, creator_unit):
            return {"individual_user_id": assignee.id, "unit_kind": None, "unit_id": None}
        return None

    return None


def _collect_task_rows_for_period(db: Session, period: Dict[str, Any]) -> List[Tasks]:
    rows = db.query(Tasks).all()
    return [t for t in rows if _task_matches_period(t, period)]


def _task_stats_for_user(db: Session, user_id: str, period: Dict[str, Any]) -> Dict[str, int]:
    stats = _empty_task_stats()
    for task in _collect_task_rows_for_period(db, period):
        bucket = _task_eval_bucket(db, task)
        if not bucket:
            continue
        if str(bucket.get("individual_user_id") or "") != str(user_id):
            continue
        _accumulate_task_stats(stats, task)
    return stats


def _task_stats_for_unit(db: Session, unit_id: str, plan_kind: str, period: Dict[str, Any]) -> Dict[str, int]:
    stats = _empty_task_stats()
    wanted_kind = (plan_kind or "").strip().upper()
    for task in _collect_task_rows_for_period(db, period):
        bucket = _task_eval_bucket(db, task)
        if not bucket:
            continue
        if str(bucket.get("unit_kind") or "") != wanted_kind:
            continue
        if str(bucket.get("unit_id") or "") != str(unit_id):
            continue
        _accumulate_task_stats(stats, task)
    return stats


# ===== helpers kế hoạch =====
def _normalize_plan_status(value: Optional[str]) -> str:
    val = (value or "").strip()
    if not val:
        return "Chưa thực hiện"
    if val == "Chưa hoàn thành":
        return "Mới triển khai bước đầu"
    if val not in _STATUS_LABELS:
        return "Chưa thực hiện"
    return val


def _extract_status_label(content: Optional[str]) -> str:
    m = re.search(r"\[\[STATUS=([^\]]+)\]\]", content or "")
    val = (m.group(1).strip() if m else "").strip()
    return _normalize_plan_status(val)


def _empty_plan_stats() -> Dict[str, int]:
    return {label: 0 for label in _STATUS_LABELS}


def _empty_plan_admin_stats() -> Dict[str, int]:
    return {"ever_carried_forward": 0, "carry_forward_times": 0}


def _plan_sort_key(plan: Plans, item: PlanItems) -> Tuple[int, int, datetime, str]:
    y = int(getattr(plan, "year", 0) or 0)
    m = int(getattr(plan, "month", 0) or 0)
    updated = getattr(item, "updated_at", None) or getattr(item, "created_at", None) or datetime.min
    iid = str(getattr(item, "id", "") or "")
    return (y, m, updated, iid)


def _collect_plan_items_for_scope(
    db: Session,
    *,
    created_by: Optional[str] = None,
    unit_id: Optional[str] = None,
    plan_kind: Optional[str] = None,
    period: Dict[str, Any],
) -> List[Tuple[Plans, PlanItems]]:
    q = db.query(Plans)
    if created_by:
        q = q.filter(Plans.created_by == created_by)
    if unit_id:
        q = q.filter(Plans.unit_id == unit_id)
    if plan_kind:
        q = q.filter(Plans.plan_kind == plan_kind)

    plans = q.all()
    target_plans = [p for p in plans if _plan_matches_period(p, period)]
    if not target_plans:
        return []

    plan_ids = [p.id for p in target_plans]
    items = db.query(PlanItems).filter(PlanItems.plan_id.in_(plan_ids)).all()
    plan_map = {p.id: p for p in target_plans}

    out: List[Tuple[Plans, PlanItems]] = []
    for item in items:
        p = plan_map.get(getattr(item, "plan_id", None))
        if p is None:
            continue
        out.append((p, item))
    return out


def _summarize_plan_items(rows: List[Tuple[Plans, PlanItems]]) -> Tuple[Dict[str, int], Dict[str, int]]:
    stats = _empty_plan_stats()
    admin_stats = _empty_plan_admin_stats()
    by_work_key: Dict[str, List[Tuple[Plans, PlanItems]]] = {}

    for plan, item in rows:
        work_key = (getattr(item, "work_key", None) or getattr(item, "id", None) or "").strip()
        if not work_key:
            continue
        by_work_key.setdefault(work_key, []).append((plan, item))

    for chain_rows in by_work_key.values():
        ordered = sorted(chain_rows, key=lambda x: _plan_sort_key(x[0], x[1]))
        _, final_item = ordered[-1]
        final_status = _extract_status_label(getattr(final_item, "content", ""))
        stats[final_status] += 1

        ever_cf = bool(getattr(final_item, "was_ever_carried_forward", False))
        cf_count = int(getattr(final_item, "carried_forward_count", 0) or 0)
        if ever_cf:
            admin_stats["ever_carried_forward"] += 1
        if cf_count > 0:
            admin_stats["carry_forward_times"] += cf_count

    return stats, admin_stats


def _plan_stats_for_user(db: Session, user_id: str, period: Dict[str, Any]) -> Tuple[Dict[str, int], Dict[str, int]]:
    rows = _collect_plan_items_for_scope(db, created_by=user_id, plan_kind="NHANVIEN", period=period)
    return _summarize_plan_items(rows)


def _plan_stats_for_unit(db: Session, unit_id: str, plan_kind: str, period: Dict[str, Any]) -> Tuple[Dict[str, int], Dict[str, int]]:
    rows = _collect_plan_items_for_scope(db, unit_id=unit_id, plan_kind=plan_kind, period=period)
    return _summarize_plan_items(rows)



def _plan_stats_for_committee(db: Session, committee_id: str, period: Dict[str, Any]) -> Tuple[Dict[str, int], Dict[str, int]]:
    plans = (
        db.query(Plans)
        .filter(Plans.scope_type == PLAN_SCOPE_COMMITTEE)
        .filter(Plans.committee_id == committee_id)
        .all()
    )
    plans = [p for p in plans if _plan_matches_period(p, period)]

    if not plans:
        return _empty_plan_stats(), _empty_plan_admin_stats()

    plan_map = {p.id: p for p in plans}
    items = db.query(PlanItems).filter(PlanItems.plan_id.in_(list(plan_map.keys()))).all()

    rows: List[Tuple[Plans, PlanItems]] = []
    for item in items:
        plan = plan_map.get(getattr(item, "plan_id", None))
        if plan is None:
            continue
        rows.append((plan, item))

    return _summarize_plan_items(rows)


def _task_stats_for_committee(db: Session, committee_id: str, period: Dict[str, Any]) -> Dict[str, int]:
    stats = _empty_task_stats()
    rows = (
        db.query(Tasks)
        .filter(Tasks.scope_type == TASK_SCOPE_COMMITTEE)
        .filter(Tasks.committee_id == committee_id)
        .all()
    )

    for task in rows:
        if not _task_matches_period(task, period):
            continue
        _accumulate_task_stats(stats, task)

    return stats

def _sum_plan_stats(stats: Dict[str, int]) -> int:
    return sum(int(v or 0) for v in stats.values())


def _infer_unit_plan_kind(db: Session, unit: Units) -> str:
    block = _unit_block_code(unit)
    cat = _unit_category_code(unit)
    if block == "HANH_CHINH":
        return "PHONG" if getattr(unit, "cap_do", None) == 2 else "TO"
    if block == "CHUYEN_MON" and cat == "KHOA":
        return "KHOA"
    if block == "CHUYEN_MON" and cat == "SUBUNIT":
        return _infer_cm_subunit_kind(db, unit.id)
    return ""


# ===== build result =====
def _build_individual_result(db: Session, user_id: str, period: Dict[str, Any]) -> Dict[str, Any]:
    target = db.get(Users, user_id)
    if not target:
        raise HTTPException(status_code=404, detail="Không tìm thấy đối tượng đánh giá.")
    plan_stats, plan_admin_stats = _plan_stats_for_user(db, user_id, period)
    task_stats = _task_stats_for_user(db, user_id, period)
    unit_name = _user_primary_unit_name_map(db, [user_id]).get(user_id, "")
    return {
        "mode": "individual",
        "subject_id": user_id,
        "subject_name": _user_display_name(target),
        "unit_name": unit_name,
        "period_label": period["label"],
        "plan_stats": plan_stats,
        "plan_admin_stats": plan_admin_stats,
        "plan_total": _sum_plan_stats(plan_stats),
        "task_stats": task_stats,
    }


def _build_unit_result(db: Session, unit_id: str, period: Dict[str, Any]) -> Dict[str, Any]:
    unit = db.get(Units, unit_id)
    if not unit:
        raise HTTPException(status_code=404, detail="Không tìm thấy đơn vị đánh giá.")
    plan_kind = _infer_unit_plan_kind(db, unit)
    if not plan_kind:
        raise HTTPException(status_code=400, detail="Đơn vị hiện tại không thuộc loại đánh giá hợp lệ.")
    plan_stats, plan_admin_stats = _plan_stats_for_unit(db, unit_id, plan_kind, period)
    task_stats = _task_stats_for_unit(db, unit_id, plan_kind, period)
    parent = db.get(Units, getattr(unit, "parent_id", None)) if getattr(unit, "parent_id", None) else None
    return {
        "mode": "unit",
        "subject_id": unit_id,
        "subject_name": getattr(unit, "ten_don_vi", "") or "",
        "unit_name": getattr(parent, "ten_don_vi", "") if parent else "",
        "period_label": period["label"],
        "plan_stats": plan_stats,
        "plan_admin_stats": plan_admin_stats,
        "plan_total": _sum_plan_stats(plan_stats),
        "task_stats": task_stats,
    }



def _build_committee_result(db: Session, committee_id: str, period: Dict[str, Any]) -> Dict[str, Any]:
    committee = db.get(Committees, committee_id)
    if not committee or not _committee_is_active_for_evaluation(committee):
        raise HTTPException(status_code=404, detail="Không tìm thấy Ban kiêm nhiệm đang hoạt động.")

    plan_stats, plan_admin_stats = _plan_stats_for_committee(db, committee_id, period)
    task_stats = _task_stats_for_committee(db, committee_id, period)

    return {
        "mode": "committee",
        "subject_id": committee_id,
        "subject_name": getattr(committee, "name", "") or "",
        "unit_name": "Ban kiêm nhiệm",
        "period_label": period["label"],
        "plan_stats": plan_stats,
        "plan_admin_stats": plan_admin_stats,
        "plan_total": _sum_plan_stats(plan_stats),
        "task_stats": task_stats,
    }

def _scope_options(db: Session, user: Users) -> Tuple[List[Dict[str, str]], List[Dict[str, str]], List[Dict[str, str]]]:
    codes = _role_codes(db, user.id)
    individual_options: List[Dict[str, str]] = []
    unit_options: List[Dict[str, str]] = []
    committee_options: List[Dict[str, str]] = _committee_options_for_evaluation(db, user)

    # Nguyên tắc cho user kiêm nhiệm:
    # - Không chọn 1 vai trò rồi loại vai trò còn lại.
    # - Cộng dồn phạm vi đánh giá theo vị trí chính và vai trò kiêm nhiệm.
    # - Cuối cùng khử trùng danh sách.
    #
    # Ví dụ:
    # approved_position_code = TRUONG_KHOA
    # role_codes có ROLE_TRUONG_KHOA + ROLE_PHO_GIAM_DOC
    # => phải có cả phạm vi đánh giá Trưởng khoa và phạm vi đánh giá BGĐ.

    # 1. Phạm vi theo vị trí chính.
    if _primary_position_is(user, {"TRUONG_KHOA", "PHO_KHOA"}):
        individual_options.extend(_cm_users_for_khoa_leader(db, user))
        unit_options.extend(_cm_units_for_khoa_leader(db, user))

    elif _primary_position_is(user, {"DIEU_DUONG_TRUONG", "KY_THUAT_VIEN_TRUONG"}):
        individual_options.extend(_cm_users_for_khoa_head(db, user))
        unit_options.extend(_cm_nhom_units_for_khoa_head(db, user))

    elif _primary_position_is(user, {"TRUONG_DON_VI", "PHO_DON_VI"}):
        individual_options.extend(_cm_users_for_donvi_leader(db, user))

    elif _primary_position_is(user, {"DIEU_DUONG_TRUONG_DON_VI", "KY_THUAT_VIEN_TRUONG_DON_VI"}):
        individual_options.extend(_cm_users_for_donvi_head(db, user))

    elif _primary_position_is(user, {"TRUONG_NHOM", "PHO_NHOM"}):
        individual_options.extend(_cm_users_for_nhom_leader(db, user))

    elif _primary_position_is(user, {"TRUONG_PHONG", "PHO_PHONG"}):
        individual_options.extend(_users_for_department_manager(db, user))
        unit_options.extend(_team_units_for_department_manager(db, user))

    elif _primary_position_is(user, {"TO_TRUONG", "PHO_TO"}):
        individual_options.extend(_users_for_team_manager(db, user))

    # 2. Phạm vi theo vai trò HĐTV/Admin/BGĐ kiêm nhiệm.
    if _is_board(db, user):
        individual_options.extend(_users_for_board(db))
        unit_options.extend(_department_units_for_board(db))
        individual_options.extend(_cm_users_for_top(db))
        unit_options.extend(_cm_units_for_top(db))

    elif _is_cm_top_codes(codes):
        individual_options.extend(_cm_users_for_top(db))
        unit_options.extend(_cm_units_for_top(db))

    # 3. Phạm vi theo các role quản lý khác nếu user không có approved_position_code tương ứng
    # hoặc hệ thống cũ còn dữ liệu role nhưng chưa có approved_position_code.
    if _is_mgr_phong(db, user):
        individual_options.extend(_users_for_department_manager(db, user))
        unit_options.extend(_team_units_for_department_manager(db, user))

    if _is_mgr_to(db, user):
        individual_options.extend(_users_for_team_manager(db, user))

    if _is_cm_khoa_leader_codes(codes):
        individual_options.extend(_cm_users_for_khoa_leader(db, user))
        unit_options.extend(_cm_units_for_khoa_leader(db, user))

    if _is_cm_khoa_head_codes(codes):
        individual_options.extend(_cm_users_for_khoa_head(db, user))
        unit_options.extend(_cm_nhom_units_for_khoa_head(db, user))

    if _is_cm_donvi_leader_codes(codes):
        individual_options.extend(_cm_users_for_donvi_leader(db, user))

    if _is_cm_donvi_head_codes(codes):
        individual_options.extend(_cm_users_for_donvi_head(db, user))

    if _is_cm_nhom_leader_codes(codes):
        individual_options.extend(_cm_users_for_nhom_leader(db, user))

    return (
        _dedup_option_items(individual_options),
        _dedup_option_items(unit_options),
        _dedup_option_items(committee_options),
    )


def _default_selection(
    individuals: List[Dict[str, str]],
    units: List[Dict[str, str]],
    committees: Optional[List[Dict[str, str]]] = None,
) -> Tuple[str, str]:
    committees = committees or []
    if individuals:
        return "individual", individuals[0]["id"]
    if units:
        return "unit", units[0]["id"]
    if committees:
        return "committee", committees[0]["id"]
    return "individual", ""


def _export_text(result: Dict[str, Any]) -> str:
    plan_admin = result.get("plan_admin_stats") or {}
    mode_label = (
        "Cá nhân" if result.get("mode") == "individual"
        else "Ban kiêm nhiệm" if result.get("mode") == "committee"
        else "Đơn vị"
    )

    lines = [
        "ĐÁNH GIÁ HOÀN THÀNH CÔNG VIỆC",
        "",
        f"Loại đánh giá: {mode_label}",
        f"Đối tượng: {result.get('subject_name', '')}",
    ]
    if result.get("unit_name"):
        lines.append(f"Đơn vị: {result.get('unit_name', '')}")
    lines.extend(
        [
            f"Kỳ đánh giá: {result.get('period_label', '')}",
            "",
            "I. Kế hoạch",
            f"- Chưa thực hiện: {result['plan_stats'].get('Chưa thực hiện', 0)}",
            f"- Mới triển khai bước đầu: {result['plan_stats'].get('Mới triển khai bước đầu', 0)}",
            f"- Đang thực hiện: {result['plan_stats'].get('Đang thực hiện', 0)}",
            f"- Đã hoàn thành: {result['plan_stats'].get('Đã hoàn thành', 0)}",
            f"- Chuyển kỳ sau: {result['plan_stats'].get('Chuyển kỳ sau', 0)}",
            "",
            "II. Chỉ số quản trị kế hoạch",
            f"- Công việc từng bị chuyển kỳ sau: {plan_admin.get('ever_carried_forward', 0)}",
            f"- Tổng số lần chuyển kỳ sau: {plan_admin.get('carry_forward_times', 0)}",
        ]
    )
    if result.get("task_stats") is not None:
        task_stats = result["task_stats"]
        lines.extend(
            [
                "",
                "III. Giao việc",
                f"- Số lượng công việc giao: {task_stats.get('total', 0)}",
                f"- Số lượng công việc hoàn thành: {task_stats.get('done', 0)}",
                f"- Số công việc hoàn thành trước thời hạn: {task_stats.get('done_early', 0)}",
                f"- Số công việc hoàn thành đúng thời hạn: {task_stats.get('done_on_time', 0)}",
                f"- Số lượng quá hạn: {task_stats.get('overdue', 0)}",
            ]
        )
    return "\n".join(lines).strip() + "\n"


@router.get("", response_class=HTMLResponse)
def evaluation_home(request: Request, db: Session = Depends(get_db)):
    user = login_required(request, db)
    if not _can_access_evaluation(db, user):
        raise HTTPException(status_code=403, detail="Bạn không có quyền truy cập tab đánh giá.")

    today = datetime.utcnow()
    period_type = (request.query_params.get("period_type") or "month").strip().lower()
    year = _coerce_int(request.query_params.get("year"), today.year)
    month = _coerce_int(request.query_params.get("month"), today.month)
    quarter = _coerce_int(request.query_params.get("quarter"), ((today.month - 1) // 3) + 1)
    period = _build_period(period_type, year, month, quarter)

    individual_options, unit_options, committee_options = _scope_options(db, user)
    selected_type = (request.query_params.get("target_type") or "").strip().lower()
    selected_id = (request.query_params.get("target_id") or "").strip()

    valid_individual_ids = {x["id"] for x in individual_options}
    valid_unit_ids = {x["id"] for x in unit_options}
    valid_committee_ids = {x["id"] for x in committee_options}

    if selected_type == "individual" and selected_id not in valid_individual_ids:
        selected_type = ""
        selected_id = ""
    if selected_type == "unit" and selected_id not in valid_unit_ids:
        selected_type = ""
        selected_id = ""
    if selected_type == "committee" and selected_id not in valid_committee_ids:
        selected_type = ""
        selected_id = ""

    if not selected_type or not selected_id:
        selected_type, selected_id = _default_selection(individual_options, unit_options, committee_options)

    result = None
    if selected_type == "individual" and selected_id:
        result = _build_individual_result(db, selected_id, period)
    elif selected_type == "unit" and selected_id:
        result = _build_unit_result(db, selected_id, period)
    elif selected_type == "committee" and selected_id:
        result = _build_committee_result(db, selected_id, period)

    return templates.TemplateResponse(
        "evaluation/evaluation.html",
        {
            "request": request,
            "user": user,
            "app_name": getattr(settings, "APP_NAME", "QLCV_App"),
            "company_name": getattr(settings, "COMPANY_NAME", ""),
            "period_type": period_type,
            "selected_year": year,
            "selected_month": month,
            "selected_quarter": quarter,
            "individual_options": individual_options,
            "unit_options": unit_options,
            "committee_options": committee_options,
            "selected_type": selected_type,
            "selected_id": selected_id,
            "result": result,
        },
    )


@router.get("/export", response_class=PlainTextResponse)
def export_evaluation_txt(request: Request, db: Session = Depends(get_db)):
    user = login_required(request, db)
    if not _can_access_evaluation(db, user):
        raise HTTPException(status_code=403, detail="Bạn không có quyền xuất đánh giá.")

    today = datetime.utcnow()
    period_type = (request.query_params.get("period_type") or "month").strip().lower()
    year = _coerce_int(request.query_params.get("year"), today.year)
    month = _coerce_int(request.query_params.get("month"), today.month)
    quarter = _coerce_int(request.query_params.get("quarter"), ((today.month - 1) // 3) + 1)
    period = _build_period(period_type, year, month, quarter)

    individual_options, unit_options, committee_options = _scope_options(db, user)
    selected_type = (request.query_params.get("target_type") or "").strip().lower()
    selected_id = (request.query_params.get("target_id") or "").strip()

    valid_individual_ids = {x["id"] for x in individual_options}
    valid_unit_ids = {x["id"] for x in unit_options}
    valid_committee_ids = {x["id"] for x in committee_options}

    if selected_type == "individual":
        if selected_id not in valid_individual_ids:
            raise HTTPException(status_code=403, detail="Đối tượng cá nhân không thuộc phạm vi đánh giá.")
        result = _build_individual_result(db, selected_id, period)
    elif selected_type == "unit":
        if selected_id not in valid_unit_ids:
            raise HTTPException(status_code=403, detail="Đơn vị không thuộc phạm vi đánh giá.")
        result = _build_unit_result(db, selected_id, period)
    elif selected_type == "committee":
        if selected_id not in valid_committee_ids:
            raise HTTPException(status_code=403, detail="Ban kiêm nhiệm không thuộc phạm vi đánh giá.")
        result = _build_committee_result(db, selected_id, period)
    else:
        raise HTTPException(status_code=400, detail="Thiếu loại đối tượng đánh giá.")

    filename = f"danh_gia_{result['mode']}_{result['subject_id']}.txt"
    headers = {"Content-Disposition": f'attachment; filename="{filename}"'}
    return PlainTextResponse(_export_text(result), headers=headers)