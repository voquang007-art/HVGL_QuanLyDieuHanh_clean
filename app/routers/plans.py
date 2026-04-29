from fastapi import APIRouter, Request, Depends, Form, HTTPException, status
from starlette.responses import RedirectResponse
from starlette.templating import Jinja2Templates
from sqlalchemy.orm import Session
from sqlalchemy import func, and_, or_
from typing import List, Optional, Dict
import os
import uuid
import logging
from datetime import datetime
from anyio import from_thread

# GIỮ NGUYÊN import & cấu trúc
from ..security.deps import get_db, login_required, user_has_any_role
from ..security.secret_lock import require_secret_lock
from ..security.policy import ActionCode
from ..security.scope import accessible_units, accessible_unit_ids, is_all_units_access
from ..models import (
    Users, Units, UserUnitMemberships,
    Plans, PlanItems, PlanStatus,
    Roles, RoleCode, UnitStatus,
    VisibilityGrants, UserRoles,
    Committees, CommitteeMembers
)
from ..config import settings
from ..chat.realtime import manager

logger = logging.getLogger("app.plans")

router = APIRouter(tags=["plans"])
templates = Jinja2Templates(directory=os.path.join(os.path.dirname(__file__), "..", "templates"))
templates.env.globals["now"] = lambda: datetime.utcnow()

PLAN_SCOPE_UNIT = "UNIT"
PLAN_SCOPE_COMMITTEE = "COMMITTEE"
COMMITTEE_MANAGER_ROLES = {"TRUONG_BAN", "PHO_TRUONG_BAN"}
# TẮT secret lock cho Plans (không đổi cấu trúc)
_ENABLE_SECRET_LOCK_PLANS = False
def _skip_secret_lock():
    return None

# ===== Helpers =====
def _user_primary_units(db: Session, user: Users) -> List[Units]:
    mems = db.query(UserUnitMemberships).filter(UserUnitMemberships.user_id == user.id).all()
    prims = [m for m in mems if getattr(m, "is_primary", True)]
    ids = [m.unit_id for m in (prims or mems)]
    if not ids: return []
    return db.query(Units).filter(Units.id.in_(ids)).all()

def _parent_unit(db: Session, unit_id: str) -> Optional[Units]:
    u = db.get(Units, unit_id)
    if not u or not u.parent_id: return None
    return db.get(Units, u.parent_id)

def _unit_children(db: Session, parent_ids: List[str]) -> List[Units]:
    if not parent_ids: return []
    return db.query(Units).filter(Units.parent_id.in_(parent_ids), Units.trang_thai == UnitStatus.ACTIVE).all()

def _unit_members_user_ids(db: Session, unit_ids: List[str]) -> List[str]:
    if not unit_ids: return []
    rows = db.query(UserUnitMemberships.user_id).filter(UserUnitMemberships.unit_id.in_(unit_ids)).distinct().all()
    return [r[0] for r in rows]

def _user_name_map(db: Session, user_ids: List[str]) -> Dict[str, str]:
    if not user_ids: return {}
    rows = db.query(Users.id, func.coalesce(Users.full_name, Users.username, "")).filter(Users.id.in_(user_ids)).all()
    return {r[0]: r[1] for r in rows}

def _is_mgr_phong(db: Session, user: Users) -> bool:
    return user_has_any_role(user, db, [RoleCode.ROLE_TRUONG_PHONG, RoleCode.ROLE_PHO_PHONG])

def _is_mgr_to(db: Session, user: Users) -> bool:
    return user_has_any_role(user, db, [RoleCode.ROLE_TO_TRUONG, RoleCode.ROLE_PHO_TO])

def _is_nv(db: Session, user: Users) -> bool:
    return user_has_any_role(user, db, [RoleCode.ROLE_NHAN_VIEN])

def _is_truong_phong(db: Session, user: Users) -> bool:
    return user_has_any_role(user, db, [RoleCode.ROLE_TRUONG_PHONG])

def _is_pho_phong(db: Session, user: Users) -> bool:
    return user_has_any_role(user, db, [RoleCode.ROLE_PHO_PHONG])

def _is_to_truong(db: Session, user: Users) -> bool:
    return user_has_any_role(user, db, [RoleCode.ROLE_TO_TRUONG])

def _is_pho_to(db: Session, user: Users) -> bool:
    return user_has_any_role(user, db, [RoleCode.ROLE_PHO_TO])

def _is_mgr_khoa(db: Session, user: Users) -> bool:
    return user_has_any_role(user, db, [
        "ROLE_TRUONG_KHOA",
        "ROLE_PHO_TRUONG_KHOA",
        "ROLE_DIEU_DUONG_TRUONG",
        "ROLE_KY_THUAT_VIEN_TRUONG",
    ])

def _is_mgr_donvi(db: Session, user: Users) -> bool:
    return user_has_any_role(user, db, [
        "ROLE_TRUONG_DON_VI",
        "ROLE_PHO_DON_VI",
        "ROLE_DIEU_DUONG_TRUONG_DON_VI",
        "ROLE_KY_THUAT_VIEN_TRUONG_DON_VI",
    ])

def _is_mgr_nhom(db: Session, user: Users) -> bool:
    return user_has_any_role(user, db, [
        "ROLE_TRUONG_NHOM",
        "ROLE_PHO_NHOM",
    ])

def _is_hc_staff(db: Session, user: Users) -> bool:
    return user_has_any_role(user, db, ["ROLE_NHAN_VIEN"])

def _is_cm_staff(db: Session, user: Users) -> bool:
    return user_has_any_role(user, db, [
        "ROLE_BAC_SI",
        "ROLE_DUOC_SI",
        "ROLE_DIEU_DUONG",
        "ROLE_KY_THUAT_VIEN",
        "ROLE_THU_KY_Y_KHOA",
        "ROLE_HO_LY",
        "ROLE_QL_CHAT_LUONG",
        "ROLE_QL_KY_THUAT",
        "ROLE_QL_AN_TOAN",
        "ROLE_QL_VAT_TU",
        "ROLE_QL_TRANG_THIET_BI",
        "ROLE_QL_MOI_TRUONG",
        "ROLE_QL_CNTT",
    ])

_CM_EXEC_ROLE_CODES = {
    "ROLE_GIAM_DOC",
    "ROLE_PHO_GIAM_DOC_TRUC",
    "ROLE_PHO_GIAM_DOC",
}

_CM_SUBUNIT_LEADER_ROLE_CODES = {
    "ROLE_TRUONG_DON_VI",
    "ROLE_PHO_DON_VI",
    "ROLE_DIEU_DUONG_TRUONG_DON_VI",
    "ROLE_KY_THUAT_VIEN_TRUONG_DON_VI",
    "ROLE_TRUONG_NHOM",
    "ROLE_PHO_NHOM",
}

def _user_role_codes(db: Session, user: Users) -> set[str]:
    rows = (
        db.query(Roles.code)
        .join(UserRoles, UserRoles.role_id == Roles.id)
        .filter(UserRoles.user_id == user.id)
        .all()
    )
    out = set()
    for (c,) in rows:
        if c is None:
            continue
        out.add(str(getattr(c, "value", c)).upper())
    return out

def _unit_block_code(unit: Optional[Units]) -> str:
    if not unit:
        return ""
    return str(getattr(getattr(unit, "block_code", None), "value", getattr(unit, "block_code", "")) or "").upper()

def _unit_category_code(unit: Optional[Units]) -> str:
    if not unit:
        return ""
    return str(getattr(getattr(unit, "unit_category", None), "value", getattr(unit, "unit_category", "")) or "").upper()

def _primary_unit_of_user(db: Session, user: Users) -> Optional[Units]:
    mem = (
        db.query(UserUnitMemberships)
        .filter(UserUnitMemberships.user_id == user.id, UserUnitMemberships.is_primary == True)
        .first()
    )
    if mem and getattr(mem, "unit_id", None):
        return db.get(Units, mem.unit_id)

    mem = (
        db.query(UserUnitMemberships)
        .filter(UserUnitMemberships.user_id == user.id)
        .first()
    )
    if mem and getattr(mem, "unit_id", None):
        return db.get(Units, mem.unit_id)
    return None

def _is_cm_exec_viewer(db: Session, user: Users) -> bool:
    return bool(_user_role_codes(db, user) & _CM_EXEC_ROLE_CODES)

def _primary_users_of_unit(db: Session, unit_id: str) -> List[str]:
    rows = (
        db.query(UserUnitMemberships.user_id)
        .filter(
            UserUnitMemberships.unit_id == unit_id,
            UserUnitMemberships.is_primary == True,
        )
        .distinct()
        .all()
    )
    return [r[0] for r in rows if r and r[0]]

def _specialized_personal_user_ids_for_khoa_manager(db: Session, khoa_id: str, self_user_id: str) -> List[str]:
    allowed_ids = set()

    # 1) cá nhân thuộc chính Khoa (không thuộc đơn vị/nhóm con)
    direct_uids = _primary_users_of_unit(db, khoa_id)
    for uid in direct_uids:
        allowed_ids.add(uid)

    # 2) cá nhân là lãnh đạo Đơn vị/Nhóm trực thuộc Khoa
    child_units = _unit_children(db, [khoa_id])
    child_ids = [u.id for u in child_units]
    if child_ids:
        child_primary_user_ids = _primary_users_of_unit(db, child_ids[0]) if len(child_ids) == 1 else []
        if len(child_ids) > 1:
            child_primary_user_ids = []
            for cid in child_ids:
                child_primary_user_ids.extend(_primary_users_of_unit(db, cid))

        child_primary_user_ids = _dedup_ids(child_primary_user_ids)
        if child_primary_user_ids:
            role_rows = (
                db.query(UserRoles.user_id, Roles.code)
                .join(Roles, Roles.id == UserRoles.role_id)
                .filter(UserRoles.user_id.in_(child_primary_user_ids))
                .all()
            )
            role_map: Dict[str, set[str]] = {}
            for uid, code in role_rows:
                role_map.setdefault(uid, set()).add(str(getattr(code, "value", code)).upper())

            for uid in child_primary_user_ids:
                if role_map.get(uid, set()) & _CM_SUBUNIT_LEADER_ROLE_CODES:
                    allowed_ids.add(uid)

    allowed_ids.add(self_user_id)
    return _dedup_ids(list(allowed_ids))
    
def _infer_cm_subunit_kind(db: Session, unit_id: str) -> str:
    """
    Suy loại SUBUNIT chuyên môn của đơn vị hiện tại:
    - DONVI nếu trong đơn vị có các role lãnh đạo Đơn vị
    - NHOM nếu trong đơn vị có các role lãnh đạo Nhóm
    - "" nếu chưa suy được
    """
    if not unit_id:
        return ""

    user_ids = _primary_users_of_unit(db, unit_id)
    if not user_ids:
        return ""

    rows = (
        db.query(UserRoles.user_id, Roles.code)
        .join(Roles, Roles.id == UserRoles.role_id)
        .filter(UserRoles.user_id.in_(user_ids))
        .all()
    )

    donvi_roles = {
        "ROLE_TRUONG_DON_VI",
        "ROLE_PHO_DON_VI",
        "ROLE_DIEU_DUONG_TRUONG_DON_VI",
        "ROLE_KY_THUAT_VIEN_TRUONG_DON_VI",
    }
    nhom_roles = {
        "ROLE_TRUONG_NHOM",
        "ROLE_PHO_NHOM",
    }

    codes = set()
    for _, code in rows:
        if code is None:
            continue
        codes.add(str(getattr(code, "value", code)).upper())

    if codes & donvi_roles:
        return "DONVI"
    if codes & nhom_roles:
        return "NHOM"
    return ""    
    

def _unit_ancestor_ids(db: Session, unit_id: Optional[str]) -> List[str]:
    out: List[str] = []
    current_id = unit_id
    seen = set()

    while current_id:
        unit = db.get(Units, current_id)
        if not unit or not getattr(unit, "parent_id", None):
            break
        parent_id = unit.parent_id
        if parent_id in seen:
            break
        seen.add(parent_id)
        out.append(parent_id)
        current_id = parent_id

    return _dedup_ids(out)


def _unit_descendant_ids(db: Session, root_unit_id: Optional[str]) -> List[str]:
    if not root_unit_id:
        return []

    out: List[str] = []
    frontier = [root_unit_id]
    seen = {root_unit_id}

    while frontier:
        children = _unit_children(db, frontier)
        next_frontier: List[str] = []

        for child in children:
            cid = getattr(child, "id", None)
            if not cid or cid in seen:
                continue
            seen.add(cid)
            out.append(cid)
            next_frontier.append(cid)

        frontier = next_frontier

    return _dedup_ids(out)


def _plans_realtime_user_ids(db: Session, plan: Plans, actor_user_id: Optional[str] = None) -> List[str]:
    user_ids: List[str] = []

    if _plan_scope_of(plan) == PLAN_SCOPE_COMMITTEE:
        committee_id = getattr(plan, "committee_id", None)
        if committee_id:
            rows = (
                db.query(CommitteeMembers.user_id)
                .filter(CommitteeMembers.committee_id == committee_id)
                .filter(CommitteeMembers.is_active == True)  # noqa: E712
                .all()
            )
            user_ids.extend([str(row[0]) for row in rows if row and row[0]])
    else:
        unit_id = getattr(plan, "unit_id", None)
        if unit_id:
            related_unit_ids = _dedup_ids(
                [unit_id] +
                _unit_ancestor_ids(db, unit_id) +
                _unit_descendant_ids(db, unit_id)
            )
            user_ids.extend(_unit_members_user_ids(db, related_unit_ids))

    if getattr(plan, "created_by", None):
        user_ids.append(str(plan.created_by))
    if actor_user_id:
        user_ids.append(str(actor_user_id))

    return _dedup_ids(user_ids)


def _emit_plans_realtime(db: Session, plan: Plans, action: str, actor_user_id: Optional[str] = None) -> None:
    payload = {
        "module": "work",
        "type": "plans_changed",
        "action": str(action or "").strip() or "updated",
        "plan_id": str(getattr(plan, "id", "") or ""),
        "unit_id": str(getattr(plan, "unit_id", "") or ""),
        "scope_type": _plan_scope_of(plan),
        "committee_id": str(getattr(plan, "committee_id", "") or ""),
        "year": int(getattr(plan, "year", 0) or 0),
        "month": int(getattr(plan, "month", 0) or 0),
        "actor_user_id": str(actor_user_id or ""),
        "changed_at": datetime.utcnow().isoformat(),
    }

    user_ids = _plans_realtime_user_ids(db, plan, actor_user_id=actor_user_id)
    if not user_ids:
        return

    try:
        from_thread.run(manager.notify_users_json, user_ids, payload)
    except Exception as ex:
        logger.exception("[plans] Lỗi phát realtime kế hoạch: %s", ex)
    
def _dedup_ids(values: List[str]) -> List[str]:
    out = []
    seen = set()
    for v in values or []:
        if v and v not in seen:
            seen.add(v)
            out.append(v)
    return out



def _normalize_plan_scope(value: Optional[str]) -> str:
    scope = (value or "").strip().upper()
    if scope == PLAN_SCOPE_COMMITTEE:
        return PLAN_SCOPE_COMMITTEE
    return PLAN_SCOPE_UNIT


def _plan_scope_of(plan: Plans) -> str:
    return _normalize_plan_scope(getattr(plan, "scope_type", None))


def _committee_name(db: Session, committee_id: Optional[str]) -> str:
    if not committee_id:
        return ""
    row = db.get(Committees, committee_id)
    return (getattr(row, "name", "") or "") if row else ""


def _fallback_plan_unit_id_for_user(db: Session, user: Users) -> Optional[str]:
    primary_unit = _primary_unit_of_user(db, user)
    if primary_unit and getattr(primary_unit, "id", None):
        return primary_unit.id

    mem = (
        db.query(UserUnitMemberships)
        .filter(UserUnitMemberships.user_id == user.id)
        .first()
    )
    return getattr(mem, "unit_id", None) if mem else None


def _get_manager_committee_ids_for_plans(db: Session, user_id: str) -> List[str]:
    rows = (
        db.query(CommitteeMembers.committee_id)
        .join(Committees, Committees.id == CommitteeMembers.committee_id)
        .filter(CommitteeMembers.user_id == user_id)
        .filter(CommitteeMembers.is_active == True)  # noqa: E712
        .filter(CommitteeMembers.committee_role.in_(list(COMMITTEE_MANAGER_ROLES)))
        .filter(Committees.status == "ACTIVE", Committees.is_active == True)  # noqa: E712
        .filter(Committees.allow_plans == True)  # noqa: E712
        .distinct()
        .all()
    )
    return [str(row[0]) for row in rows if row and row[0]]


def _get_member_committee_ids_for_plans(db: Session, user_id: str) -> List[str]:
    rows = (
        db.query(CommitteeMembers.committee_id)
        .join(Committees, Committees.id == CommitteeMembers.committee_id)
        .filter(CommitteeMembers.user_id == user_id)
        .filter(CommitteeMembers.is_active == True)  # noqa: E712
        .filter(Committees.status == "ACTIVE", Committees.is_active == True)  # noqa: E712
        .filter(Committees.allow_plans == True)  # noqa: E712
        .distinct()
        .all()
    )
    return [str(row[0]) for row in rows if row and row[0]]


def _user_can_create_committee_plan(db: Session, user_id: str, committee_id: str) -> bool:
    committee_id = (committee_id or "").strip()
    if not committee_id:
        return False

    committee = db.get(Committees, committee_id)
    if not committee:
        return False
    if (committee.status or "").upper() != "ACTIVE":
        return False
    if not bool(getattr(committee, "is_active", False)):
        return False
    if not bool(getattr(committee, "allow_plans", True)):
        return False

    row = (
        db.query(CommitteeMembers.id)
        .filter(CommitteeMembers.committee_id == committee_id)
        .filter(CommitteeMembers.user_id == user_id)
        .filter(CommitteeMembers.is_active == True)  # noqa: E712
        .filter(CommitteeMembers.committee_role.in_(list(COMMITTEE_MANAGER_ROLES)))
        .first()
    )
    return row is not None


def _user_can_view_committee_plan(db: Session, user: Users, plan: Plans) -> bool:
    committee_id = getattr(plan, "committee_id", None)
    if not committee_id:
        return False

    # Người tạo luôn được xem kế hoạch mình tạo.
    if getattr(plan, "created_by", None) == user.id:
        return True

    # Thành viên Ban được xem kế hoạch của Ban.
    member_row = (
        db.query(CommitteeMembers.id)
        .join(Committees, Committees.id == CommitteeMembers.committee_id)
        .filter(CommitteeMembers.committee_id == committee_id)
        .filter(CommitteeMembers.user_id == user.id)
        .filter(CommitteeMembers.is_active == True)  # noqa: E712
        .filter(Committees.status == "ACTIVE", Committees.is_active == True)  # noqa: E712
        .filter(Committees.allow_plans == True)  # noqa: E712
        .first()
    )
    if member_row:
        return True

    # HĐTV/Admin xem được toàn bộ theo quyền all units hiện tại.
    if is_all_units_access(db, user):
        return True

    # BGĐ không tự động thấy toàn bộ Ban; chỉ thấy Ban managed_by = BGD.
    role_codes = _user_role_codes(db, user)
    if bool(role_codes & _CM_EXEC_ROLE_CODES):
        committee = db.get(Committees, committee_id)
        if committee and (getattr(committee, "managed_by", "") or "").upper() == "BGD":
            return True

    return False


def _get_committee_plan_options(db: Session, user: Users) -> List[Committees]:
    committee_ids = _get_manager_committee_ids_for_plans(db, user.id)
    if not committee_ids:
        return []

    return (
        db.query(Committees)
        .filter(Committees.id.in_(committee_ids))
        .filter(Committees.status == "ACTIVE", Committees.is_active == True)  # noqa: E712
        .filter(Committees.allow_plans == True)  # noqa: E712
        .order_by(Committees.name.asc())
        .all()
    )


def _get_viewable_committee_plan_ids(db: Session, user: Users) -> List[str]:
    committee_ids = set(_get_member_committee_ids_for_plans(db, user.id))

    if is_all_units_access(db, user):
        rows = (
            db.query(Committees.id)
            .filter(Committees.status == "ACTIVE", Committees.is_active == True)  # noqa: E712
            .filter(Committees.allow_plans == True)  # noqa: E712
            .all()
        )
        committee_ids.update(str(row[0]) for row in rows if row and row[0])

    elif bool(_user_role_codes(db, user) & _CM_EXEC_ROLE_CODES):
        rows = (
            db.query(Committees.id)
            .filter(Committees.status == "ACTIVE", Committees.is_active == True)  # noqa: E712
            .filter(Committees.allow_plans == True)  # noqa: E712
            .filter(Committees.managed_by == "BGD")
            .all()
        )
        committee_ids.update(str(row[0]) for row in rows if row and row[0])

    return _dedup_ids(list(committee_ids))


def _decorate_plan_scope(db: Session, plan: Plans) -> None:
    scope = _plan_scope_of(plan)
    setattr(plan, "_scope_type", scope)

    if scope == PLAN_SCOPE_COMMITTEE:
        setattr(plan, "_scope_label", "Ban kiêm nhiệm")
        setattr(plan, "_scope_name", _committee_name(db, getattr(plan, "committee_id", None)))
    else:
        setattr(plan, "_scope_label", "Đơn vị")
        unit_name = ""
        try:
            unit = getattr(plan, "unit", None)
            if unit is None and getattr(plan, "unit_id", None):
                unit = db.get(Units, plan.unit_id)
            unit_name = getattr(unit, "ten_don_vi", "") or ""
        except Exception:
            unit_name = ""
        setattr(plan, "_scope_name", unit_name)

def _filter_personal_plan_user_ids_for_manager(db: Session, manager: Users, user_ids: List[str]) -> List[str]:
    user_ids = _dedup_ids(user_ids)

    # Trưởng phòng / Tổ trưởng: được xem kế hoạch cá nhân của toàn bộ phạm vi thuộc quyền
    if _is_truong_phong(db, manager) or _is_to_truong(db, manager):
        return user_ids

    filtered = []
    for uid in user_ids:
        target = db.get(Users, uid)
        if not target:
            continue

        # Phó phòng không được xem kế hoạch cá nhân của Trưởng phòng
        if _is_pho_phong(db, manager) and _is_truong_phong(db, target):
            continue

        # Tổ phó không được xem kế hoạch cá nhân của Tổ trưởng
        if _is_pho_to(db, manager) and _is_to_truong(db, target):
            continue

        filtered.append(uid)

    return filtered
    
def _user_membership_unit_ids(db: Session, user: Users) -> List[str]:
    rows = (
        db.query(UserUnitMemberships.unit_id)
        .filter(UserUnitMemberships.user_id == user.id)
        .distinct()
        .all()
    )
    return [r[0] for r in rows if r and r[0]]


def _active_visibility_modes(db: Session, user: Users) -> set[str]:
    unit_ids = _user_membership_unit_ids(db, user)
    if not unit_ids:
        return set()

    now = datetime.utcnow()
    grants = (
        db.query(VisibilityGrants)
        .filter(VisibilityGrants.grantee_unit_id.in_(unit_ids))
        .all()
    )

    modes = set()
    for g in grants:
        if g.effective_from and g.effective_from > now:
            continue
        if g.effective_to and g.effective_to < now:
            continue
        mode_val = getattr(g.mode, "value", g.mode)
        if mode_val:
            modes.add(str(mode_val).upper())
    return modes


def _has_plan_visibility_grant(db: Session, user: Users) -> bool:
    modes = _active_visibility_modes(db, user)
    return ("VIEW_ALL" in modes) or ("PLANS_ONLY" in modes)   

def _pad2(n: Optional[str]) -> Optional[str]:
    if not n: return None
    s = str(n).strip()
    if not s.isdigit(): return None
    return s if len(s) == 2 else ("0"+s)[-2:]

def _compose_date(y: Optional[str], m: Optional[str], d: Optional[str]) -> Optional[str]:
    y = (y or "").strip()
    m = _pad2(m); d = _pad2(d)
    if not (y and m and d): return None
    try:
        datetime.strptime(f"{y}-{m}-{d}", "%Y-%m-%d")
        return f"{y}-{m}-{d}"
    except Exception:
        return None

_PLAN_ITEM_STATUS_LABELS = [
    "Chưa thực hiện",
    "Mới triển khai bước đầu",
    "Đang thực hiện",
    "Đã hoàn thành",
    "Chuyển kỳ sau",
]

def _normalize_item_status(status_txt: Optional[str]) -> str:
    val = (status_txt or "").strip()
    if not val:
        return "Chưa thực hiện"
    if val == "Chưa hoàn thành":
        return "Mới triển khai bước đầu"
    if val not in _PLAN_ITEM_STATUS_LABELS:
        return "Chưa thực hiện"
    return val

def _inject_item_tags(content: str, start: Optional[str], end: Optional[str], status_txt: Optional[str]) -> str:
    st_norm = _normalize_item_status(status_txt)
    parts = []
    if start:
        parts.append(f"[[START={start}]]")
    if end:
        parts.append(f"[[END={end}]]")
    parts.append(f"[[STATUS={st_norm}]]")
    return ("".join(parts) + " " + (content or "").strip()).strip()

def _extract_period_and_status_from_content(content: str, fallback_due: Optional[datetime]) -> (str, str):
    start, end, st = None, None, ""
    try:
        import re
        m = re.search(r"\[\[START=([0-9]{4}-[0-9]{2}-[0-9]{2})\]\]", content or "")
        start = m.group(1) if m else None

        m = re.search(r"\[\[END=([0-9]{4}-[0-9]{2}-[0-9]{2})\]\]", content or "")
        end = m.group(1) if m else None

        m = re.search(r"\[\[STATUS=([^\]]+)\]\]", content or "")
        st = m.group(1).strip() if m else ""
    except Exception:
        pass

    def _fmt(d):
        try:
            return datetime.strptime(d, "%Y-%m-%d").strftime("%d-%m-%Y")
        except Exception:
            return None

    if start or end:
        s = _fmt(start) if start else "?"
        e = _fmt(end) if end else "?"
        period = f"Từ {s or '-'} đến {e or '-'}"
    else:
        period = fallback_due.strftime("%d-%m-%Y") if fallback_due else "-"

    return period, _normalize_item_status(st)

def _strip_tags_for_display(content: str) -> str:
    try:
        import re
        return re.sub(r"\[\[(START|END|STATUS)=[^\]]+\]\]\s*", "", content or "").strip()
    except Exception:
        return (content or "").strip()

def _next_year_month(year: int, month: int) -> (int, int):
    y = int(year)
    m = int(month)
    if m >= 12:
        return y + 1, 1
    return y, m + 1

def _ensure_next_period_plan(db: Session, p: Plans) -> Plans:
    next_year, next_month = _next_year_month(int(p.year), int(p.month))

    existed = (
        db.query(Plans)
        .filter(
            Plans.unit_id == p.unit_id,
            Plans.scope_type == _plan_scope_of(p),
            Plans.committee_id == getattr(p, "committee_id", None),
            Plans.year == next_year,
            Plans.month == next_month,
            Plans.created_by == p.created_by,
            Plans.plan_kind == p.plan_kind,
            Plans.title == p.title,
        )
        .first()
    )
    if existed:
        return existed

    new_plan = Plans(
        unit_id=p.unit_id,
        scope_type=_plan_scope_of(p),
        committee_id=getattr(p, "committee_id", None),
        year=next_year,
        month=next_month,
        title=p.title,
        description=p.description,
        plan_kind=p.plan_kind,
        status=PlanStatus.DRAFT,
        created_by=p.created_by,
        approved_by=None,
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow(),
    )
    db.add(new_plan)
    db.flush()
    return new_plan

def _remove_auto_carried_forward_item(db: Session, current_plan: Plans, work_key: Optional[str], source_item_id: Optional[str]) -> None:
    if not work_key or not source_item_id:
        return

    next_year, next_month = _next_year_month(int(current_plan.year), int(current_plan.month))
    next_plan = (
        db.query(Plans)
        .filter(
            Plans.unit_id == current_plan.unit_id,
            Plans.scope_type == _plan_scope_of(current_plan),
            Plans.committee_id == getattr(current_plan, "committee_id", None),
            Plans.year == next_year,
            Plans.month == next_month,
            Plans.created_by == current_plan.created_by,
            Plans.plan_kind == current_plan.plan_kind,
            Plans.title == current_plan.title,
        )
        .first()
    )
    if not next_plan:
        return

    row = (
        db.query(PlanItems)
        .filter(
            PlanItems.plan_id == next_plan.id,
            PlanItems.work_key == work_key,
            PlanItems.source_item_id == source_item_id,
        )
        .first()
    )
    if not row:
        return

    _, st = _extract_period_and_status_from_content(row.content or "", getattr(row, "due_date", None))
    if _normalize_item_status(st) == "Chưa thực hiện":
        db.delete(row)

def _build_plan_item(
    plan_id: str,
    content: str,
    start: Optional[str],
    end: Optional[str],
    status_txt: Optional[str],
    work_key: Optional[str] = None,
    source_item_id: Optional[str] = None,
    carried_forward_count: int = 0,
    was_ever_carried_forward: bool = False,
) -> PlanItems:
    st_norm = _normalize_item_status(status_txt)
    return PlanItems(
        plan_id=plan_id,
        content=_inject_item_tags(content, start, end, st_norm),
        due_date=datetime.strptime(end, "%Y-%m-%d") if end else None,
        work_key=(work_key or str(uuid.uuid4())),
        source_item_id=source_item_id,
        carried_forward_count=int(carried_forward_count or 0),
        was_ever_carried_forward=bool(was_ever_carried_forward),
    )

def _auto_create_next_period_item(db: Session, current_plan: Plans, current_item: PlanItems) -> None:
    _, st = _extract_period_and_status_from_content(current_item.content or "", getattr(current_item, "due_date", None))
    if _normalize_item_status(st) != "Chuyển kỳ sau":
        return

    next_plan = _ensure_next_period_plan(db, current_plan)

    existed = (
        db.query(PlanItems)
        .filter(
            PlanItems.plan_id == next_plan.id,
            PlanItems.work_key == current_item.work_key,
        )
        .first()
    )
    if existed:
        existed.source_item_id = current_item.id
        existed.carried_forward_count = int(current_item.carried_forward_count or 0)
        existed.was_ever_carried_forward = True
        db.add(existed)
        return

    new_item = _build_plan_item(
        plan_id=next_plan.id,
        content=_strip_tags_for_display(current_item.content or ""),
        start=None,
        end=None,
        status_txt="Chưa thực hiện",
        work_key=current_item.work_key,
        source_item_id=current_item.id,
        carried_forward_count=int(current_item.carried_forward_count or 0),
        was_ever_carried_forward=True,
    )
    db.add(new_item)

# ===== Routes =====
@router.get("")
def plans_home(request: Request, db: Session = Depends(get_db)):
    user = login_required(request, db)
    now = datetime.utcnow()

    filter_type = request.query_params.get("filter_type") or ""
    filter_id   = request.query_params.get("filter_id") or ""
    selected_plan_id = request.query_params.get("plan_id") or ""
    selected_kind    = request.query_params.get("kind") or ""

    q_year = (request.query_params.get("year") or "").strip()
    q_month = (request.query_params.get("month") or "").strip()

    y = None
    m = None

    if q_year:
        try:
            y = int(q_year)
        except Exception:
            y = None

    if q_month:
        try:
            m = int(q_month)
        except Exception:
            m = None

    if selected_plan_id and (y is None or m is None):
        try:
            selected_plan = db.get(Plans, selected_plan_id)
            if selected_plan:
                if y is None:
                    y = int(selected_plan.year)
                if m is None:
                    m = int(selected_plan.month)
        except Exception:
            pass

    display_year = y if y is not None else now.year
    display_month = m if m is not None else now.month

    native_all_access = is_all_units_access(db, user)
    grant_all_plan_access = _has_plan_visibility_grant(db, user)

    # Chỉ dùng cho BÀI TOÁN XEM KẾ HOẠCH
    can_view_all_plans = native_all_access or grant_all_plan_access

    scope_ids = list(accessible_unit_ids(db, user))

    is_hc_mgr_phong = _is_mgr_phong(db, user)
    is_hc_mgr_to = _is_mgr_to(db, user)

    is_cm_mgr_khoa = _is_mgr_khoa(db, user)
    is_cm_mgr_donvi = _is_mgr_donvi(db, user)
    is_cm_mgr_nhom = _is_mgr_nhom(db, user)

    is_mgr_phong = bool(is_hc_mgr_phong or is_cm_mgr_khoa)
    is_mgr_to = bool(is_hc_mgr_to or is_cm_mgr_donvi or is_cm_mgr_nhom)

    is_hc_staff = _is_hc_staff(db, user) and (not is_mgr_phong) and (not is_mgr_to)
    is_cm_staff = _is_cm_staff(db, user) and (not is_mgr_phong) and (not is_mgr_to)

    # Phân loại quyền gốc cho UI/thao tác, KHÔNG để grant biến user thành HĐTV/manager
    is_staff = (not native_all_access) and (is_hc_staff or is_cm_staff)

    prims = _user_primary_units(db, user)

    role_codes = _user_role_codes(db, user)
    primary_unit = _primary_unit_of_user(db, user)
    primary_parent_unit = _parent_unit(db, primary_unit.id) if primary_unit else None
    primary_block_code = _unit_block_code(primary_unit)
    primary_category_code = _unit_category_code(primary_unit)
    primary_cm_subunit_kind = (
        _infer_cm_subunit_kind(db, primary_unit.id)
        if primary_unit and primary_block_code == "CHUYEN_MON" and primary_category_code == "SUBUNIT"
        else ""
    )

    is_cm_exec = _is_cm_exec_viewer(db, user)

    committee_plan_options = _get_committee_plan_options(db, user)
    viewable_committee_ids = _get_viewable_committee_plan_ids(db, user)

    createable_units: List[Units] = []
    visible_units: List[Units] = []

    if native_all_access:
        visible_units = (
            db.query(Units)
            .filter(Units.id.in_(scope_ids))
            .order_by(Units.cap_do, Units.order_index)
            .all()
        )
        createable_units = visible_units

    else:
        if is_mgr_phong:
            phong_ids = []
            for u in prims:
                if getattr(u, "cap_do", None) == 2:
                    phong_ids.append(u.id)
                elif getattr(u, "cap_do", None) == 3 and getattr(u, "parent_id", None):
                    phong_ids.append(u.parent_id)

            phong_ids = _dedup_ids(phong_ids)
            phong_list = db.query(Units).filter(Units.id.in_(phong_ids)).all()
            to_list = _unit_children(db, phong_ids)

            visible_units = sorted((phong_list + to_list), key=lambda u: (u.cap_do, u.order_index))
            createable_units = visible_units

        elif is_mgr_to:
            to_ids = []
            phong_ids = []

            for u in prims:
                if getattr(u, "cap_do", None) == 3:
                    to_ids.append(u.id)
                    if getattr(u, "parent_id", None):
                        phong_ids.append(u.parent_id)

            to_ids = _dedup_ids(to_ids)
            phong_ids = _dedup_ids(phong_ids)

            phong_list = db.query(Units).filter(Units.id.in_(phong_ids)).all() if phong_ids else []
            to_list = db.query(Units).filter(Units.id.in_(to_ids)).all() if to_ids else []

            # Bộ lọc được xem cả phòng mẹ + tổ mình
            visible_units = sorted((phong_list + to_list), key=lambda u: (u.cap_do, u.order_index))

            # Modal tạo kế hoạch chỉ được chọn đúng tổ của mình
            createable_units = sorted(to_list, key=lambda u: (u.cap_do, u.order_index))

        else:
            ids = []
            create_ids = []

            for u in prims:
                create_ids.append(u.id)
                ids.append(u.id)

                p = _parent_unit(db, u.id)
                if p:
                    ids.append(p.id)

            ids = _dedup_ids(ids)
            create_ids = _dedup_ids(create_ids)

            if ids:
                visible_units = (
                    db.query(Units)
                    .filter(Units.id.in_(ids), Units.cap_do != 1)
                    .order_by(Units.cap_do, Units.order_index)
                    .all()
                )

            if create_ids:
                createable_units = (
                    db.query(Units)
                    .filter(Units.id.in_(create_ids), Units.cap_do != 1)
                    .order_by(Units.cap_do, Units.order_index)
                    .all()
                )

    # ===== Override siết riêng cho KHỐI CHUYÊN MÔN =====
    if primary_block_code == "CHUYEN_MON":
        visible_unit_ids: List[str] = []
        createable_unit_ids: List[str] = []

        # BGĐ: xem Khoa và các đơn vị/nhóm trực thuộc Khoa.
        # Không return sớm, để user kiêm nhiệm BGĐ + Trưởng khoa vẫn cộng thêm phạm vi Trưởng khoa.
        if is_cm_exec:
            khoa_units = (
                db.query(Units)
                .filter(
                    Units.trang_thai == UnitStatus.ACTIVE,
                    Units.parent_id.isnot(None),
                )
                .all()
            )
            for u in khoa_units:
                if _unit_category_code(u) == "KHOA":
                    visible_unit_ids.append(u.id)
                    visible_unit_ids.extend([child.id for child in _unit_children(db, [u.id])])

        # Trưởng/Phó khoa: xem Khoa của mình và đơn vị/nhóm trực thuộc; được tạo kế hoạch cấp Khoa.
        if is_cm_mgr_khoa and primary_unit and primary_category_code == "KHOA":
            child_units = _unit_children(db, [primary_unit.id])
            visible_unit_ids.append(primary_unit.id)
            visible_unit_ids.extend([child.id for child in child_units])
            createable_unit_ids.append(primary_unit.id)

        # Trưởng/Phó đơn vị, Điều dưỡng/KTV trưởng đơn vị, Trưởng/Phó nhóm:
        # xem đơn vị/nhóm của mình và Khoa mẹ; được tạo kế hoạch ở đơn vị/nhóm của mình.
        if (is_cm_mgr_donvi or is_cm_mgr_nhom) and primary_unit and primary_category_code == "SUBUNIT":
            if primary_parent_unit:
                visible_unit_ids.append(primary_parent_unit.id)
            visible_unit_ids.append(primary_unit.id)
            createable_unit_ids.append(primary_unit.id)

        # Nhân sự chuyên môn: xem Khoa mẹ và đơn vị hiện tại; tạo kế hoạch cá nhân tại đơn vị chính.
        if is_cm_staff and not (is_cm_mgr_khoa or is_cm_mgr_donvi or is_cm_mgr_nhom):
            if primary_parent_unit and _unit_category_code(primary_parent_unit) == "KHOA":
                visible_unit_ids.append(primary_parent_unit.id)
            if primary_unit:
                visible_unit_ids.append(primary_unit.id)
                createable_unit_ids.append(primary_unit.id)

        visible_unit_ids = _dedup_ids(visible_unit_ids)
        createable_unit_ids = _dedup_ids(createable_unit_ids)

        visible_units = (
            db.query(Units)
            .filter(Units.id.in_(visible_unit_ids))
            .order_by(Units.cap_do, Units.order_index)
            .all()
            if visible_unit_ids else []
        )
        createable_units = (
            db.query(Units)
            .filter(Units.id.in_(createable_unit_ids))
            .order_by(Units.cap_do, Units.order_index)
            .all()
            if createable_unit_ids else []
        )

    # Nhân sự khả kiến cho bộ lọc thao tác theo phạm vi gốc
    filter_users: List[Dict] = []

    if primary_block_code == "CHUYEN_MON":
        if is_cm_mgr_khoa and primary_unit and primary_category_code == "KHOA":
            uids = _specialized_personal_user_ids_for_khoa_manager(db, primary_unit.id, user.id)
            uids = [uid for uid in uids if uid != user.id]
            name_map = _user_name_map(db, uids)
            filter_users = [{"id": uid, "full_name": name_map.get(uid, "")} for uid in sorted(uids)]
        else:
            filter_users = []
    else:
        if not native_all_access:
            if is_mgr_phong:
                unit_ids = [u.id for u in visible_units]
                uids = [uid for uid in _unit_members_user_ids(db, unit_ids) if uid != user.id]
                uids = _filter_personal_plan_user_ids_for_manager(db, user, uids)
                name_map = _user_name_map(db, uids)
                filter_users = [{"id": uid, "full_name": name_map.get(uid, "")} for uid in sorted(uids)]

            elif is_mgr_to:
                to_ids = [u.id for u in prims if getattr(u, "cap_do", None) == 3]
                uids = [uid for uid in _unit_members_user_ids(db, to_ids) if uid != user.id]
                uids = _filter_personal_plan_user_ids_for_manager(db, user, uids)
                name_map = _user_name_map(db, uids)
                filter_users = [{"id": uid, "full_name": name_map.get(uid, "")} for uid in sorted(uids)]
        else:
            filter_users = []

    # Lấy kế hoạch
    q = db.query(Plans)
    if y is not None:
        q = q.filter(Plans.year == y)
    if m is not None:
        q = q.filter(Plans.month == m)

    if not can_view_all_plans:
        if is_staff:
            phong_ids = []
            to_ids = []

            for u in prims:
                if getattr(u, "cap_do", None) == 3:
                    to_ids.append(u.id)
                    if getattr(u, "parent_id", None):
                        phong_ids.append(u.parent_id)
                elif getattr(u, "cap_do", None) == 2:
                    phong_ids.append(u.id)

            phong_ids = _dedup_ids(phong_ids)
            to_ids = _dedup_ids(to_ids)

            q = q.filter(
                ((Plans.plan_kind == "PHONG") & (Plans.unit_id.in_(phong_ids))) |
                ((Plans.plan_kind == "KHOA") & (Plans.unit_id.in_(phong_ids))) |
                ((Plans.plan_kind == "TO") & (Plans.unit_id.in_(to_ids))) |
                ((Plans.plan_kind == "DONVI") & (Plans.unit_id.in_(to_ids))) |
                ((Plans.plan_kind == "NHOM") & (Plans.unit_id.in_(to_ids))) |
                ((Plans.plan_kind == "NHANVIEN") & (Plans.created_by == user.id))
            )

        elif is_mgr_to:
            to_ids = []
            phong_ids = []

            for u in prims:
                if getattr(u, "cap_do", None) == 3:
                    to_ids.append(u.id)
                    if getattr(u, "parent_id", None):
                        phong_ids.append(u.parent_id)

            to_ids = _dedup_ids(to_ids)
            phong_ids = _dedup_ids(phong_ids)

            member_ids = _unit_members_user_ids(db, to_ids)
            member_ids = _filter_personal_plan_user_ids_for_manager(db, user, member_ids)

            q = q.filter(
                ((Plans.plan_kind == "PHONG") & (Plans.unit_id.in_(phong_ids))) |
                ((Plans.plan_kind == "KHOA") & (Plans.unit_id.in_(phong_ids))) |
                ((Plans.plan_kind == "TO") & (Plans.unit_id.in_(to_ids))) |
                ((Plans.plan_kind == "DONVI") & (Plans.unit_id.in_(to_ids))) |
                ((Plans.plan_kind == "NHOM") & (Plans.unit_id.in_(to_ids))) |
                ((Plans.plan_kind == "NHANVIEN") & (Plans.created_by.in_(member_ids)))
            )

        elif is_mgr_phong:
            phong_ids = []
            for u in prims:
                if getattr(u, "cap_do", None) == 2:
                    phong_ids.append(u.id)
                elif getattr(u, "cap_do", None) == 3 and getattr(u, "parent_id", None):
                    phong_ids.append(u.parent_id)

            phong_ids = _dedup_ids(phong_ids)
            to_list = _unit_children(db, phong_ids)
            to_ids = _dedup_ids([t.id for t in to_list])

            all_unit_ids = _dedup_ids(phong_ids + to_ids)
            member_ids = _unit_members_user_ids(db, all_unit_ids)
            member_ids = _filter_personal_plan_user_ids_for_manager(db, user, member_ids)

            q = q.filter(
                ((Plans.plan_kind == "PHONG") & (Plans.unit_id.in_(phong_ids))) |
                ((Plans.plan_kind == "KHOA") & (Plans.unit_id.in_(phong_ids))) |
                ((Plans.plan_kind == "TO") & (Plans.unit_id.in_(to_ids))) |
                ((Plans.plan_kind == "DONVI") & (Plans.unit_id.in_(to_ids))) |
                ((Plans.plan_kind == "NHOM") & (Plans.unit_id.in_(to_ids))) |
                ((Plans.plan_kind == "NHANVIEN") & (Plans.created_by.in_(member_ids)))
            )

        else:
            q = q.filter((Plans.plan_kind == "NHANVIEN") & (Plans.created_by == user.id))

    # ===== Override query xem kế hoạch cho KHỐI CHUYÊN MÔN =====
    if primary_block_code == "CHUYEN_MON":
        q = db.query(Plans)
        if y is not None:
            q = q.filter(Plans.year == y)
        if m is not None:
            q = q.filter(Plans.month == m)

        khoa_id = None
        subunit_id = None

        if primary_unit:
            if primary_category_code == "KHOA":
                khoa_id = primary_unit.id
            elif primary_category_code == "SUBUNIT":
                subunit_id = primary_unit.id
                if primary_parent_unit and _unit_category_code(primary_parent_unit) == "KHOA":
                    khoa_id = primary_parent_unit.id

        cm_conditions = []

        # Vai trò BGĐ: xem kế hoạch Khoa và Đơn vị toàn khối chuyên môn.
        # Không chặn vai trò Trưởng khoa nếu user đang kiêm nhiệm.
        if is_cm_exec:
            cm_conditions.append(
                (Plans.plan_kind == "KHOA") |
                (Plans.plan_kind == "DONVI")
            )

        # Vai trò Trưởng/Phó khoa: xem Khoa mình, đơn vị/nhóm thuộc Khoa mình, kế hoạch cá nhân trong phạm vi Khoa.
        if is_cm_mgr_khoa and khoa_id:
            child_ids = [u.id for u in _unit_children(db, [khoa_id])]
            personal_ids = _specialized_personal_user_ids_for_khoa_manager(db, khoa_id, user.id)
            cm_conditions.append(
                ((Plans.plan_kind == "KHOA") & (Plans.unit_id == khoa_id)) |
                ((Plans.plan_kind.in_(["DONVI", "NHOM"])) & (Plans.unit_id.in_(child_ids))) |
                ((Plans.plan_kind == "NHANVIEN") & (Plans.created_by.in_(personal_ids)))
            )

        # Vai trò đơn vị/nhóm hoặc nhân sự thường: giữ phạm vi hiện có.
        if not cm_conditions:
            own_subunit_ids = [subunit_id] if subunit_id else []
            cm_conditions.append(
                ((Plans.plan_kind == "KHOA") & (Plans.unit_id == khoa_id)) |
                ((Plans.plan_kind.in_(["DONVI", "NHOM"])) & (Plans.unit_id.in_(own_subunit_ids))) |
                ((Plans.plan_kind == "NHANVIEN") & (Plans.created_by == user.id))
            )

        cm_filter = None
        for condition in cm_conditions:
            cm_filter = condition if cm_filter is None else (cm_filter | condition)

        if cm_filter is not None:
            q = q.filter(cm_filter)

    if filter_type == "unit" and filter_id:
        q = q.filter(Plans.unit_id == filter_id)
    elif filter_type == "user" and filter_id:
        q = q.filter(Plans.created_by == filter_id)
    elif filter_type == "committee" and filter_id:
        q = q.filter(
            Plans.scope_type == PLAN_SCOPE_COMMITTEE,
            Plans.committee_id == filter_id,
        )

    if filter_type == "unit" and filter_id:
        uids = _unit_members_user_ids(db, [filter_id])
        q = q.filter((Plans.unit_id == filter_id) | (Plans.created_by.in_(uids)))
    elif filter_type == "user" and filter_id:
        q = q.filter(Plans.created_by == filter_id)
    elif filter_type == "committee" and filter_id:
        q = q.filter(
            Plans.scope_type == PLAN_SCOPE_COMMITTEE,
            Plans.committee_id == filter_id,
        )

    if selected_kind == "phong":
        q = q.filter(Plans.plan_kind == "PHONG")
    elif selected_kind == "to":
        q = q.filter(Plans.plan_kind == "TO")
    elif selected_kind == "khoa":
        q = q.filter(Plans.plan_kind == "KHOA")
    elif selected_kind == "donvi":
        q = q.filter(Plans.plan_kind == "DONVI")
    elif selected_kind == "nhom":
        q = q.filter(Plans.plan_kind == "NHOM")
    elif selected_kind == "nhanvien":
        q = q.filter(Plans.plan_kind == "NHANVIEN")
    elif selected_kind == "committee":
        q = q.filter(Plans.scope_type == PLAN_SCOPE_COMMITTEE)

    unit_plans = q.order_by(Plans.created_at.desc()).all()

    committee_plans: List[Plans] = []
    if viewable_committee_ids and filter_type not in {"unit", "user"}:
        committee_query = db.query(Plans)
        if y is not None:
            committee_query = committee_query.filter(Plans.year == y)
        if m is not None:
            committee_query = committee_query.filter(Plans.month == m)

        committee_query = committee_query.filter(
            Plans.scope_type == PLAN_SCOPE_COMMITTEE,
            Plans.committee_id.in_(viewable_committee_ids),
        )

        if filter_type == "committee" and filter_id:
            committee_query = committee_query.filter(Plans.committee_id == filter_id)

        if selected_kind and selected_kind != "committee":
            committee_query = committee_query.filter(Plans.id.is_(None))

        committee_plans = committee_query.order_by(Plans.created_at.desc()).all()

    plan_map: Dict[str, Plans] = {}
    for plan in unit_plans + committee_plans:
        if getattr(plan, "id", None):
            plan_map[str(plan.id)] = plan

    plans = sorted(
        plan_map.values(),
        key=lambda item: getattr(item, "created_at", None) or datetime.min,
        reverse=True,
    )

    # Gắn items để hiển thị chi tiết
    creator_ids = list({p.created_by for p in plans if p.created_by})
    name_map = _user_name_map(db, creator_ids)
    for p in plans:
        _decorate_plan_scope(db, p)
        p._creator_name = name_map.get(p.created_by, "")
        items = db.query(PlanItems).filter(PlanItems.plan_id == p.id).all()
        for it in items:
            vis = _strip_tags_for_display(it.content or "")
            it._content_visible = vis if vis else (it.content or "").strip()
            period, st = _extract_period_and_status_from_content(it.content or "", getattr(it, "due_date", None))
            it._period_label = period
            it._status_label = st
        setattr(p, "items", items)

    filterable_entities = {
        "units": [{"id": u.id, "ten_don_vi": u.ten_don_vi} for u in visible_units],
        "users": filter_users,
        "committees": [
            {"id": c.id, "name": c.name}
            for c in (
                db.query(Committees)
                .filter(Committees.id.in_(viewable_committee_ids))
                .order_by(Committees.name.asc())
                .all()
                if viewable_committee_ids else []
            )
        ],
    }

    return templates.TemplateResponse("plans.html", {
        "request": request,
        "app_name": settings.APP_NAME,
        "company_name": settings.COMPANY_NAME,
        "year": display_year,
        "month": display_month,
        "plans": plans,
        "user": user,
        "_is_hdtv": native_all_access,
        "_can_view_all_plans": can_view_all_plans,
        "_is_manager": bool(is_mgr_phong or is_mgr_to),
        "_is_manager_phong": is_mgr_phong,
        "_is_manager_to": is_mgr_to,
        "_is_staff": is_staff,

        "_is_hc_mgr_phong": is_hc_mgr_phong,
        "_is_hc_mgr_to": is_hc_mgr_to,
        "_is_cm_mgr_khoa": is_cm_mgr_khoa,
        "_is_cm_mgr_donvi": is_cm_mgr_donvi,
        "_is_cm_mgr_nhom": is_cm_mgr_nhom,
        "_is_hc_staff": is_hc_staff,
        "_is_cm_staff": is_cm_staff,
        "_is_cm_exec": is_cm_exec,
        "_primary_block_code": primary_block_code,
        "_primary_category_code": primary_category_code,
        "_primary_cm_subunit_kind": primary_cm_subunit_kind,
        
        "_edit_mode": (request.query_params.get("mode") or "") == "edit",
        "filterable_entities": filterable_entities,
        "createable_units": [{"id": u.id, "ten_don_vi": u.ten_don_vi} for u in createable_units],
        "committee_plan_options": [{"id": c.id, "name": c.name} for c in committee_plan_options],
        "has_committee_plan_scope": bool(committee_plan_options),
        "selected_filter_type": filter_type,
        "selected_filter_id": filter_id,
        "selected_plan_id": request.query_params.get("plan_id") or "",
        "selected_kind": selected_kind,
    })

@router.get("/details/{plan_id}", name="plan_details")
def plan_details(request: Request, plan_id: str, db: Session = Depends(get_db)):
    user = login_required(request, db)
    p = db.get(Plans, plan_id)
    if not p:
        raise HTTPException(status_code=404, detail="Không tìm thấy kế hoạch.")

    primary_unit = _primary_unit_of_user(db, user)
    primary_parent_unit = _parent_unit(db, primary_unit.id) if primary_unit else None
    primary_block_code = _unit_block_code(primary_unit)
    primary_category_code = _unit_category_code(primary_unit)
    primary_cm_subunit_kind = (
        _infer_cm_subunit_kind(db, primary_unit.id)
        if primary_unit and primary_block_code == "CHUYEN_MON" and primary_category_code == "SUBUNIT"
        else ""
    )
    is_cm_exec = _is_cm_exec_viewer(db, user)

    native_all_access = is_all_units_access(db, user)
    grant_all_plan_access = _has_plan_visibility_grant(db, user)
    mgr_phong = bool(_is_mgr_phong(db, user) or _is_mgr_khoa(db, user))
    mgr_to = bool(_is_mgr_to(db, user) or _is_mgr_donvi(db, user) or _is_mgr_nhom(db, user))

    allowed = False
    if _plan_scope_of(p) == PLAN_SCOPE_COMMITTEE:
        allowed = _user_can_view_committee_plan(db, user, p)
    elif native_all_access or grant_all_plan_access:
        allowed = True
    elif p.created_by == user.id:
        allowed = True
    else:
        prims = _user_primary_units(db, user)

        if mgr_phong:
            phong_ids = []
            for u in prims:
                if getattr(u, "cap_do", None) == 2:
                    phong_ids.append(u.id)
                elif getattr(u, "cap_do", None) == 3 and getattr(u, "parent_id", None):
                    phong_ids.append(u.parent_id)

            phong_ids = _dedup_ids(phong_ids)
            to_list = _unit_children(db, phong_ids)
            to_ids = _dedup_ids([t.id for t in to_list])

            all_unit_ids = _dedup_ids(phong_ids + to_ids)
            member_ids = _unit_members_user_ids(db, all_unit_ids)
            member_ids = _filter_personal_plan_user_ids_for_manager(db, user, member_ids)

            if p.plan_kind in ["PHONG", "KHOA"] and p.unit_id in phong_ids:
                allowed = True
            elif p.plan_kind in ["TO", "DONVI", "NHOM"] and p.unit_id in to_ids:
                allowed = True
            elif p.plan_kind == "NHANVIEN" and p.created_by in member_ids:
                allowed = True

        elif mgr_to:
            to_ids = []
            phong_ids = []

            for u in prims:
                if getattr(u, "cap_do", None) == 3:
                    to_ids.append(u.id)
                    if getattr(u, "parent_id", None):
                        phong_ids.append(u.parent_id)

            to_ids = _dedup_ids(to_ids)
            phong_ids = _dedup_ids(phong_ids)

            member_ids = _unit_members_user_ids(db, to_ids)
            member_ids = _filter_personal_plan_user_ids_for_manager(db, user, member_ids)

            if p.plan_kind in ["PHONG", "KHOA"] and p.unit_id in phong_ids:
                allowed = True
            elif p.plan_kind in ["TO", "DONVI", "NHOM"] and p.unit_id in to_ids:
                allowed = True
            elif p.plan_kind == "NHANVIEN" and p.created_by in member_ids:
                allowed = True

        else:
            phong_ids = []
            to_ids = []

            for u in prims:
                if getattr(u, "cap_do", None) == 3:
                    to_ids.append(u.id)
                    if getattr(u, "parent_id", None):
                        phong_ids.append(u.parent_id)
                elif getattr(u, "cap_do", None) == 2:
                    phong_ids.append(u.id)

            phong_ids = _dedup_ids(phong_ids)
            to_ids = _dedup_ids(to_ids)

            if p.plan_kind in ["PHONG", "KHOA"] and p.unit_id in phong_ids:
                allowed = True
            elif p.plan_kind in ["TO", "DONVI", "NHOM"] and p.unit_id in to_ids:
                allowed = True
                
    if not allowed:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Bạn không có quyền xem kế hoạch này.")

    items = db.query(PlanItems).filter(PlanItems.plan_id == plan_id).all()
    for it in items:
        vis = _strip_tags_for_display(it.content or "")
        it._content_visible = vis if vis else (it.content or "").strip()
        period, st = _extract_period_and_status_from_content(it.content or "", getattr(it, "due_date", None))
        it._period_label = period
        it._status_label = st
    p.items = items

    _decorate_plan_scope(db, p)
    p._creator_name = db.query(func.coalesce(Users.full_name, Users.username, "")).filter(Users.id == p.created_by).scalar() or ""
    committee_plan_options = _get_committee_plan_options(db, user)

    return templates.TemplateResponse("plans.html", {
        "request": request,
        "year": p.year,
        "month": p.month,
        "plans": [p],
        "user": user,
        "_is_hdtv": native_all_access,
        "_can_view_all_plans": (native_all_access or grant_all_plan_access),
        "_is_manager": bool(mgr_phong or mgr_to),
        "_is_manager_phong": mgr_phong,
        "_is_manager_to": mgr_to,
        "_is_staff": (not native_all_access) and (not mgr_phong) and (not mgr_to),

        "_is_hc_mgr_phong": _is_mgr_phong(db, user),
        "_is_hc_mgr_to": _is_mgr_to(db, user),
        "_is_cm_mgr_khoa": _is_mgr_khoa(db, user),
        "_is_cm_mgr_donvi": _is_mgr_donvi(db, user),
        "_is_cm_mgr_nhom": _is_mgr_nhom(db, user),
        "_is_hc_staff": _is_hc_staff(db, user),
        "_is_cm_staff": _is_cm_staff(db, user),
        "_is_cm_exec": is_cm_exec,
        "_primary_block_code": primary_block_code,
        "_primary_category_code": primary_category_code,
        "_primary_cm_subunit_kind": primary_cm_subunit_kind,
        "_edit_mode": (request.query_params.get("mode") or "") == "edit",
        "filterable_entities": {"units": [], "users": [], "committees": []},
        "createable_units": [],
        "committee_plan_options": [{"id": c.id, "name": c.name} for c in committee_plan_options],
        "has_committee_plan_scope": bool(committee_plan_options),
        "selected_filter_type": "",
        "selected_filter_id": "",
        "selected_plan_id": plan_id,
        "selected_kind": "",
    })

@router.post("/create", name="add_plan")
def create_plan(
    request: Request,
    title: str = Form(...),
    year: int = Form(...),
    month: int = Form(...),
    description: str = Form(""),
    unit_id: str = Form(""),
    scope_type: str = Form(PLAN_SCOPE_UNIT),
    committee_id: str = Form(""),

    # NHẬN CẢ HAI KIỂU TÊN TRƯỜNG: có [] và không có []
    item_contents: Optional[List[str]] = Form(None),
    item_contents2: Optional[List[str]] = Form(None, alias="item_contents[]"),

    item_start_y: Optional[List[str]] = Form(None),
    item_start_y2: Optional[List[str]] = Form(None, alias="item_start_y[]"),
    item_start_m: Optional[List[str]] = Form(None),
    item_start_m2: Optional[List[str]] = Form(None, alias="item_start_m[]"),
    item_start_d: Optional[List[str]] = Form(None),
    item_start_d2: Optional[List[str]] = Form(None, alias="item_start_d[]"),

    item_end_y: Optional[List[str]] = Form(None),
    item_end_y2: Optional[List[str]] = Form(None, alias="item_end_y[]"),
    item_end_m: Optional[List[str]] = Form(None),
    item_end_m2: Optional[List[str]] = Form(None, alias="item_end_m[]"),
    item_end_d: Optional[List[str]] = Form(None),
    item_end_d2: Optional[List[str]] = Form(None, alias="item_end_d[]"),

    item_statuses: Optional[List[str]] = Form(None),
    item_statuses2: Optional[List[str]] = Form(None, alias="item_statuses[]"),

    creator_kind: str = Form(""),

    _secret_check: Users = Depends(
        require_secret_lock(ActionCode.ASSIGN_TASK_DOWNSTREAM) if _ENABLE_SECRET_LOCK_PLANS else _skip_secret_lock
    ),
    db: Session = Depends(get_db),
):
    user = login_required(request, db)
    # Kiểm tra quyền lập kế hoạch theo vị trí + phạm vi đơn vị
    is_hc_mgr_phong = _is_mgr_phong(db, user)
    is_hc_mgr_to = _is_mgr_to(db, user)

    is_cm_mgr_khoa = _is_mgr_khoa(db, user)
    is_cm_mgr_donvi = _is_mgr_donvi(db, user)
    is_cm_mgr_nhom = _is_mgr_nhom(db, user)

    is_hc_staff = _is_hc_staff(db, user)
    is_cm_staff = _is_cm_staff(db, user)

    clean_scope_type = _normalize_plan_scope(scope_type)
    clean_committee_id = (committee_id or "").strip()

    if clean_scope_type == PLAN_SCOPE_COMMITTEE:
        if not _user_can_create_committee_plan(db, user.id, clean_committee_id):
            raise HTTPException(status_code=403, detail="Chỉ Trưởng ban/Phó trưởng ban mới được tạo Kế hoạch Ban kiêm nhiệm.")

        unit_id = _fallback_plan_unit_id_for_user(db, user)
        if not unit_id:
            raise HTTPException(status_code=400, detail="Tài khoản chưa có đơn vị gốc để lưu kế hoạch Ban.")

        unit = db.get(Units, unit_id)
        if not unit:
            raise HTTPException(status_code=400, detail="Đơn vị gốc không hợp lệ.")

        kind = "COMMITTEE"
        allowed = True
        unit_cap = getattr(unit, "cap_do", None)

    else:
        clean_committee_id = ""
        unit = db.get(Units, unit_id)
        if not unit:
            raise HTTPException(status_code=400, detail="Đơn vị không hợp lệ.")

        kind = (creator_kind or "").strip().upper()
        scope_ids = set(accessible_unit_ids(db, user) or [])

        allowed = False
        unit_cap = getattr(unit, "cap_do", None)

    if clean_scope_type != PLAN_SCOPE_COMMITTEE:
        if is_all_units_access(db, user):
            allowed = True

        elif is_hc_mgr_phong:
            # Khối hành chính cấp phòng
            if unit_id in scope_ids and unit_cap == 2 and kind in ["PHONG", "NV", "NHANVIEN", ""]:
                allowed = True

        elif is_hc_mgr_to:
            # Khối hành chính cấp tổ
            if unit_id in scope_ids and unit_cap == 3 and kind in ["TO", "NV", "NHANVIEN", ""]:
                allowed = True

        elif is_cm_mgr_khoa:
            # Khối chuyên môn cấp khoa:
            # chỉ được tạo Kế hoạch khoa tại chính khoa và Kế hoạch cá nhân
            if unit_id in scope_ids:
                if kind in ["KHOA", "NV", "NHANVIEN", ""]:
                    allowed = True

        elif is_cm_mgr_donvi:
            # Khối chuyên môn cấp đơn vị
            if unit_id in scope_ids and kind in ["DONVI", "NV", "NHANVIEN", ""]:
                allowed = True

        elif is_cm_mgr_nhom:
            # Khối chuyên môn cấp nhóm
            if unit_id in scope_ids and kind in ["NHOM", "NV", "NHANVIEN", ""]:
                allowed = True

        elif is_hc_staff or is_cm_staff:
            # Nhân sự trực tiếp chỉ tạo kế hoạch cá nhân
            if unit_id in scope_ids and kind in ["NV", "NHANVIEN", ""]:
                allowed = True

    if not allowed:
        raise HTTPException(status_code=403, detail="Bạn không có quyền tạo loại kế hoạch này.")

    p = Plans(
        title=(title or "").strip(),
        year=int(year), month=int(month),
        description=(description or "").strip(),
        plan_kind=(
            "COMMITTEE" if clean_scope_type == PLAN_SCOPE_COMMITTEE
            else "PHONG" if (creator_kind or "").strip().lower() == "phong"
            else "TO" if (creator_kind or "").strip().lower() == "to"
            else "KHOA" if (creator_kind or "").strip().lower() == "khoa"
            else "DONVI" if (creator_kind or "").strip().lower() == "donvi"
            else "NHOM" if (creator_kind or "").strip().lower() == "nhom"
            else "NHANVIEN" if (creator_kind or "").strip().lower() in ("nhanvien", "nv")
            else None
        ),
        unit_id=unit_id,
        scope_type=clean_scope_type,
        committee_id=clean_committee_id or None,
        status=PlanStatus.DRAFT,
        created_by=user.id, created_at=datetime.utcnow(), updated_at=datetime.utcnow(),
    )
    db.add(p); db.commit(); db.refresh(p)

    db.flush()

    # Chọn nguồn dữ liệu: ưu tiên field có []
    contents = item_contents2 or item_contents
    sy_list  = item_start_y2 or item_start_y
    sm_list  = item_start_m2 or item_start_m
    sd_list  = item_start_d2 or item_start_d
    ey_list  = item_end_y2   or item_end_y
    em_list  = item_end_m2   or item_end_m
    ed_list  = item_end_d2   or item_end_d
    st_list  = item_statuses2 or item_statuses

    def _get(lst, i):
        return (lst[i] if lst and i < len(lst) else None)

    if contents:
        n = len(contents)
        for i in range(n):
            content = (_get(contents, i) or "").strip()

            sy = _get(sy_list, i) or str(year)
            ey = _get(ey_list, i) or str(year)
            sm = _get(sm_list, i)
            sd = _get(sd_list, i)
            em = _get(em_list, i)
            ed = _get(ed_list, i)

            start = _compose_date(sy, sm, sd)
            end   = _compose_date(ey, em, ed)
            raw_status = _get(st_list, i)

            # Bỏ dòng trống hoàn toàn
            if not content and not start and not end and not (raw_status or "").strip():
                continue

            status_norm = _normalize_item_status(raw_status)

            cf_count = 1 if status_norm == "Chuyển kỳ sau" else 0
            ever_cf = status_norm == "Chuyển kỳ sau"

            it = _build_plan_item(
                plan_id=p.id,
                content=content,
                start=start,
                end=end,
                status_txt=status_norm,
                work_key=str(uuid.uuid4()),
                source_item_id=None,
                carried_forward_count=cf_count,
                was_ever_carried_forward=ever_cf,
            )
            db.add(it)
            db.flush()

            if status_norm == "Chuyển kỳ sau":
                _auto_create_next_period_item(db, p, it)

    db.commit()
    _emit_plans_realtime(db, p, action="created", actor_user_id=user.id)
    return RedirectResponse(
        url=f"/plans?year={p.year}&month={p.month}&plan_id={p.id}",
        status_code=302,
    )

@router.post("/update", name="update_plan")
def update_plan(
    request: Request,
    plan_id: str = Form(...),
    title: str = Form(...),
    year: int = Form(...),
    month: int = Form(...),
    description: str = Form(""),

    # NHẬN CẢ HAI KIỂU TÊN TRƯỜNG: có [] và không có []
    item_contents: Optional[List[str]] = Form(None),
    item_contents2: Optional[List[str]] = Form(None, alias="item_contents[]"),

    item_start_y: Optional[List[str]] = Form(None),
    item_start_y2: Optional[List[str]] = Form(None, alias="item_start_y[]"),
    item_start_m: Optional[List[str]] = Form(None),
    item_start_m2: Optional[List[str]] = Form(None, alias="item_start_m[]"),
    item_start_d: Optional[List[str]] = Form(None),
    item_start_d2: Optional[List[str]] = Form(None, alias="item_start_d[]"),

    item_end_y: Optional[List[str]] = Form(None),
    item_end_y2: Optional[List[str]] = Form(None, alias="item_end_y[]"),
    item_end_m: Optional[List[str]] = Form(None),
    item_end_m2: Optional[List[str]] = Form(None, alias="item_end_m[]"),
    item_end_d: Optional[List[str]] = Form(None),
    item_end_d2: Optional[List[str]] = Form(None, alias="item_end_d[]"),

    item_statuses: Optional[List[str]] = Form(None),
    item_statuses2: Optional[List[str]] = Form(None, alias="item_statuses[]"),

    _secret_check: Users = Depends(
        require_secret_lock(ActionCode.ASSIGN_TASK_DOWNSTREAM) if _ENABLE_SECRET_LOCK_PLANS else _skip_secret_lock
    ),
    db: Session = Depends(get_db),
):
    user = login_required(request, db)
    p = db.get(Plans, plan_id)
    if not p:
        raise HTTPException(status_code=404, detail="Không tìm thấy kế hoạch.")
    if p.created_by != user.id:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Chỉ người tạo kế hoạch mới được cập nhật.")

    p.title = (title or "").strip()
    p.year = int(year)
    p.month = int(month)
    p.description = (description or "").strip()
    p.updated_at = datetime.utcnow()
    db.add(p)
    db.flush()

    # Chọn nguồn dữ liệu: ưu tiên field có []
    contents = item_contents2 or item_contents
    sy_list  = item_start_y2 or item_start_y
    sm_list  = item_start_m2 or item_start_m
    sd_list  = item_start_d2 or item_start_d
    ey_list  = item_end_y2   or item_end_y
    em_list  = item_end_m2   or item_end_m
    ed_list  = item_end_d2   or item_end_d
    st_list  = item_statuses2 or item_statuses

    def _get(lst, i):
        return (lst[i] if lst and i < len(lst) else None)

    old_rows = (
        db.query(PlanItems)
        .filter(PlanItems.plan_id == p.id)
        .order_by(PlanItems.created_at.asc(), PlanItems.id.asc())
        .all()
    )

    # Xóa toàn bộ dòng hiện tại rồi dựng lại nhưng vẫn giữ work_key / số lần carry theo dòng cũ
    db.query(PlanItems).filter(PlanItems.plan_id == p.id).delete(synchronize_session=False)

    if contents:
        n = len(contents)
        for i in range(n):
            old = old_rows[i] if i < len(old_rows) else None

            content = (_get(contents, i) or "").strip()

            sy = _get(sy_list, i) or str(year)
            ey = _get(ey_list, i) or str(year)
            sm = _get(sm_list, i)
            sd = _get(sd_list, i)
            em = _get(em_list, i)
            ed = _get(ed_list, i)

            start = _compose_date(sy, sm, sd)
            end   = _compose_date(ey, em, ed)
            raw_status = _get(st_list, i)

            if not content and not start and not end and not (raw_status or "").strip():
                if old and old.work_key:
                    _remove_auto_carried_forward_item(db, p, old.work_key, old.id)
                continue

            status_norm = _normalize_item_status(raw_status)

            old_status = ""
            if old:
                _, old_status = _extract_period_and_status_from_content(old.content or "", getattr(old, "due_date", None))
                old_status = _normalize_item_status(old_status)

            work_key = (old.work_key if old and old.work_key else str(uuid.uuid4()))
            source_item_id = (old.source_item_id if old else None)

            inherited_cf = int(getattr(old, "carried_forward_count", 0) or 0) if old else 0
            inherited_ever = bool(getattr(old, "was_ever_carried_forward", False)) if old else False

            if status_norm == "Chuyển kỳ sau":
                if old_status == "Chuyển kỳ sau":
                    cf_count = inherited_cf
                else:
                    cf_count = inherited_cf + 1
                ever_cf = True
            else:
                cf_count = inherited_cf
                ever_cf = inherited_ever

            new_item = _build_plan_item(
                plan_id=p.id,
                content=content,
                start=start,
                end=end,
                status_txt=status_norm,
                work_key=work_key,
                source_item_id=source_item_id,
                carried_forward_count=cf_count,
                was_ever_carried_forward=ever_cf,
            )
            db.add(new_item)
            db.flush()

            if status_norm == "Chuyển kỳ sau":
                _auto_create_next_period_item(db, p, new_item)
            elif old and old.work_key:
                _remove_auto_carried_forward_item(db, p, old.work_key, old.id)

    db.commit()
    _emit_plans_realtime(db, p, action="updated", actor_user_id=user.id)
    return RedirectResponse(
        url=f"/plans?year={p.year}&month={p.month}&plan_id={p.id}",
        status_code=302,
    )

@router.post("/delete")
def delete_plan(request: Request, plan_id: str = Form(...), db: Session = Depends(get_db)):
    user = login_required(request, db)
    p = db.get(Plans, plan_id)
    if not p: raise HTTPException(status_code=404, detail="Không tìm thấy kế hoạch.")
    if p.created_by != user.id:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Chỉ người tạo kế hoạch mới được xóa.")
    y, m = p.year, p.month
    realtime_user_ids = _plans_realtime_user_ids(db, p, actor_user_id=user.id)
    payload = {
        "module": "work",
        "type": "plans_changed",
        "action": "deleted",
        "plan_id": str(getattr(p, "id", "") or ""),
        "unit_id": str(getattr(p, "unit_id", "") or ""),
        "scope_type": _plan_scope_of(p),
        "committee_id": str(getattr(p, "committee_id", "") or ""),
        "year": int(getattr(p, "year", 0) or 0),
        "month": int(getattr(p, "month", 0) or 0),
        "actor_user_id": str(user.id or ""),
        "changed_at": datetime.utcnow().isoformat(),
    }

    db.query(PlanItems).filter(PlanItems.plan_id == plan_id).delete(synchronize_session=False)
    db.delete(p)
    db.commit()

    if realtime_user_ids:
        try:
            from_thread.run(manager.notify_users_json, realtime_user_ids, payload)
        except Exception as ex:
            logger.exception("[plans] Lỗi phát realtime sau xóa kế hoạch: %s", ex)

    return RedirectResponse(url=f"/plans?year={y}&month={m}", status_code=302)
