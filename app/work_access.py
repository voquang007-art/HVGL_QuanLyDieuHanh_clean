from __future__ import annotations

from typing import Any, Dict, Set


APPROVER_ROLES: Set[str] = {
    "ROLE_TRUONG_PHONG",
    "ROLE_PHO_PHONG",
    "ROLE_TRUONG_KHOA",
    "ROLE_PHO_TRUONG_KHOA",
    "ROLE_DIEU_DUONG_TRUONG",
    "ROLE_KY_THUAT_VIEN_TRUONG",
    "ROLE_TRUONG_DON_VI",
    "ROLE_PHO_DON_VI",
    "ROLE_DIEU_DUONG_TRUONG_DON_VI",
    "ROLE_KY_THUAT_VIEN_TRUONG_DON_VI",
    "ROLE_TO_TRUONG",
    "ROLE_PHO_TO",
    "ROLE_TRUONG_NHOM",
    "ROLE_PHO_NHOM",
}

BOARD_ROLES: Set[str] = {
    "ROLE_LANH_DAO",
    "ROLE_TONG_GIAM_DOC",
    "ROLE_PHO_TONG_GIAM_DOC_THUONG_TRUC",
    "ROLE_PHO_TONG_GIAM_DOC",
    "ROLE_GIAM_DOC",
    "ROLE_PHO_GIAM_DOC_TRUC",
    "ROLE_PHO_GIAM_DOC",
}

DRAFT_APPROVAL_HIDDEN_ROLES: Set[str] = {
    "ROLE_HO_LY",
}

STAFF_ROLES: Set[str] = {
    "ROLE_NHAN_VIEN",
    "ROLE_DIEU_DUONG",
    "ROLE_KY_THUAT_VIEN",
    "ROLE_DUOC_SI",
    "ROLE_HO_LY",
    "ROLE_BAC_SI",
    "ROLE_THU_KY_Y_KHOA",
    "ROLE_QL_CHAT_LUONG",
    "ROLE_QL_KY_THUAT",
    "ROLE_QL_AN_TOAN",
    "ROLE_QL_VAT_TU",
    "ROLE_QL_TRANG_THIET_BI",
    "ROLE_QL_MOI_TRUONG",
    "ROLE_QL_CNTT",
}


def normalize_role_codes(raw_roles: Any) -> Set[str]:
    if raw_roles is None:
        return set()

    if isinstance(raw_roles, str):
        parts = raw_roles.replace(";", ",").replace("|", ",").split(",")
        return {str(x).strip().upper() for x in parts if str(x).strip()}

    if isinstance(raw_roles, (list, tuple, set)):
        return {str(x).strip().upper() for x in raw_roles if str(x).strip()}

    value = str(raw_roles).strip().upper()
    return {value} if value else set()


def get_work_access_flags(
    raw_roles: Any,
    *,
    is_admin: bool = False,
    is_admin_or_leader: bool = False,
) -> Dict[str, Any]:
    role_codes = normalize_role_codes(raw_roles)

    is_admin_flag = bool(is_admin) or ("ROLE_ADMIN" in role_codes)
    is_approver = bool(APPROVER_ROLES & role_codes)

    is_staff = bool(STAFF_ROLES & role_codes) and (not is_approver)

    is_board = (
        (
            bool(BOARD_ROLES & role_codes)
            or (bool(is_admin_or_leader) and not is_approver and not is_staff)
        )
        and not is_approver
    )

    show_plans = True
    show_tasks = bool(is_admin_flag or is_board or is_approver)
    show_inbox = bool(is_admin_flag or is_approver or (is_staff and not is_board))
    show_draft_approval = bool(
        (is_admin_flag or is_board or is_approver or is_staff)
        and not (DRAFT_APPROVAL_HIDDEN_ROLES & role_codes)
    )

    return {
        "role_codes": role_codes,
        "is_admin": is_admin_flag,
        "is_board": is_board,
        "is_approver": is_approver,
        "is_staff": is_staff,
        "show_plans": show_plans,
        "show_tasks": show_tasks,
        "show_inbox": show_inbox,
        "show_draft_approval": show_draft_approval,
        "default_path": "/plans",
    }


def get_work_access_from_session(session: Any) -> Dict[str, Any]:
    session = session or {}
    return get_work_access_flags(
        session.get("roles"),
        is_admin=bool(session.get("is_admin")),
        is_admin_or_leader=bool(session.get("is_admin_or_leader")),
    )
    


# =========================
# Committee / Ban kiêm nhiệm helpers
# =========================

COMMITTEE_MANAGER_ROLES: Set[str] = {
    "TRUONG_BAN",
    "PHO_TRUONG_BAN",
}


def get_user_committee_ids(db: Any, user_id: str, *, active_only: bool = True) -> list[str]:
    from .models import CommitteeMembers, Committees

    query = (
        db.query(CommitteeMembers.committee_id)
        .join(Committees, Committees.id == CommitteeMembers.committee_id)
        .filter(CommitteeMembers.user_id == user_id)
        .filter(CommitteeMembers.is_active == True)  # noqa: E712
    )

    if active_only:
        query = query.filter(
            Committees.status == "ACTIVE",
            Committees.is_active == True,  # noqa: E712
        )

    rows = query.distinct().all()
    return [str(committee_id) for (committee_id,) in rows if committee_id]


def user_is_committee_member(db: Any, user_id: str, committee_id: str) -> bool:
    from .models import CommitteeMembers, Committees

    row = (
        db.query(CommitteeMembers.id)
        .join(Committees, Committees.id == CommitteeMembers.committee_id)
        .filter(CommitteeMembers.user_id == user_id)
        .filter(CommitteeMembers.committee_id == committee_id)
        .filter(CommitteeMembers.is_active == True)  # noqa: E712
        .filter(Committees.status == "ACTIVE", Committees.is_active == True)  # noqa: E712
        .first()
    )
    return row is not None


def user_is_committee_manager(db: Any, user_id: str, committee_id: str) -> bool:
    from .models import CommitteeMembers, Committees

    row = (
        db.query(CommitteeMembers.id)
        .join(Committees, Committees.id == CommitteeMembers.committee_id)
        .filter(CommitteeMembers.user_id == user_id)
        .filter(CommitteeMembers.committee_id == committee_id)
        .filter(CommitteeMembers.is_active == True)  # noqa: E712
        .filter(CommitteeMembers.committee_role.in_(list(COMMITTEE_MANAGER_ROLES)))
        .filter(Committees.status == "ACTIVE", Committees.is_active == True)  # noqa: E712
        .first()
    )
    return row is not None


def user_can_manage_committees_from_roles(raw_roles: Any, *, is_admin: bool = False) -> bool:
    role_codes = normalize_role_codes(raw_roles)
    return bool(is_admin) or ("ROLE_ADMIN" in role_codes)